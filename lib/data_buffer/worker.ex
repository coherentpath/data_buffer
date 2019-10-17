defmodule DataBuffer.Worker do
  @moduledoc false

  use GenServer

  require Logger

  alias DataBuffer.{FlusherSupervisor, Tables}

  ################################
  # Public API
  ################################

  @doc false
  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, args}
    }
  end

  @spec start_link(buffer :: DataBuffer.t(), keyword()) :: GenServer.on_start()
  def start_link(buffer, opts) do
    GenServer.start_link(__MODULE__, {buffer, opts}, name: buffer)
  end

  @spec insert(
          buffer :: DataBuffer.t(),
          key :: any(),
          val :: any(),
          partitions :: integer(),
          max_size :: integer() | :infinity
        ) ::
          :ok | :error
  def insert(buffer, key, val, partitions, max_size) do
    do_insert(buffer, key, val, partitions, max_size)
  rescue
    ArgumentError -> :error
  end

  @spec flush(buffer :: DataBuffer.t(), key :: any()) :: :ok
  def flush(buffer, key) do
    GenServer.call(buffer, {:flush, key})
  end

  @spec count(buffer :: DataBuffer.t(), key :: any(), partitions :: integer()) :: integer() | nil
  def count(buffer, key, partitions) do
    do_count(buffer, key, partitions)
  end

  ################################
  # GenServer Callbacks
  ################################

  @impl GenServer
  def init({buffer, opts}) do
    state = do_state(buffer, opts)
    do_table_init(state)

    {:ok, state, state.timeout()}
  end

  @impl GenServer
  def handle_call({:flush, key}, _from, state) do
    do_flush(state, key)
    {:reply, :ok, state, state.timeout()}
  end

  @impl GenServer
  def handle_cast({:schedule_flush, key}, state) do
    do_schedule_flush(state, key)
    {:noreply, state, state.timeout()}
  end

  @impl GenServer
  def handle_info({:flush, key}, state) do
    do_flush(state, key)
    {:noreply, state, state.timeout()}
  end

  def handle_info(:timeout, config) do
    {:noreply, config, :hibernate}
  end

  ################################
  # Private Functions
  ################################

  defp do_state(buffer, opts) do
    state =
      [buffer: buffer]
      |> Keyword.merge(opts)
      |> Enum.into(%{})

    %{state | partitions: state.partitions - 1}
  end

  defp do_table_init(state) do
    for partition <- 0..state.partitions do
      Tables.init(:buffer, state.buffer, partition)
      Tables.init(:counter, state.buffer, partition)
    end
  end

  defp do_insert(buffer, key, val, partitions, max_size) do
    partition = get_partition(key, partitions)
    buffer_table = Tables.name(:buffer, buffer, partition)
    counter_table = Tables.name(:counter, buffer, partition)
    count = :ets.update_counter(counter_table, key, 1, {0, 0})
    :ets.insert(buffer_table, {key, val})

    cond do
      count >= max_size -> GenServer.call(buffer, {:flush, key})
      count == 1 -> GenServer.cast(buffer, {:schedule_flush, key})
      true -> :ok
    end

    :ok
  end

  defp do_schedule_flush(state, key) do
    interval = do_calculate_interval(state)
    Process.send_after(self(), {:flush, key}, interval)
  end

  defp do_calculate_interval(%{jitter: 0, interval: interval}) do
    interval
  end

  defp do_calculate_interval(%{jitter: jitter, interval: interval}) do
    interval + :rand.uniform(jitter)
  end

  defp do_flush(state, key) do
    partition = get_partition(key, state.partitions)
    opts = [retry_delay: state.retry_delay, retry_max: state.retry_max]
    FlusherSupervisor.start_flusher(state.buffer, key, partition, opts)
  end

  defp do_count(buffer, key, partitions) do
    partition = get_partition(key, partitions)
    counter_table = Tables.name(:counter, buffer, partition)

    case :ets.lookup(counter_table, key) do
      [{^key, count}] -> count
      _ -> nil
    end
  end

  defp get_partition(_key, 0) do
    0
  end

  defp get_partition(key, partitions) do
    :erlang.phash2(key, partitions)
  end
end
