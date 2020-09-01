defmodule DataBuffer.Partition do
  @moduledoc false

  use GenServer

  alias DataBuffer.{Flusher, FlusherPool}

  require Logger

  @type partition :: atom()

  @opts_schema %{
    name: [type: :atom, required: true],
    buffer: [type: :module, required: true],
    max_size: [type: :integer, default: 5_000, required: true],
    flush_interval: [type: :integer, default: 10_000, required: true],
    flush_jitter: [type: :integer, default: 2_000, required: true],
    flush_meta: [type: :any, required: false],
    flush_timeout: [type: :integer, default: 60_000, required: true]
  }

  ################################
  # Public API
  ################################

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.fetch!(opts, :name),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    with {:ok, opts} <- validate_opts(opts) do
      server_opts = Keyword.take(opts, [:name])
      GenServer.start_link(__MODULE__, opts, server_opts)
    end
  end

  @spec flush(partition()) :: :ok
  def flush(partition) do
    GenServer.call(partition, :flush)
  end

  @spec sync_flush(partition()) :: :ok
  def sync_flush(partition) do
    GenServer.call(partition, :sync_flush)
  end

  @spec dump(partition()) :: :ok
  def dump(partition) do
    GenServer.call(partition, :dump)
  end

  @spec insert(partition(), any()) :: :ok
  def insert(partition, data) do
    GenServer.call(partition, {:insert, data})
  end

  @spec size(partition()) :: integer()
  def size(partition) do
    GenServer.call(partition, :size)
  end

  ################################
  # GenServer Callbacks
  ################################

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    state =
      opts
      |> init_state()
      |> do_prepare_flush

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:insert, data}, _from, state) do
    state = do_insert(state, data)
    {:reply, :ok, state}
  end

  def handle_call(:flush, _from, state) do
    state = do_flush(state)
    {:reply, :ok, state}
  end

  def handle_call(:sync_flush, _from, state) do
    {data, state} = do_sync_flush(state)
    {:reply, data, state}
  end

  def handle_call(:dump, _from, state) do
    data = do_dump_table(state)
    {:reply, data, state}
  end

  def handle_call(:size, _from, state) do
    {:reply, state.size, state}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    state = do_flush(state)
    {:noreply, state}
  end

  def handle_info(:flush_timeout, state) do
    state = do_timeout_flush(state)
    {:noreply, state}
  end

  def handle_info(:flush_complete, state) do
    state = do_complete_flush(state)
    {:noreply, state}
  end

  @doc false
  @impl GenServer
  def terminate(_reason, state) do
    {_data, state} = do_sync_flush(state)
    state
  end

  ################################
  # Private API
  ################################

  defp validate_opts(opts) do
    KeywordValidator.validate(opts, @opts_schema, strict: false)
  end

  defp init_state(opts) do
    %{
      name: Keyword.get(opts, :name),
      buffer: Keyword.get(opts, :buffer),
      max_size: Keyword.get(opts, :max_size),
      flush_interval: Keyword.get(opts, :flush_interval),
      flush_jitter: Keyword.get(opts, :flush_jitter),
      flush_timeout: Keyword.get(opts, :flush_timeout),
      flush_opts: [
        meta: Keyword.get(opts, :flush_meta)
      ],
      flush_ref: nil,
      size: 0,
      flusher: nil,
      flusher_timeout_ref: nil,
      table: nil
    }
  end

  defp init_table(state) do
    table = :ets.new(:partition, [:private, :set])
    %{state | table: table, size: 0}
  end

  defp schedule_flush(state) do
    if is_reference(state.flush_ref), do: Process.cancel_timer(state.flush_ref)
    time = state.flush_interval + :random.uniform(state.flush_jitter)
    flush_ref = Process.send_after(self(), :flush, time)
    %{state | flush_ref: flush_ref}
  end

  defguardp is_full(size, max_size) when size >= max_size

  defp do_insert(%{flusher: flusher, size: size, max_size: max_size} = state, data)
       when is_pid(flusher) and is_full(size, max_size) do
    state
    |> do_await_flush()
    |> do_insert(data)
  end

  defp do_insert(%{size: size, max_size: max_size} = state, data)
       when is_full(size, max_size) do
    state
    |> do_flush()
    |> do_insert(data)
  end

  defp do_insert(state, data) do
    :ets.insert(state.table, {make_ref(), data})
    %{state | size: state.size + 1}
  end

  defp do_flush(state) do
    {:ok, flusher} = FlusherPool.start_flusher(state.buffer, state.flush_opts)
    :ets.give_away(state.table, flusher, :ok)
    flusher_timeout_ref = Process.send_after(self(), :flush_timeout, state.flush_timeout)
    state = %{state | flusher: flusher, flusher_timeout_ref: flusher_timeout_ref}
    do_prepare_flush(state)
  end

  defp do_sync_flush(state) do
    data = Flusher.flush(state.table, state.buffer, state.flush_opts)
    {data, do_prepare_flush(state)}
  end

  defp do_prepare_flush(state) do
    state
    |> init_table()
    |> schedule_flush()
  end

  defp do_await_flush(state) do
    receive do
      :flush_complete -> do_complete_flush(state)
    end
  end

  defp do_timeout_flush(state) do
    if is_pid(state.flusher), do: Process.exit(state.flusher, :normal)
    Logger.error("[DataBuffer] Flush timeout error.")
    do_complete_flush(state)
  end

  defp do_complete_flush(state) do
    if is_reference(state.flusher_timeout_ref),
      do: Process.cancel_timer(state.flusher_timeout_ref)

    %{state | flusher: nil, flusher_timeout_ref: nil}
  end

  defp do_dump_table(state) do
    :ets.tab2list(state.table)
  end
end
