defmodule DataBuffer.Partition do
  @moduledoc false

  use GenServer

  alias DataBuffer.{Flusher, FlusherPool}

  require Logger

  defmodule State do
    @moduledoc false

    defstruct [
      :name,
      :buffer,
      :max_size,
      :max_size_jitter,
      :flush_size,
      :flush_interval,
      :flush_jitter,
      :flush_timeout,
      :flush_opts,
      :flush_complete_ref,
      :flush_schedule_ref,
      :flush_schedule_timer_ref,
      :size,
      :flusher,
      :flush_timeout_ref,
      :flush_timeout_timer_ref,
      :table
    ]
  end

  @type partition :: atom()
  @type table :: :ets.tid()

  @opts_schema KeywordValidator.schema!(
                 name: [is: :atom, required: true],
                 buffer: [is: :mod, required: true],
                 max_size: [is: :integer, default: 5_000, required: true],
                 max_size_jitter: [is: :integer, default: 0, required: true],
                 flush_interval: [is: :integer, default: 10_000, required: true],
                 flush_jitter: [is: :integer, default: 2_000, required: true],
                 flush_meta: [is: :any, required: false],
                 flush_timeout: [is: :timeout, default: 60_000, required: true]
               )

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

  @spec flush(partition(), timeout()) :: :ok
  def flush(partition, timeout \\ 5_000) do
    GenServer.call(partition, :flush, timeout)
  end

  @spec sync_flush(partition(), timeout()) :: :ok
  def sync_flush(partition, timeout \\ 5_000) do
    GenServer.call(partition, :sync_flush, timeout)
  end

  @spec dump(partition(), timeout()) :: :ok
  def dump(partition, timeout \\ 5_000) do
    GenServer.call(partition, :dump, timeout)
  end

  @spec insert(partition(), any(), timeout()) :: :ok
  def insert(partition, data, timeout \\ 5_000) do
    GenServer.call(partition, {:insert, data}, timeout)
  end

  @spec insert_batch(partition(), Enumerable.t(), timeout()) :: :ok
  def insert_batch(partition, data, timeout \\ 5_000) do
    GenServer.call(partition, {:insert_batch, data}, timeout)
  end

  @spec size(partition(), timeout()) :: integer()
  def size(partition, timeout \\ 5_000) do
    GenServer.call(partition, :size, timeout)
  end

  @spec info(partition(), timeout()) :: integer()
  def info(partition, timeout \\ 5_000) do
    GenServer.call(partition, :info, timeout)
  end

  ################################
  # GenServer Callbacks
  ################################

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    state = init_state(opts)
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:insert, data}, _from, %State{} = state) do
    state = do_insert(state, data)
    {:reply, :ok, state}
  end

  def handle_call({:insert_batch, data}, _from, %State{} = state) do
    state = do_insert_batch(state, data)
    {:reply, :ok, state}
  end

  def handle_call(:flush, _from, %State{} = state) do
    state = do_flush(state)
    {:reply, :ok, state}
  end

  def handle_call(:sync_flush, _from, %State{} = state) do
    {data, state} = do_sync_flush(state)
    {:reply, data, state}
  end

  def handle_call(:dump, _from, %State{} = state) do
    data = do_dump_table(state)
    {:reply, data, state}
  end

  def handle_call(:size, _from, %State{} = state) do
    {:reply, state.size, state}
  end

  def handle_call(:info, _from, %State{} = state) do
    info = do_get_info(state)
    {:reply, info, state}
  end

  @impl GenServer
  def handle_info(:flush, %State{} = state) do
    state = do_flush(state)
    {:noreply, state}
  end

  def handle_info({:flush_schedule, flush_schedule_ref}, %State{} = state) do
    state = do_scheduled_flush(state, flush_schedule_ref)
    {:noreply, state}
  end

  def handle_info({:flush_timeout, flush_schedule_ref}, %State{} = state) do
    state = do_timeout_flush(state, flush_schedule_ref)
    {:noreply, state}
  end

  def handle_info({:flush_complete, flush_complete_ref}, %State{} = state) do
    state = do_complete_flush(state, flush_complete_ref)
    {:noreply, state}
  end

  @doc false
  @impl GenServer
  def terminate(_reason, %State{} = state) do
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
    state = %State{
      name: Keyword.get(opts, :name),
      buffer: Keyword.get(opts, :buffer),
      max_size: Keyword.get(opts, :max_size),
      max_size_jitter: Keyword.get(opts, :max_size_jitter),
      flush_size: 0,
      flush_interval: Keyword.get(opts, :flush_interval),
      flush_jitter: Keyword.get(opts, :flush_jitter),
      flush_timeout: Keyword.get(opts, :flush_timeout),
      flush_opts: [
        meta: Keyword.get(opts, :flush_meta)
      ],
      size: 0
    }

    do_prepare_flush(state)
  end

  defp do_prepare_flush(state) do
    table = :ets.new(:partition, [:private, :ordered_set])
    flush_size = state.max_size + Enum.random(0..state.max_size_jitter)

    if is_reference(state.flush_schedule_timer_ref),
      do: Process.cancel_timer(state.flush_schedule_timer_ref)

    time = state.flush_interval + Enum.random(0..state.flush_jitter)
    flush_schedule_ref = make_ref()

    flush_schedule_timer_ref =
      Process.send_after(self(), {:flush_schedule, flush_schedule_ref}, time)

    %{
      state
      | table: table,
        size: 0,
        flush_size: flush_size,
        flush_schedule_ref: flush_schedule_ref,
        flush_schedule_timer_ref: flush_schedule_timer_ref
    }
  end

  defguardp is_full(size, flush_size) when size >= flush_size

  defp do_insert(%State{flusher: flusher, size: size, flush_size: flush_size} = state, data)
       when is_pid(flusher) and is_full(size, flush_size) do
    state
    |> do_await_flush()
    |> do_insert(data)
  end

  defp do_insert(%State{size: size, flush_size: flush_size} = state, data)
       when is_full(size, flush_size) do
    state
    |> do_flush()
    |> do_insert(data)
  end

  defp do_insert(state, data) do
    size = state.size + 1
    :ets.insert(state.table, {size, data})
    %{state | size: size}
  end

  defp do_insert_batch(%State{flusher: flusher, size: size, flush_size: flush_size} = state, data)
       when is_pid(flusher) and is_full(size, flush_size) do
    state
    |> do_await_flush()
    |> do_insert_batch(data)
  end

  defp do_insert_batch(%State{size: size, flush_size: flush_size} = state, data)
       when is_full(size, flush_size) do
    state
    |> do_flush()
    |> do_insert_batch(data)
  end

  defp do_insert_batch(%State{size: size, table: table} = state, data) do
    {rows, size} =
      Enum.map_reduce(data, size, fn data, size ->
        {{size + 1, data}, size + 1}
      end)

    :ets.insert(table, rows)
    %{state | size: size}
  end

  defp do_scheduled_flush(state, flush_schedule_ref) do
    if state.flush_schedule_ref == flush_schedule_ref do
      do_flush(state)
    else
      state
    end
  end

  defp do_flush(state) do
    flush_complete_ref = make_ref()
    {:ok, flusher} = FlusherPool.start_flusher(state.buffer, state.flush_opts)
    :ets.give_away(state.table, flusher, {state.name, flush_complete_ref, state.size})
    flush_timeout_ref = make_ref()

    flush_timeout_timer_ref =
      Process.send_after(self(), {:flush_timeout, flush_timeout_ref}, state.flush_timeout)

    state = %{
      state
      | table: nil,
        flush_complete_ref: flush_complete_ref,
        flusher: flusher,
        flush_timeout_ref: flush_timeout_ref,
        flush_timeout_timer_ref: flush_timeout_timer_ref
    }

    do_prepare_flush(state)
  end

  defp do_sync_flush(state) do
    opts = Keyword.put(state.flush_opts, :size, state.size)
    data = Flusher.flush(state.table, state.buffer, opts)
    {data, do_prepare_flush(state)}
  end

  defp do_await_flush(state) do
    receive do
      {:flush_complete, flush_complete_ref} -> do_complete_flush(state, flush_complete_ref)
      {:flush_timeout, flush_timeout_ref} -> do_timeout_flush(state, flush_timeout_ref)
    end
  end

  defp do_timeout_flush(state, flush_timeout_ref) do
    if state.flush_timeout_ref == flush_timeout_ref do
      if is_pid(state.flusher), do: Process.exit(state.flusher, :timeout)

      buffer = state.buffer |> to_string() |> String.replace_leading("Elixir.", "")

      Logger.error("""
      DataBuffer: flush timeout error for #{buffer}. This means your \
      handle_flush/2 callback failed to return within its timeout. You can \
      address this by:

      1. Increasing your buffer flush_timeout.
      2. Lowering your buffer max_size.
      3. Improving the performance of your handle_flush/2 callback.

      See DataBuffer.start_link/2 for more information.
      """)

      do_clear_flush(state)
    else
      state
    end
  end

  defp do_complete_flush(state, flush_complete_ref) do
    if state.flush_complete_ref == flush_complete_ref do
      do_clear_flush(state)
    else
      state
    end
  end

  defp do_clear_flush(state) do
    if is_reference(state.flush_timeout_timer_ref),
      do: Process.cancel_timer(state.flush_timeout_timer_ref)

    %{
      state
      | flush_complete_ref: nil,
        flusher: nil,
        flush_timeout_ref: nil,
        flush_timeout_timer_ref: nil
    }
  end

  defp do_dump_table(state) do
    :ets.tab2list(state.table)
  end

  defp do_get_info(state) do
    %{
      size: state.size,
      name: state.name,
      flush_size: state.flush_size,
      flush_interval: state.flush_interval,
      flush_jitter: state.flush_jitter,
      flush_timeout: state.flush_timeout,
      pid: self()
    }
  end
end
