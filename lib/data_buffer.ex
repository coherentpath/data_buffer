defmodule DataBuffer do
  @moduledoc """
  DataBuffer provides an efficient way to buffer and batch process data in Elixir applications.

  DataBuffer is designed to accumulate data in memory and periodically flush it based on
  configurable thresholds (size or time intervals). This is particularly useful for:

  - Batching database writes to improve performance
  - Aggregating events before sending to external services
  - Implementing write-through caches with batch persistence
  - Collecting metrics or logs for bulk processing

  ## Features

  - **Automatic flushing** based on buffer size or time intervals
  - **Multiple partitions** for concurrent data processing
  - **Configurable jitter** to prevent thundering herd problems
  - **Telemetry integration** for monitoring and observability
  - **Graceful shutdown** with automatic flush on termination
  - **Backpressure handling** with configurable timeouts

  ## Usage

  To use DataBuffer, you need to define a module that implements the `DataBuffer` behaviour:

      defmodule MyApp.MyBuffer do
        use DataBuffer

        def start_link(opts) do
          DataBuffer.start_link(__MODULE__, opts)
        end

        @impl DataBuffer
        def handle_flush(data_stream, opts) do
          # Process the buffered data
          # data_stream is an Enumerable of all buffered items
          # opts contains metadata and flush information

          data = Enum.to_list(data_stream)
          MyApp.Database.bulk_insert(data)
          :ok
        end
      end

  Then start your buffer as part of your supervision tree:

      children = [
        {MyApp.MyBuffer,
         name: MyApp.MyBuffer,
         partitions: 4,
         max_size: 1000,
         flush_interval: 5000}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  ## Configuration Options

  - `:name` - The name to register the buffer process (required)
  - `:partitions` - Number of partition processes (default: 1)
  - `:max_size` - Maximum items before automatic flush (default: 5000)
  - `:max_size_jitter` - Random jitter added to max_size (default: 0)
  - `:flush_interval` - Time in ms between automatic flushes (default: 10000)
  - `:flush_jitter` - Random jitter added to flush_interval (default: 2000)
  - `:flush_timeout` - Timeout in ms for flush operations (default: 60000)
  - `:flush_meta` - Metadata passed to handle_flush callback (optional)

  ## Telemetry Events

  DataBuffer emits the following telemetry events:

  - `[:data_buffer, :insert, :start]` - When an insert operation starts
  - `[:data_buffer, :insert, :stop]` - When an insert operation completes
  - `[:data_buffer, :flush, :start]` - When a flush operation starts
  - `[:data_buffer, :flush, :stop]` - When a flush operation completes

  Each event includes metadata about the buffer, partition, and operation.
  """

  use Supervisor

  @type t :: module()

  @callback handle_flush(Enumerable.t(), meta :: any()) :: any()

  alias DataBuffer.{Partition, PartitionPool, Telemetry}

  ################################
  # Public API
  ################################

  @doc """
  Starts the data buffer as a supervisor process.

  This function starts a DataBuffer supervisor that manages partition and flusher processes.
  The buffer parameter should be the module implementing the DataBuffer behaviour.

  ## Options

  See the module documentation for a complete list of configuration options.

  ## Examples

      DataBuffer.start_link(MyBuffer, name: MyBuffer, partitions: 2, max_size: 500)

  """
  @spec start_link(buffer :: DataBuffer.t(), keyword()) :: Supervisor.on_start()
  def start_link(buffer, opts) do
    Supervisor.start_link(__MODULE__, {buffer, opts}, name: buffer)
  end

  @doc """
  Inserts a single data item into the buffer.

  The data is added to one of the buffer's partitions using round-robin distribution.
  If the partition reaches its `max_size` threshold, it will automatically trigger a flush.

  ## Parameters

  - `buffer` - The buffer module or registered name
  - `data` - Any term to be buffered
  - `timeout` - Operation timeout in milliseconds (default: 5000)

  ## Examples

      DataBuffer.insert(MyBuffer, %{id: 1, name: "Alice"})
      DataBuffer.insert(MyBuffer, "log entry", 10_000)

  """
  @spec insert(buffer :: DataBuffer.t(), data :: any(), timeout()) :: :ok
  def insert(buffer, data, timeout \\ 5_000) do
    Telemetry.span(:insert, %{buffer: buffer}, fn ->
      partition = PartitionPool.next(buffer)
      result = Partition.insert(partition, data, timeout)
      {result, %{buffer: buffer, partition: partition}}
    end)
  end

  @doc """
  Inserts multiple data items into the buffer as a batch.

  All items in the batch are inserted into the same partition atomically.
  This is more efficient than calling `insert/3` multiple times for bulk operations.

  ## Parameters

  - `buffer` - The buffer module or registered name
  - `data` - An enumerable of items to buffer
  - `timeout` - Operation timeout in milliseconds (default: 5000)

  ## Examples

      users = [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}]
      DataBuffer.insert_batch(MyBuffer, users)

      DataBuffer.insert_batch(MyBuffer, 1..1000)

  """
  @spec insert_batch(buffer :: DataBuffer.t(), data :: Enumerable.t(), timeout()) :: :ok
  def insert_batch(buffer, data, timeout \\ 5_000) do
    Telemetry.span(:insert, %{buffer: buffer}, fn ->
      partition = PartitionPool.next(buffer)
      result = Partition.insert_batch(partition, data, timeout)
      {result, %{buffer: buffer, partition: partition}}
    end)
  end

  @doc """
  Triggers an immediate flush of all partitions in the buffer.

  This function initiates flush operations on all partitions asynchronously.
  It returns `:ok` once all flush operations have been started, but doesn't
  wait for them to complete. Use `sync_flush/2` if you need to wait for completion.

  ## Parameters

  - `buffer` - The buffer module or registered name
  - `timeout` - Operation timeout in milliseconds (default: 5000)

  ## Examples

      DataBuffer.flush(MyBuffer)
      DataBuffer.flush(MyBuffer, 10_000)

  """
  @spec flush(buffer :: DataBuffer.t(), timeout()) :: :ok
  def flush(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer) do
      Partition.flush(partition, timeout)
    end

    :ok
  end

  @doc """
  Synchronously flushes all partitions and returns the results.

  Unlike `flush/2`, this function waits for all flush operations to complete
  and returns the results from each partition's `handle_flush/2` callback.

  ## Parameters

  - `buffer` - The buffer module or registered name
  - `timeout` - Operation timeout in milliseconds (default: 5000)

  ## Returns

  A list containing the return values from each partition's flush operation.

  ## Examples

      results = DataBuffer.sync_flush(MyBuffer)
      # results might be [:ok, :ok, :ok, :ok] for 4 partitions

  """
  @spec sync_flush(buffer :: DataBuffer.t(), timeout()) :: [any()]
  def sync_flush(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer), reduce: [] do
      results -> [Partition.sync_flush(partition, timeout) | results]
    end
  end

  @doc """
  Dumps all buffered data without triggering a flush.

  This function returns all data currently buffered across all partitions
  without processing it through the `handle_flush/2` callback. This is useful
  for debugging or inspecting the buffer's contents.

  ## Parameters

  - `buffer` - The buffer module or registered name
  - `timeout` - Operation timeout in milliseconds (default: 5000)

  ## Returns

  A flat list of all buffered items across all partitions.

  ## Examples

      data = DataBuffer.dump(MyBuffer)
      IO.inspect(data, label: "Buffered items")

  """
  @spec dump(buffer :: DataBuffer.t(), timeout()) :: [any()]
  def dump(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer), reduce: [] do
      data -> data ++ Partition.dump(partition, timeout)
    end
  end

  @doc """
  Returns the total number of items currently buffered.

  This function sums the sizes of all partitions to give the total buffer size.

  ## Parameters

  - `buffer` - The buffer module or registered name
  - `timeout` - Operation timeout in milliseconds (default: 5000)

  ## Returns

  The total number of items across all partitions.

  ## Examples

      count = DataBuffer.size(MyBuffer)
      IO.puts("Buffer contains \#{count} items")

  """
  @spec size(buffer :: DataBuffer.t(), timeout()) :: integer()
  def size(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer), reduce: 0 do
      size -> size + Partition.size(partition, timeout)
    end
  end

  @doc """
  Returns detailed information about each partition in the buffer.

  This function provides insight into the state of each partition, including
  current size, flush thresholds, and timing configuration.

  ## Parameters

  - `buffer` - The buffer module or registered name
  - `timeout` - Operation timeout in milliseconds (default: 5000)

  ## Returns

  A list of maps, one for each partition, containing:
  - `:name` - The partition's registered name
  - `:pid` - The partition's process ID
  - `:size` - Current number of buffered items
  - `:flush_size` - Size threshold for automatic flush
  - `:flush_interval` - Time interval for automatic flush
  - `:flush_jitter` - Random jitter for flush interval
  - `:flush_timeout` - Timeout for flush operations

  ## Examples

      info = DataBuffer.info(MyBuffer)
      Enum.each(info, fn partition ->
        IO.puts("Partition \#{partition.name}: \#{partition.size} items")
      end)

  """

  @spec info(buffer :: DataBuffer.t()) :: [map()]
  @spec info(buffer :: DataBuffer.t(), timeout()) :: [map()]
  def info(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer) do
      Partition.info(partition, timeout)
    end
  end

  ################################
  # Supervisor Callbacks
  ################################

  @impl Supervisor
  def init({buffer, opts}) do
    children = [
      {DataBuffer.FlusherPool, buffer},
      {DataBuffer.PartitionPool, [buffer, opts]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defmacro __using__(_opts) do
    quote location: :keep do
      @behaviour DataBuffer

      if Module.get_attribute(__MODULE__, :doc) == nil do
        @doc """
        Returns a specification to start this buffer under a supervisor.

        See `Supervisor`.
        """
      end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      defoverridable(child_spec: 1)
    end
  end
end
