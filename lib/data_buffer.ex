defmodule DataBuffer do
  @moduledoc """
  Defines a data buffer.
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
  """
  @spec start_link(buffer :: DataBuffer.t(), keyword()) :: Supervisor.on_start()
  def start_link(buffer, opts) do
    Supervisor.start_link(__MODULE__, {buffer, opts}, name: buffer)
  end

  @doc """
  Inserts `data` into the provided `buffer`.
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
  Performs a flush operation on the provided `buffer`.
  """
  @spec flush(buffer :: DataBuffer.t(), timeout()) :: :ok
  def flush(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer) do
      Partition.flush(partition, timeout)
    end

    :ok
  end

  @doc """
  Syncronously flushes the provided `buffer` - returning the results.
  """
  @spec sync_flush(buffer :: DataBuffer.t(), timeout()) :: [any()]
  def sync_flush(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer), reduce: [] do
      results -> [Partition.sync_flush(partition, timeout) | results]
    end
  end

  @doc """
  Dumps data from the provided `buffer` - bypassing the flush operation.
  """
  @spec dump(buffer :: DataBuffer.t(), timeout()) :: [any()]
  def dump(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer), reduce: [] do
      data -> data ++ Partition.dump(partition, timeout)
    end
  end

  @doc """
  Returns the current size of the provided `buffer`.
  """
  @spec size(buffer :: DataBuffer.t(), timeout()) :: integer()
  def size(buffer, timeout \\ 5_000) do
    for partition <- PartitionPool.all(buffer), reduce: 0 do
      size -> size + Partition.size(partition, timeout)
    end
  end

  @doc """
  Returns the details of each partition within the provided `buffer`.
  """
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

  defmacro __using__(opts) do
    quote location: :keep do
      @behaviour DataBuffer

      if Module.get_attribute(__MODULE__, :doc) == nil do
        @doc """
        Returns a specification to start this buffer under a supervisor.

        See `Supervisor`.
        """
      end

      def child_spec(opts) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable(child_spec: 1)
    end
  end
end
