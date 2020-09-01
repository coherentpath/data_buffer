defmodule DataBuffer do
  @moduledoc """
  Defines a data buffer.
  """

  use Supervisor

  @type buffer :: module()

  @callback handle_flush(Enumerable.t(), meta :: any()) :: any()

  alias DataBuffer.{Partition, PartitionPool}

  ################################
  # Public API
  ################################

  @spec start_link(DataBuffer.t(), keyword()) :: Supervisor.on_start()
  def start_link(buffer, opts) do
    Supervisor.start_link(__MODULE__, {buffer, opts}, name: buffer)
  end

  @spec insert(DataBuffer.t(), any()) :: :ok
  def insert(buffer, data) do
    buffer |> PartitionPool.get() |> Partition.insert(data)
  end

  @spec flush(DataBuffer.t()) :: :ok
  def flush(buffer) do
    for partition <- PartitionPool.all(buffer), do: Partition.flush(partition)
    :ok
  end

  @spec dump(DataBuffer.t()) :: :ok
  def dump(buffer) do
    for partition <- PartitionPool.all(buffer), reduce: [] do
      data -> data ++ Partition.dump(partition)
    end
  end

  @spec size(DataBuffer.t()) :: integer()
  def size(buffer) do
    for partition <- PartitionPool.all(buffer), reduce: 0 do
      size -> size + Partition.size(partition)
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
