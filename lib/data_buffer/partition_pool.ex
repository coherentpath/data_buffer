defmodule DataBuffer.PartitionPool do
  @moduledoc false

  use Supervisor

  @opts_schema %{
    partitions: [type: :integer, default: 1, required: true]
  }

  ################################
  # Public API
  ################################

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, opts}
    }
  end

  @spec start_link(DataBuffer.buffer(), keyword()) :: Supervisor.on_start()
  def start_link(buffer, opts) do
    name = partition_pool_name(buffer)

    with {:ok, pool_opts} <- validate_opts(opts) do
      Supervisor.start_link(__MODULE__, {buffer, pool_opts, opts}, name: name)
    end
  end

  @spec count(DataBuffer.buffer()) :: integer()
  def count(buffer) do
    :persistent_term.get({buffer, :partitions})
  end

  @spec get(DataBuffer.buffer()) :: atom()
  def get(buffer) do
    parition = buffer |> count() |> :random.uniform()
    partition_name(buffer, parition)
  end

  @spec all(DataBuffer.buffer()) :: [atom()]
  def all(buffer) do
    for partition <- 1..count(buffer), reduce: [] do
      partitions -> [partition_name(buffer, partition) | partitions]
    end
  end

  ################################
  # Supervisor Callbacks
  ################################

  @impl Supervisor
  def init({buffer, pool_opts, opts}) do
    init_config(buffer, pool_opts)
    partitions = build_partitions(buffer, pool_opts, opts)
    Supervisor.init(partitions, strategy: :one_for_one)
  end

  ################################
  # Private API
  ################################

  defp partition_pool_name(buffer), do: :"#{buffer}.WorkerPool"

  defp validate_opts(opts) do
    KeywordValidator.validate(opts, @opts_schema, strict: false)
  end

  defp init_config(buffer, pool_opts) do
    partitions = Keyword.fetch!(pool_opts, :partitions)
    :persistent_term.put({buffer, :partitions}, partitions)
  end

  defp build_partitions(buffer, pool_opts, opts) do
    partitions = Keyword.fetch!(pool_opts, :partitions)

    for partition <- 1..partitions, reduce: [] do
      partitions -> build_partitions(buffer, opts, partition, partitions)
    end
  end

  defp build_partitions(buffer, opts, partition, partitions) do
    opts =
      opts
      |> Keyword.put(:name, partition_name(buffer, partition))
      |> Keyword.put(:buffer, buffer)

    [{DataBuffer.Partition, opts} | partitions]
  end

  defp partition_name(buffer, partition), do: :"#{buffer}.Partition.#{partition}"
end
