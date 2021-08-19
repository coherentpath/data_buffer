defmodule DataBuffer.PartitionPool do
  @moduledoc false

  use Supervisor

  @atomics_ix 1
  @counter_max 2_000_000_000
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
      start: {__MODULE__, :start_link, opts},
      type: :supervisor
    }
  end

  @spec start_link(DataBuffer.t(), keyword()) :: Supervisor.on_start()
  def start_link(buffer, opts) do
    name = partition_pool_name(buffer)

    with {:ok, pool_opts} <- validate_opts(opts) do
      Supervisor.start_link(__MODULE__, {buffer, pool_opts, opts}, name: name)
    end
  end

  @spec next(DataBuffer.t()) :: atom()
  def next(buffer) do
    {rotation, partition_count, partition_names} = partition_info(buffer)
    counter = get_counter(rotation)
    partition_ix = rem(counter, partition_count)
    Enum.at(partition_names, partition_ix)
  end

  @spec all(DataBuffer.t()) :: [atom()]
  def all(buffer) do
    {_, _, partition_names} = partition_info(buffer)
    partition_names
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

  defp partition_pool_name(buffer), do: :"#{buffer}.PartitionPool"

  defp partition_name(buffer, partition), do: :"#{buffer}.Partition.#{partition}"

  defp validate_opts(opts) do
    KeywordValidator.validate(opts, @opts_schema, strict: false)
  end

  defp init_config(buffer, pool_opts) do
    rotation = :atomics.new(@atomics_ix, [])
    partition_count = Keyword.fetch!(pool_opts, :partitions)
    partition_names = Enum.map(1..partition_count, &partition_name(buffer, &1))
    paritions = {rotation, partition_count, partition_names}
    :persistent_term.put({buffer, :partitions}, paritions)
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

  defp partition_info(buffer), do: :persistent_term.get({buffer, :partitions})

  defp get_counter(rotation) do
    case :atomics.add_get(rotation, @atomics_ix, 1) do
      counter when counter >= @counter_max ->
        :atomics.put(rotation, @atomics_ix, 0)
        counter

      counter ->
        counter
    end
  end
end
