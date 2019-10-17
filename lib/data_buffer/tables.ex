defmodule DataBuffer.Tables do
  @moduledoc false

  @default_opts [
    :public,
    :named_table,
    write_concurrency: true,
    read_concurrency: true
  ]

  ################################
  # Public API
  ################################

  @spec name(:buffer | :counter, atom(), integer()) :: atom()
  def name(:buffer, name, partition), do: :"#{name}.#{partition}"
  def name(:counter, name, partition), do: :"#{name}.Counter.#{partition}"

  @spec init(:buffer | :counter, atom(), integer()) :: atom()
  def init(:buffer, name, p) do
    name = name(:buffer, name, p)
    init_table(name, :duplicate_bag)
  end

  def init(:counter, name, p) do
    name = name(:counter, name, p)
    init_table(name, :set)
  end

  ################################
  # Private API
  ################################

  defp init_table(name, type) do
    opts = [type | @default_opts]

    case :ets.info(name) do
      :undefined -> :ets.new(name, opts)
      _ -> name
    end
  end
end
