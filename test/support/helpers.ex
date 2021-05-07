defmodule DataBuffer.Helpers do
  import ExUnit.Callbacks, only: [start_supervised: 1]

  @partitions 2

  def partitions, do: @partitions

  def start_buffer(opts \\ []) do
    {buffer, opts} = Keyword.pop(opts, :buffer, TestBuffer)
    {flush_meta, opts} = Keyword.pop(opts, :flush_meta, %{})
    default_opts = [flush_meta: Map.merge(flush_meta, %{pid: self()}), partitions: @partitions]
    opts = Keyword.merge(default_opts, opts)
    start_supervised({buffer, opts})
  end
end
