defmodule Test do
  def start_link do
    DataBuffer.start_link(__MODULE__, max_size: 10_000, partitions: 8)
  end

  def handle_flush(_data, _meta) do
    :ok
  end
end

Test.start_link()
words = Enum.reduce(1..5000, "", fn item, acc -> "#{acc}#{item}" end)

Benchee.run(%{
  "test" => fn ->
    DataBuffer.insert(Test, words)
  end
})
