defmodule Buffer do
  use DataBuffer, interval: 60_000, partitions: 8
end

keys = 1..10_000

Buffer.start_link()

Benchee.run(
  %{
    "insert" => fn key -> Buffer.insert(key, "bar") end
  },
  parallel: System.schedulers_online(),
  before_each: fn _ -> Enum.random(keys) end
)

Benchee.run(%{
  "insert" => fn -> Buffer.insert("foo", "bar") end
})
