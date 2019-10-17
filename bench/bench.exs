defmodule BufferOne do
  use DataBuffer, interval: 60_000, partitions: 8
end

defmodule BufferTwo do
  use DataBuffer, interval: 60_000, partitions: 1
end

keys = 1..10_000

BufferOne.start_link()
BufferTwo.start_link()

IO.puts("** Parallel Bench **\n")

Benchee.run(
  %{
    "insert - multi partition" => fn key -> BufferOne.insert(key, "bar") end,
    "insert - single partition" => fn key -> BufferTwo.insert(key, "bar") end
  },
  parallel: System.schedulers_online(),
  before_each: fn _ -> Enum.random(keys) end
)

Supervisor.stop(BufferOne)
Supervisor.stop(BufferTwo)

BufferOne.start_link()
BufferTwo.start_link()

IO.puts("** Non-Parallel Bench **\n")

Benchee.run(
  %{
    "insert - multi partition" => fn key -> BufferOne.insert(key, "bar") end,
    "insert - single partition" => fn key -> BufferTwo.insert(key, "bar") end
  },
  before_each: fn _ -> Enum.random(keys) end
)
