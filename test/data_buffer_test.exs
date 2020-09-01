defmodule DataBufferTest do
  use ExUnit.Case, async: false

  @partitions 2

  defmodule TestBuffer do
    use DataBuffer

    def start_link(opts) do
      DataBuffer.start_link(__MODULE__, opts)
    end

    @impl DataBuffer
    def handle_flush(data_stream, meta) do
      data = Enum.into(data_stream, [])
      send(meta.pid, {:data, data})
    end
  end

  describe "insert/2" do
    test "inserts data into the buffer" do
      start_buffer()

      assert [] = DataBuffer.dump(TestBuffer)
      DataBuffer.insert(TestBuffer, "foo")
      assert [{_, "foo"}] = DataBuffer.dump(TestBuffer)
    end

    test "inserts duplicate data into the buffer" do
      start_buffer()
      assert [] = DataBuffer.dump(TestBuffer)

      for _ <- 1..500 do
        DataBuffer.insert(TestBuffer, "foo")
      end

      assert length(DataBuffer.dump(TestBuffer)) == 500
    end

    test "will flush after hitting max_size" do
      start_buffer(max_size: 1)
      assert [] = DataBuffer.dump(TestBuffer)
      DataBuffer.insert(TestBuffer, "foo")
      DataBuffer.insert(TestBuffer, "foo")
      DataBuffer.insert(TestBuffer, "foo")
      DataBuffer.insert(TestBuffer, "foo")
      assert_receive {:data, ["foo"]}
      assert_receive {:data, ["foo"]}
    end
  end

  describe "flush/2" do
    test "flushes data from the buffer" do
      start_buffer()

      DataBuffer.insert(TestBuffer, "foo")
      DataBuffer.flush(TestBuffer)

      assert_receive {:data, ["foo"]}
    end

    test "flushes duplicate data from the buffer" do
      start_buffer()

      for _ <- 1..500 do
        DataBuffer.insert(TestBuffer, "foo")
      end

      DataBuffer.flush(TestBuffer)
      data = receive_all()

      assert length(data) == 500
    end
  end

  describe "size/1" do
    test "returns the correct size" do
      start_buffer(max_size: 50_000)

      for _ <- 1..10_000 do
        spawn(fn -> DataBuffer.insert(TestBuffer, "foo") end)
        spawn(fn -> DataBuffer.insert(TestBuffer, "bar") end)
      end

      await_size(20_000)

      assert DataBuffer.size(TestBuffer) == 20_000
    end
  end

  test "flushes data after flush_interval and flush_jitter" do
    start_buffer(flush_interval: 50, flush_jitter: 1)
    DataBuffer.insert(TestBuffer, "foo")
    assert_receive {:data, ["foo"]}, 150
  end

  defp start_buffer(opts \\ []) do
    default_opts = [flush_meta: %{pid: self()}, partitions: @partitions]
    opts = Keyword.merge(default_opts, opts)
    start_supervised({TestBuffer, opts})
  end

  defp receive_all do
    for _ <- 1..@partitions, reduce: [] do
      data ->
        assert_receive {:data, new_data}
        data ++ new_data
    end
  end

  def await_size(size) do
    :timer.sleep(10)

    case DataBuffer.size(TestBuffer) do
      ^size -> :ok
      _ -> await_size(size)
    end
  end
end
