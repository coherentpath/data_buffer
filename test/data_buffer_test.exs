defmodule DataBufferTest do
  use ExUnit.Case, async: false

  import DataBuffer.Helpers
  import ExUnit.CaptureLog

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

    test "will flush after hitting max_size + max_size_jitter" do
      start_buffer(max_size: 0, max_size_jitter: 1)
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

    test "flushes in the order inserted" do
      start_buffer(partitions: 1)

      for x <- 0..500 do
        DataBuffer.insert(TestBuffer, x)
      end

      DataBuffer.flush(TestBuffer)

      assert_receive {:data, data}

      for x <- 0..500 do
        assert Enum.at(data, x) == x
      end
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

  test "handles flush attempts that raise an exception or exit" do
    assert capture_log(fn ->
             start_buffer(
               buffer: TestErrorBuffer,
               max_size: 1,
               partitions: 1,
               flush_meta: %{kind: :error}
             )

             DataBuffer.insert(TestErrorBuffer, "foo")
             DataBuffer.insert(TestErrorBuffer, "foo")
             stop_supervised!(TestErrorBuffer)
           end) =~ "(RuntimeError) boom"

    assert capture_log(fn ->
             start_buffer(
               buffer: TestErrorBuffer,
               max_size: 1,
               partitions: 1,
               flush_meta: %{kind: :exit}
             )

             DataBuffer.insert(TestErrorBuffer, "foo")
             DataBuffer.insert(TestErrorBuffer, "foo")
             stop_supervised!(TestErrorBuffer)
           end) =~ "(exit) \"boom\""
  end

  test "will handle an insert when waiting on a timeout" do
    assert capture_log(fn ->
             start_buffer(
               max_size: 1,
               partitions: 1,
               flush_timeout: 250,
               flush_meta: %{sleep: 500}
             )

             DataBuffer.insert(TestBuffer, "foo")
             DataBuffer.insert(TestBuffer, "foo")
             DataBuffer.insert(TestBuffer, "foo")
           end) =~ "DataBuffer: flush timeout error"
  end

  defp receive_all(partitions \\ partitions()) do
    for _ <- 1..partitions, reduce: [] do
      data ->
        assert_receive {:data, new_data}
        data ++ new_data
    end
  end

  defp await_size(size) do
    :timer.sleep(10)

    case DataBuffer.size(TestBuffer) do
      ^size -> :ok
      _ -> await_size(size)
    end
  end
end
