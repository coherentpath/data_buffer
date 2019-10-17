defmodule DataBufferTest do
  use ExUnit.Case, async: false

  defmodule TestBufferOne do
    use DataBuffer, interval: 1_000
  end

  defmodule TestBufferTwo do
    use DataBuffer, interval: 1_000
  end

  defmodule TestBufferThree do
    use DataBuffer, interval: 100

    def handle_flush(key, data) do
      send(:data_buffer_test, {key, data})
    end
  end

  defmodule TestBufferFour do
    use DataBuffer, timeout: 100
  end

  defmodule TestBufferFive do
    use DataBuffer
  end

  defmodule TestBufferSix do
    use DataBuffer, interval: 0, retry_delay: 0, retry_max: 5

    def handle_flush(key, count) do
      send(:data_buffer_test, {key, count})
      raise "error"
    end
  end

  defmodule TestBufferSeven do
    use DataBuffer, interval: 0, retry_delay: 200, retry_max: 5

    def handle_flush(key, _count) do
      send(:data_buffer_test, {key, :os.system_time(:millisecond)})
      :error
    end
  end

  defmodule TestBufferEight do
    use DataBuffer, max_size: 1, interval: 60_000

    def handle_flush(key, count) do
      send(:data_buffer_test, {key, count})
      :ok
    end
  end

  describe "insert/2" do
    test "accepts a binary as a key" do
      start_supervised(TestBufferOne)
      TestBufferOne.insert("foo", "bar")
      assert_key_exists(TestBufferOne, "foo")
    end

    test "accepts a tuple as a key" do
      start_supervised(TestBufferOne)
      TestBufferOne.insert({"foo", "bar"}, "foo")
      assert_key_exists(TestBufferOne, {"foo", "bar"})
    end

    test "accepts an integer as a key" do
      start_supervised(TestBufferOne)
      TestBufferOne.insert(1, "foo")
      assert_key_exists(TestBufferOne, 1)
    end

    test "accepts an atom as a key" do
      start_supervised(TestBufferOne)
      TestBufferOne.insert(:foo, "foo")
      assert_key_exists(TestBufferOne, :foo)
    end

    test "maintains a proper count for a single key" do
      start_supervised(TestBufferOne)
      TestBufferOne.insert("foo", "foo")
      TestBufferOne.insert("foo", "foo")
      TestBufferOne.insert("foo", "foo")
      assert_key_count(TestBufferOne, "foo", 3)
    end

    test "data returned is a list of values without keys" do
      start_supervised(TestBufferThree)
      Process.register(self(), :data_buffer_test)

      for _ <- 1..10 do
        spawn(fn -> TestBufferThree.insert("foo", "foo") end)
      end

      :timer.sleep(200)

      assert_receive({"foo", data})
      assert Enum.count(data) == 10

      Enum.each(data, fn item ->
        assert item == "foo"
      end)
    end

    test "maintains a proper count for a single key with concurrency" do
      start_supervised(TestBufferOne)

      for _ <- 1..10_000 do
        spawn(fn -> TestBufferOne.insert("foo", "foo") end)
        spawn(fn -> TestBufferOne.insert("bar", "bar") end)
      end

      await_count(TestBufferOne, "foo", 10_000)
      await_count(TestBufferOne, "bar", 10_000)

      assert_key_count(TestBufferOne, "foo", 10_000)
      assert_key_count(TestBufferOne, "bar", 10_000)
    end

    test "does not mix keys from different buffers" do
      start_supervised(TestBufferOne)
      start_supervised(TestBufferTwo)

      TestBufferOne.insert("foo", "foo")
      assert_key_count(TestBufferOne, "foo", 1)

      TestBufferTwo.insert("foo", "foo")
      assert_key_count(TestBufferTwo, "foo", 1)
    end

    test "will cause a flush after the specified interval" do
      start_supervised(TestBufferThree)
      Process.register(self(), :data_buffer_test)
      TestBufferThree.insert("foo", "bar")
      TestBufferThree.insert("foo", "bar")
      :timer.sleep(200)

      assert_receive({"foo", ["bar", "bar"]})
    end

    test "will cause the worker to hibernate after the specified timeout" do
      start_supervised(TestBufferFour)
      TestBufferFour.insert("foo", "foo")
      :timer.sleep(200)
      pid = Process.whereis(TestBufferFour)
      info = Process.info(pid)

      assert info[:current_function] == {:erlang, :hibernate, 3}
    end

    test "will cause the worker to flush if the key hits max_size" do
      start_supervised(TestBufferEight)
      Process.register(self(), :data_buffer_test)
      TestBufferEight.insert("foo", "foo")

      assert_receive({"foo", ["foo"]})
    end

    @tag capture_log: true
    test "will return :error if the process isnt alive" do
      assert :error = TestBufferFive.insert("foo", "foo")
    end

    @tag capture_log: true
    test "will retry failed flush operations" do
      start_supervised(TestBufferSix)
      Process.register(self(), :data_buffer_test)
      TestBufferSix.insert("foo", "foo")

      :timer.sleep(100)

      assert_receive({"foo", ["foo"]})
      assert_receive({"foo", ["foo"]})
      assert_receive({"foo", ["foo"]})
      assert_receive({"foo", ["foo"]})
      assert_receive({"foo", ["foo"]})
      refute_receive({"foo", ["foo"]})
    end

    @tag capture_log: true
    test "will retry with backoff flush operations that dont return :ok " do
      start_supervised(TestBufferSeven)
      Process.register(self(), :data_buffer_test)
      TestBufferSeven.insert("foo", "foo")

      :timer.sleep(100)

      assert_receive({"foo", time1}, 500)
      assert_receive({"foo", time2}, 500)
      assert time2 - time1 > 200
      assert_receive({"foo", time1}, 500)
      assert_receive({"foo", time2}, 500)
      assert time2 - time1 > 200
    end
  end

  def assert_key_exists(buffer, key) do
    assert buffer.count(key) |> is_integer()
  end

  def assert_key_count(buffer, key, count) do
    assert buffer.count(key) == count
  end

  def await_count(buffer, key, count) do
    :timer.sleep(10)

    case buffer.count(key) do
      ^count -> :ok
      _ -> await_count(buffer, key, count)
    end
  end
end
