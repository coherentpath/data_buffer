defmodule DataBuffer.TelemetryTest do
  use ExUnit.Case, async: false

  import DataBuffer.Helpers

  test "emits [:data_buffer, :insert, :start]" do
    pid = self()

    :telemetry.attach(
      :insert_start,
      [:data_buffer, :insert, :start],
      fn event, measurements, meta, _ ->
        assert event == [:data_buffer, :insert, :start]
        assert Map.has_key?(measurements, :system_time)
        assert meta.buffer == TestBuffer
        send(pid, :insert_start)
      end,
      []
    )

    start_buffer()
    DataBuffer.insert(TestBuffer, "foo")

    assert_receive :insert_start
  after
    :telemetry.detach(:insert_start)
  end

  test "emits [:data_buffer, :insert, :stop]" do
    pid = self()

    :telemetry.attach(
      :insert_stop,
      [:data_buffer, :insert, :stop],
      fn event, measurements, meta, _ ->
        assert event == [:data_buffer, :insert, :stop]
        assert Map.has_key?(measurements, :duration)
        assert meta.buffer == TestBuffer
        send(pid, :insert_stop)
      end,
      []
    )

    start_buffer()
    DataBuffer.insert(TestBuffer, "foo")

    assert_receive :insert_stop
  after
    :telemetry.detach(:insert_stop)
  end

  test "emits [:data_buffer, :flush, :start]" do
    pid = self()

    :telemetry.attach(
      :flush_start,
      [:data_buffer, :flush, :start],
      fn event, measurements, meta, _ ->
        assert event == [:data_buffer, :flush, :start]
        assert Map.has_key?(measurements, :system_time)
        assert meta.buffer == TestBuffer
        send(pid, :flush_start)
      end,
      []
    )

    start_buffer(partitions: 1, max_size: 1)
    DataBuffer.insert(TestBuffer, "foo")
    DataBuffer.insert(TestBuffer, "foo")

    assert_receive :flush_start
  after
    :telemetry.detach(:flush_start)
  end

  test "emits [:data_buffer, :flush, :stop]" do
    pid = self()

    :telemetry.attach(
      :flush_stop,
      [:data_buffer, :flush, :stop],
      fn event, measurements, meta, _ ->
        assert event == [:data_buffer, :flush, :stop]
        assert Map.has_key?(measurements, :duration)
        assert meta.buffer == TestBuffer
        send(pid, :flush_stop)
      end,
      []
    )

    start_buffer(partitions: 1, max_size: 1)
    DataBuffer.insert(TestBuffer, "foo")
    DataBuffer.insert(TestBuffer, "foo")

    assert_receive :flush_stop
  after
    :telemetry.detach(:flush_stop)
  end
end
