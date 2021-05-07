defmodule TestBuffer do
  use DataBuffer

  def start_link(opts) do
    DataBuffer.start_link(__MODULE__, opts)
  end

  @impl DataBuffer
  def handle_flush(data_stream, meta) do
    if Map.has_key?(meta, :sleep) do
      :timer.sleep(meta.sleep)
    end

    data = Enum.into(data_stream, [])
    send(meta.pid, {:data, data})
  end
end

defmodule TestErrorBuffer do
  use DataBuffer

  def start_link(opts) do
    DataBuffer.start_link(__MODULE__, opts)
  end

  @impl DataBuffer
  def handle_flush(_data_stream, %{kind: :error}) do
    raise RuntimeError, "boom"
  end

  def handle_flush(_data_stream, %{kind: :exit}) do
    exit("boom")
  end
end
