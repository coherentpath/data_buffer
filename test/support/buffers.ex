defmodule TestBuffer do
  use DataBuffer

  def start_link(opts) do
    DataBuffer.start_link(__MODULE__, opts)
  end

  @impl DataBuffer
  def handle_flush(data_stream, opts) do
    meta = Keyword.get(opts, :meta)
    size = Keyword.get(opts, :size)

    if Map.has_key?(meta, :sleep) do
      :timer.sleep(meta.sleep)
    end

    data = Enum.into(data_stream, [])
    send(meta.pid, {:data, data, size})
  end
end

defmodule TestErrorBuffer do
  use DataBuffer

  def start_link(opts) do
    DataBuffer.start_link(__MODULE__, opts)
  end

  @impl DataBuffer
  def handle_flush(_data_stream, opts) do
    opts
    |> Keyword.get(:meta, %{})
    |> Map.get(:kind)
    |> case do
      :error -> raise RuntimeError, "boom"
      :exit -> exit("boom")
      _ -> :ok
    end
  end
end
