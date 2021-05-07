defmodule DataBuffer.Flusher do
  @moduledoc false

  use GenServer

  require Logger

  alias DataBuffer.Telemetry

  ################################
  # Public API
  ################################

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, opts},
      restart: :temporary
    }
  end

  @spec start_link(DataBuffer.t(), keyword()) :: GenServer.on_start()
  def start_link(buffer, opts \\ []) do
    GenServer.start_link(__MODULE__, {buffer, opts})
  end

  @spec flush(DataBuffer.Partition.table(), atom(), keyword()) :: {:ok, any()} | {:error, any()}
  def flush(table, buffer, opts \\ []) do
    meta = Keyword.get(opts, :meta)
    data = handle_data(table)
    buffer.handle_flush(data, meta)
  end

  ################################
  # GenServer Callbacks
  ################################

  @impl GenServer
  def init(state) do
    {:ok, state}
  end

  @impl GenServer
  def handle_info({:"ETS-TRANSFER", table, from, size}, {buffer, opts} = state) do
    try do
      Telemetry.span(:flush, %{buffer: buffer, size: size}, fn ->
        flush(table, buffer, opts)
        {:ok, %{buffer: buffer}}
      end)
    catch
      kind, reason -> Logger.error(Exception.format(kind, reason, __STACKTRACE__))
    end

    send(from, :flush_complete)
    {:stop, :normal, state}
  end

  ################################
  # GenServer Callbacks
  ################################

  defp handle_data(table) do
    data = :ets.tab2list(table)
    :ets.delete(table)
    Stream.map(data, fn {_, item} -> item end)
  end
end
