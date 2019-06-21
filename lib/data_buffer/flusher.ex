defmodule DataBuffer.Flusher do
  @moduledoc false

  require Logger

  alias DataBuffer.Tables

  @type option :: {:backoff, non_neg_integer()}
  @type options :: [option()]

  ################################
  # Public API
  ################################

  @doc false
  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, args},
      restart: :transient
    }
  end

  @spec start_link(buffer :: DataBuffer.t(), key :: any(), partition :: integer(), options()) ::
          {:ok, pid()}
  def start_link(buffer, key, partition, opts \\ []) do
    Task.start_link(__MODULE__, :run, [buffer, key, partition, opts])
  end

  @spec run(buffer :: DataBuffer.t(), key :: any(), partition :: integer(), options()) ::
          :error | :ok
  def run(buffer, key, partition, opts) do
    retry_max = Keyword.get(opts, :retry_max, 5)
    retry_delay = Keyword.get(opts, :retry_delay, 1_000)

    with {:ok, data} <- get_data(buffer, key, partition) do
      do_run(buffer, key, data, {retry_delay, retry_max, 1})
    end
  end

  defp do_run(buffer, key, data, retry) do
    do_flush(buffer, key, data)
  rescue
    exception -> do_error(buffer, key, data, retry, exception, __STACKTRACE__)
  end

  defp get_data(buffer, key, partition) do
    buffer_table = Tables.name(:buffer, buffer, partition)
    counter_table = Tables.name(:counter, buffer, partition)

    :ets.take(counter_table, key)

    case :ets.take(buffer_table, key) do
      [] -> :ok
      data -> {:ok, Enum.map(data, fn {_key, val} -> val end)}
    end
  end

  defp do_flush(buffer, key, data) do
    case apply(buffer, :handle_flush, [key, data]) do
      :ok -> :ok
      invalid -> raise DataBuffer.Error, "invalid return value: #{inspect(invalid)}"
    end
  end

  defp do_error(buffer, key, data, retry, exception, stack) do
    {retry_delay, retry_max, attempt} = retry

    if attempt >= retry_max do
      log_error(exception, stack)
      apply(buffer, :handle_error, [key, data])
      :error
    else
      :timer.sleep(retry_delay)
      do_run(buffer, key, data, {retry_delay, retry_max, attempt + 1})
    end
  end

  defp log_error(exception, stack) do
    error = Exception.format(:error, exception, stack)
    Logger.error(error)
  end
end
