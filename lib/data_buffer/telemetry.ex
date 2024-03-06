defmodule DataBuffer.Telemetry do
  @moduledoc """
  DataBuffer produces multiple telemetry events.

  ## Events

  * `[:data_buffer, :insert, :start]` - Called when a buffer insert starts.

    #### Measurements
      * `:system_time` - The current monotonic system time.

    #### Metadata
      * `:buffer` - The name of the buffer.

  * `[:data_buffer, :insert, :stop]` - Called when a buffer insert stops.

    #### Measurements
      * `:duration` - The amount of time taken to insert data to the buffer.

    #### Metadata
      * `:buffer` - The name of the buffer.

  * `[:data_buffer, :insert, :exception]` - Called when a buffer insert has an exception.

    #### Measurements
      * `:duration` - The amount of time before the error occurred.

    #### Metadata
      * `:buffer` - The name of the buffer.
      * `:kind` - The kind of error raised.
      * `:reason` - The reason for the error.
      * `:stacktrace` - The stacktrace of the error.

  * `[:data_buffer, :flush, :start]` - Called when a buffer flush starts.

    #### Measurements
      * `:system_time` - The current monotonic system time.

    #### Metadata
      * `:buffer` - The name of the buffer.
      * `:size` - The buffer size.

  * `[:data_buffer, :flush, :stop]` - Called when a buffer flush stops.

    #### Measurements
      * `:duration` - The amount of time taken to flush the buffer.

    #### Metadata
      * `:buffer` - The name of the buffer.

  * `[:data_buffer, :flush, :exception]` - Called when a buffer flush has an exception.

    #### Measurements
      * `:duration` - The amount of time before the error occurred.

    #### Metadata
      * `:buffer` - The name of the buffer.
      * `:kind` - The kind of error raised.
      * `:reason` - The reason for the error.
      * `:stacktrace` - The stacktrace of the error.
  """

  @doc false
  @spec span(atom(), map(), (-> {any, map})) :: any()
  def span(name, meta, fun) do
    :telemetry.span([:data_buffer, name], meta, fun)
  end
end
