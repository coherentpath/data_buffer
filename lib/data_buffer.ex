defmodule DataBuffer do
  @moduledoc """
  Defines a data buffer.

  A data buffer is an efficient way to maintain a local list of values associated
  with a given key that can later be flushed to persistent storage. In fast moving
  systems, this provides a scalable way keep track of values without putting heavy
  loads on a database.

  Creating a buffer is as easy as:

      defmodule Buffer do
        use DataBuffer
      end

  Once we have defined our buffer module, we must then implement the `c:handle_flush/2`
  callback that allows us to perform an operation with a provided key and list of
  values.

  This could mean something like dumping the data to a database.

      defmodule Buffer do
        use DataBuffer

        def handle_flush(key, data) do
          # write to the database...
          # handle_flush MUST return an :ok atom
          :ok
        end
      end

  We must then add our buffer to our supervision tree.
      children = [
        Buffer
      ]

  Each flush operation is handled with its own supervised `Task` process. By
  default, a failed flush operation will retry about 3 times within 3 seconds.

  ## Usage

  With our buffer started, we can now insert values by key. A key can be any
  valid term.

      Buffer.insert("mykey", "myval")

  Key values are maintained in an ETS table. All keys are scoped to the given
  buffer module - so multiple buffers using the same keys will not cause issues.

  With the default buffer we setup above, the "mykey" data will be flushed
  after 5 seconds. Assuming no new operations occur with our buffer, the process
  will be placed into hibernation after 10 seconds. All of this is configurable
  through the options below.

  ## Options

  A data buffer comes with a few configurable options. We can pass any of these
  options along with the use macro.

      use DataBuffer, interval: 60_000, jitter: 20_000

    * `:partitions` - The number of table partitions to use for the buffer - defaults
      to `1`.
    * `:interval` - The time in milliseconds between the first insert for a
      given key and its next flush callback being invoked. Defaults to `5_000`.
    * `:jitter` - A max time in milliseconds that will be added to `interval` to
      ensure some randomness in each flush invocation. The time added would be
      randomly selected between 0 and `jitter`. Defaults to `0`.
    * `:timeout` - The time in milliseconds between the last operation on a
      a buffer, and the process being hibernated. Defaults to `10_000`.
    * `:retry_delay` - The time in milliseconds between a `c:handle_flush/2` callback
      failing, and the next attempt occuring. Defaults to `1_000`.
    * `:retry_max` - The max amount of retry attempts that will occur for the
      `c:handle_flush/2` callback.

  ## Errors

  If the `c:handle_flush/2` callback returns an invalid value or raises an exception
  after `:retry_max` attempts, the `c:handle_error/2` callback will be invoked.

  This callback is provided the same key and data values as `c:handle_flush/2`, but the
  assumption can be made that our normal persistence layer is no longer functional.
  It is then left up to the developer how to best handle this situation.

      def handle_error(key, data) do
        # Put the data back into the buffer...
        # Or put the data to local disk...
        :ok
      end

  """

  @doc """
  Starts the buffer process.

  ## Options

  The options available are the same provided in the "Options" section.

  """
  @callback start_link(options()) :: GenServer.on_start()

  @doc """
  Callback for flushing a key from the buffer.

  When a buffer key hits its set time interval, this function will be called and
  provided with the key as well its current data.

  This function is called within its own Task and is supervised. If the
  callback does not return `:ok` - the task will fail and attempt retries
  based on the `:retry_delay` and `:retry_max` options.
  """
  @callback handle_flush(key :: any(), data :: [any()]) :: :ok

  @doc """
  Callback for handling errors that occur during flush invocation.

  If the flush attempts hit the `:retry_max`, this function will be called and
  provided with the key as well its current data. It is then up to the developer
  to decide how best to handle the data.
  """
  @callback handle_error(key :: any(), data :: [any()]) :: any()

  @doc """
  Inserts a value to the buffer that is associated with a given key.

  Each key is scoped to the buffer module. So duplicate keys across different
  buffer modules will not cause issues.
  """
  @callback insert(key :: any(), value :: any()) :: :ok | :error

  @doc """
  Asynchronously flushes a given key from the buffer.
  """
  @callback flush(key :: any()) :: :ok

  @doc """
  Returns the current number of items associated with a given key from the buffer.
  """
  @callback count(key :: any()) :: integer() | nil

  @type t :: module
  @type option ::
          {:interval, non_neg_integer()}
          | {:jitter, non_neg_integer()}
          | {:timeout, non_neg_integer()}
          | {:partitions, non_neg_integer()}
          | {:retry_delay, non_neg_integer()}
          | {:retry_max, non_neg_integer()}
  @type options :: [option()]

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour DataBuffer

      default_opts = [
        partitions: 1,
        interval: 5_000,
        jitter: 0,
        timeout: 10_000,
        retry_delay: 1_000,
        retry_max: 5
      ]

      opt_keys = Keyword.keys(default_opts)

      @opts default_opts |> Keyword.merge(opts) |> Keyword.take(opt_keys)
      @partitions Keyword.get(@opts, :partitions) - 1

      @impl DataBuffer
      def start_link(opts \\ []) do
        opts = Keyword.merge(@opts, opts)
        DataBuffer.Supervisor.start_link(__MODULE__, opts)
      end

      @doc false
      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]}
        }
      end

      @impl DataBuffer
      def handle_flush(_key, _data) do
        :ok
      end

      @impl DataBuffer
      def handle_error(_key, _data) do
        :ok
      end

      @impl DataBuffer
      def insert(key, val) do
        DataBuffer.Worker.insert(__MODULE__, key, val, @partitions)
      end

      @impl DataBuffer
      def flush(key) do
        DataBuffer.Worker.flush(__MODULE__, key)
      end

      @impl DataBuffer
      def count(key) do
        DataBuffer.Worker.count(__MODULE__, key, @partitions)
      end

      defoverridable handle_flush: 2, handle_error: 2
    end
  end
end
