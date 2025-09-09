# DataBuffer

[![Hex.pm](https://img.shields.io/hexpm/v/data_buffer.svg)](https://hex.pm/packages/data_buffer)
[![Documentation](https://img.shields.io/badge/documentation-gray)](https://hexdocs.pm/data_buffer)

DataBuffer is a high-performance Elixir library for buffering and batch processing data. It provides automatic flushing based on size or time thresholds, making it ideal for scenarios where you need to aggregate data before processing it in bulk.

## Features

- 🚀 **High Performance** - Efficient in-memory buffering with ETS-backed storage
- ⚡ **Automatic Flushing** - Configurable size and time-based triggers
- 🔄 **Multiple Partitions** - Distribute load across multiple buffer partitions
- 📊 **Telemetry Integration** - Built-in observability and monitoring
- 🛡️ **Fault Tolerant** - Graceful shutdown with automatic flush on termination
- ⏱️ **Backpressure Handling** - Configurable timeouts and overflow protection
- 🎲 **Jitter Support** - Prevent thundering herd with configurable jitter

## Installation

Add `data_buffer` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:data_buffer, "~> 0.7.1"}
  ]
end
```

## Quick Start

### 1. Define Your Buffer Module

Create a module that implements the `DataBuffer` behaviour:

```elixir
defmodule MyApp.EventBuffer do
  use DataBuffer

  def start_link(opts) do
    DataBuffer.start_link(__MODULE__, opts)
  end

  @impl DataBuffer
  def handle_flush(data_stream, opts) do
    # Process your buffered data here
    events = Enum.to_list(data_stream)
    
    # Example: Bulk insert to database
    MyApp.Repo.insert_all("events", events)
    
    # Or send to external service
    MyApp.Analytics.track_batch(events)
    
    :ok
  end
end
```

### 2. Add to Your Supervision Tree

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      # Start your buffer with configuration
      {MyApp.EventBuffer,
       name: MyApp.EventBuffer,
       partitions: 4,
       max_size: 1000,
       flush_interval: 5000}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

### 3. Use Your Buffer

```elixir
# Insert single items
DataBuffer.insert(MyApp.EventBuffer, %{type: "click", user_id: 123})

# Insert batches for better performance
events = [
  %{type: "view", user_id: 123},
  %{type: "click", user_id: 456},
  %{type: "purchase", user_id: 789}
]
DataBuffer.insert_batch(MyApp.EventBuffer, events)

# Manually trigger flush if needed
DataBuffer.flush(MyApp.EventBuffer)

# Check buffer status
size = DataBuffer.size(MyApp.EventBuffer)
info = DataBuffer.info(MyApp.EventBuffer)
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:name` | atom | required | Process name for the buffer |
| `:partitions` | integer | 1 | Number of partition processes |
| `:max_size` | integer | 5000 | Maximum items before automatic flush |
| `:max_size_jitter` | integer | 0 | Random jitter (0 to n) added to max_size |
| `:flush_interval` | integer | 10000 | Time in ms between automatic flushes |
| `:flush_jitter` | integer | 2000 | Random jitter (0 to n) added to flush_interval |
| `:flush_timeout` | integer | 60000 | Timeout in ms for flush operations |
| `:flush_meta` | any | nil | Metadata passed to handle_flush callback |

## Advanced Usage

### Custom Flush Metadata

Pass metadata to your flush handler for context:

```elixir
{MyApp.EventBuffer,
 name: MyApp.EventBuffer,
 flush_meta: %{destination: "analytics_db", priority: :high}}

# In your handle_flush callback:
def handle_flush(data_stream, opts) do
  meta = Keyword.get(opts, :meta)
  size = Keyword.get(opts, :size)
  
  case meta.destination do
    "analytics_db" -> process_analytics(data_stream)
    "metrics_db" -> process_metrics(data_stream)
  end
end
```

### Multiple Buffers

You can run multiple buffers for different data types:

```elixir
children = [
  {MyApp.EventBuffer, name: MyApp.EventBuffer, max_size: 1000},
  {MyApp.MetricsBuffer, name: MyApp.MetricsBuffer, max_size: 5000},
  {MyApp.LogBuffer, name: MyApp.LogBuffer, flush_interval: 1000}
]
```

### Synchronous Operations

For testing or specific use cases, use synchronous operations:

```elixir
# Wait for flush to complete and get results
results = DataBuffer.sync_flush(MyApp.EventBuffer)

# Dump buffer contents without flushing
data = DataBuffer.dump(MyApp.EventBuffer)
```

### Monitoring with Telemetry

DataBuffer emits telemetry events that you can hook into:

```elixir
:telemetry.attach(
  "buffer-metrics",
  [:data_buffer, :flush, :stop],
  fn _event_name, measurements, metadata, _config ->
    Logger.info("Flushed #{metadata.size} items from #{metadata.buffer}")
  end,
  nil
)
```

Available events:
- `[:data_buffer, :insert, :start]` / `[:data_buffer, :insert, :stop]`
- `[:data_buffer, :flush, :start]` / `[:data_buffer, :flush, :stop]`

## Implementation Details

### Architecture

DataBuffer uses a multi-process architecture for reliability and performance:

1. **Supervisor Process** - Manages the buffer's child processes
2. **Partition Processes** - Handle data storage and triggering flushes
3. **Flusher Processes** - Execute flush operations in separate processes

### Partitioning Strategy

Data is distributed across partitions using round-robin selection. Each partition:
- Maintains its own ETS table for data storage
- Has independent size and time triggers
- Flushes asynchronously without blocking other partitions

### Flush Triggers

Flushes are triggered when:
1. **Size threshold** is reached (`max_size` + random jitter)
2. **Time interval** expires (`flush_interval` + random jitter)
3. **Manual flush** is called via `DataBuffer.flush/2`
4. **Process termination** occurs (graceful shutdown)

### Backpressure Handling

When a partition is full and flushing:
- New inserts wait for the flush to complete
- Configurable timeout prevents indefinite blocking
- Multiple partitions help distribute load

### Fault Tolerance

- Flushes run in separate processes to isolate failures
- Timeouts prevent stuck flush operations
- Graceful shutdown ensures data is flushed on termination
- Supervisor restarts failed components

## Use Cases

DataBuffer is ideal for:

- **Database Write Batching** - Accumulate records for bulk inserts
- **Event Aggregation** - Collect events before sending to analytics services
- **Log Processing** - Buffer log entries for batch processing
- **Metrics Collection** - Aggregate metrics before reporting
- **API Rate Limiting** - Batch API calls to respect rate limits
- **Stream Processing** - Buffer streaming data for chunk processing

## Performance Considerations

- **Partition Count**: More partitions = better concurrency but more memory overhead
- **Buffer Size**: Larger buffers = fewer flushes but more memory usage
- **Flush Timeout**: Balance between reliability and throughput
- **Jitter**: Prevents synchronized flushes across multiple buffers/nodes

## Testing

For testing, you can use smaller thresholds and synchronous operations:

```elixir
defmodule MyApp.EventBufferTest do
  use ExUnit.Case

  setup do
    {:ok, _pid} = MyApp.EventBuffer.start_link(
      name: TestBuffer,
      max_size: 10,
      flush_interval: 100
    )
    
    {:ok, buffer: TestBuffer}
  end

  test "buffers and flushes data", %{buffer: buffer} do
    DataBuffer.insert(buffer, %{id: 1})
    DataBuffer.insert(buffer, %{id: 2})
    
    results = DataBuffer.sync_flush(buffer)
    assert length(results) == 1
  end
end
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details

