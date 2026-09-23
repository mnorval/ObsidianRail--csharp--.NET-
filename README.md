# ObsidianRail

State-of-the-art **.NET 8 worker**: a bounded `Channel<T>` event fabric with filter chain, fan-out sinks, and a hosted producer.

Patterns that matter in production:
- `BoundedChannelFullMode.DropOldest` so producers never deadlock
- `record struct` events (zero-alloc-ish payload)
- filter short-circuit before sink fan-out
- `IAsyncDisposable` shutdown that completes the writer
- `BackgroundService` as the only ticking clock

## Run
```bash
dotnet run
```

Requires the .NET 8 SDK. Swap `ConsoleSink` for Seq / OpenTelemetry without touching producers.
