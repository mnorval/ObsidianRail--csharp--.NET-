using System.Threading.Channels;

namespace ObsidianRail;

/// <summary>
/// Bounded channel pipeline: producers never block the tick loop beyond DropOldest.
/// Filters are applied in registration order; sinks fan-out concurrently.
/// </summary>
public sealed class Rail : IAsyncDisposable
{
    private readonly Channel<RailEvent> _ch;
    private readonly List<IFilter> _filters = [];
    private readonly List<ISink> _sinks = [];
    private readonly CancellationTokenSource _cts = new();
    private Task? _pump;

    public Rail(int capacity = 256)
    {
        _ch = Channel.CreateBounded<RailEvent>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest
        });
    }

    public Rail Use(IFilter filter) { _filters.Add(filter); return this; }
    public Rail To(ISink sink) { _sinks.Add(sink); return this; }

    public void Start()
    {
        _pump = Task.Run(PumpAsync);
    }

    public ValueTask PublishAsync(RailEvent ev, CancellationToken ct = default)
        => _ch.Writer.WriteAsync(ev, ct);

    private async Task PumpAsync()
    {
        var ct = _cts.Token;
        try
        {
            await foreach (var ev in _ch.Reader.ReadAllAsync(ct))
            {
                if (_filters.Exists(f => !f.Accept(ev))) continue;
                if (_sinks.Count == 1)
                {
                    await _sinks[0].EmitAsync(ev, ct);
                    continue;
                }
                await Task.WhenAll(_sinks.Select(s => s.EmitAsync(ev, ct).AsTask()));
            }
        }
        catch (OperationCanceledException) { }
    }

    public async ValueTask DisposeAsync()
    {
        _ch.Writer.TryComplete();
        _cts.Cancel();
        if (_pump is not null) await _pump;
        _cts.Dispose();
    }
}

public sealed class MinSeverityFilter(Severity floor) : IFilter
{
    public bool Accept(in RailEvent ev) => ev.Level >= floor;
}

public sealed class ConsoleSink : ISink
{
    public ValueTask EmitAsync(RailEvent ev, CancellationToken ct)
    {
        var tag = ev.Tags.Count == 0 ? "" : " " + string.Join(',', ev.Tags.Select(kv => $"{kv.Key}={kv.Value}"));
        Console.WriteLine($"{ev.Ts:HH:mm:ss.fff}  {ev.Level,-5}  {ev.Stream,-12}  {ev.Body}{tag}");
        return ValueTask.CompletedTask;
    }
}

public sealed class MetricSink : ISink
{
    private long _count;
    private long _faults;
    public long Count => Interlocked.Read(ref _count);
    public long Faults => Interlocked.Read(ref _faults);

    public ValueTask EmitAsync(RailEvent ev, CancellationToken ct)
    {
        Interlocked.Increment(ref _count);
        if (ev.Level == Severity.Fault) Interlocked.Increment(ref _faults);
        return ValueTask.CompletedTask;
    }
}
