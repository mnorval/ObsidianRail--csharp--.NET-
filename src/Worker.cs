namespace ObsidianRail;

public sealed class Fabricator : BackgroundService
{
    private readonly Rail _rail;
    private readonly MetricSink _metrics;
    private static readonly string[] Streams = ["ledger", "edge", "auth", "mesh"];
    private static readonly string[] Bodies =
    [
        "settlement committed",
        "peer handshake",
        "token rotated",
        "quota shadow-breach",
        "replica lag 12ms"
    ];

    public Fabricator(Rail rail, MetricSink metrics)
    {
        _rail = rail;
        _metrics = metrics;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var rng = Random.Shared;
        var n = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            n++;
            var level = (n % 17) switch
            {
                0 => Severity.Fault,
                1 or 2 => Severity.Warn,
                _ => Severity.Info
            };
            var ev = new RailEvent(
                DateTimeOffset.UtcNow,
                Streams[rng.Next(Streams.Length)],
                level,
                Bodies[rng.Next(Bodies.Length)],
                new Dictionary<string, string>
                {
                    ["seq"] = n.ToString(),
                    ["site"] = n % 2 == 0 ? "cpt" : "jnb"
                });
            await _rail.PublishAsync(ev, stoppingToken);
            if (n % 20 == 0)
                Console.WriteLine($"-- metrics  emitted={_metrics.Count} faults={_metrics.Faults}");
            await Task.Delay(280, stoppingToken);
        }
    }
}
