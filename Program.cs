using ObsidianRail;

var metrics = new MetricSink();
var rail = new Rail(capacity: 128)
    .Use(new MinSeverityFilter(Severity.Info))
    .To(new ConsoleSink())
    .To(metrics);

rail.Start();

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton(rail);
builder.Services.AddSingleton(metrics);
builder.Services.AddHostedService<Fabricator>();

var host = builder.Build();
Console.WriteLine("ObsidianRail — bounded channel fabric online\n");
await host.RunAsync();
