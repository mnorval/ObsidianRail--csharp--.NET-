namespace ObsidianRail;

public enum Severity { Trace, Info, Warn, Fault }

public readonly record struct RailEvent(
    DateTimeOffset Ts,
    string Stream,
    Severity Level,
    string Body,
    IReadOnlyDictionary<string, string> Tags);

public interface ISink
{
    ValueTask EmitAsync(RailEvent ev, CancellationToken ct);
}

public interface IFilter
{
    bool Accept(in RailEvent ev);
}
