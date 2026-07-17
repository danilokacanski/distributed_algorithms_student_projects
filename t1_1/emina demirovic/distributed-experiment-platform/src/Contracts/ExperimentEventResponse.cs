namespace Contracts;

public sealed class ExperimentEventResponse
{
    public Guid Id { get; set; }

    public string Type { get; set; } = string.Empty;

    public DateTimeOffset OccurredAtUtc { get; set; }

    public string? WorkerId { get; set; }

    public int Attempt { get; set; }

    public string? Details { get; set; }

    public bool CancellationRequested { get; set; }
}