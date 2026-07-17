namespace Coordinator.Data;

public sealed class ExperimentEventEntity
{
    public Guid Id { get; set; }

    public Guid ExperimentId { get; set; }

    public ExperimentEventType Type { get; set; }

    public DateTimeOffset OccurredAtUtc { get; set; }

    public string? WorkerId { get; set; }

    public int Attempt { get; set; }

    public string? Details { get; set; }

    public ExperimentEntity Experiment { get; set; } = null!;
}