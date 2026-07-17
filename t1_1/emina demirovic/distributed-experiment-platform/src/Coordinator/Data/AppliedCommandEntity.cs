namespace Coordinator.Data;

public sealed class AppliedCommandEntity
{
    public Guid CommandId { get; set; }

    public string CommandType { get; set; } =
        string.Empty;

    public Guid ExperimentId { get; set; }

    public DateTimeOffset OccurredAtUtc { get; set; }
}