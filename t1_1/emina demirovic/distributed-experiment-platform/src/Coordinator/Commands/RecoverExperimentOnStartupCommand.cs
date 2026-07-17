namespace Coordinator.Commands;

public sealed record RecoverExperimentOnStartupCommand(
    Guid CommandId,
    DateTimeOffset OccurredAtUtc,
    Guid ExperimentId,
    Guid EventId,
    string? PreviousWorkerId,
    int Attempt)
    : CoordinatorCommand(
        CommandId,
        OccurredAtUtc);