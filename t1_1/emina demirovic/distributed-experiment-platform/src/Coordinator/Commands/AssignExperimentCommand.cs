namespace Coordinator.Commands;

public sealed record AssignExperimentCommand(
    Guid CommandId,
    DateTimeOffset OccurredAtUtc,
    Guid ExperimentId,
    Guid EventId,
    string WorkerId,
    int Attempt)
    : CoordinatorCommand(
        CommandId,
        OccurredAtUtc);