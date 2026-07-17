namespace Coordinator.Commands;

public sealed record RequestExperimentCancellationCommand(
    Guid CommandId,
    DateTimeOffset OccurredAtUtc,
    Guid ExperimentId,
    Guid EventId)
    : CoordinatorCommand(
        CommandId,
        OccurredAtUtc);