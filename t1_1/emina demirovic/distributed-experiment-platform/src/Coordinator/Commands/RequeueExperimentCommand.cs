namespace Coordinator.Commands;

public sealed record RequeueExperimentCommand(
    Guid CommandId,
    DateTimeOffset OccurredAtUtc,
    Guid ExperimentId,
    Guid EventId,
    string WorkerId,
    int Attempt,
    bool CancelInsteadOfRequeue)
    : CoordinatorCommand(
        CommandId,
        OccurredAtUtc);