namespace Coordinator.Commands;

public sealed record CompleteExperimentCommand(
    Guid CommandId,
    DateTimeOffset OccurredAtUtc,
    Guid ExperimentId,
    Guid EventId,
    string WorkerId,
    int Attempt,
    bool Succeeded,
    bool WasCancelled,
    string? ResultMessage,
    string? MetricsJson,
    long? ExecutionDurationMs)
    : CoordinatorCommand(
        CommandId,
        OccurredAtUtc);