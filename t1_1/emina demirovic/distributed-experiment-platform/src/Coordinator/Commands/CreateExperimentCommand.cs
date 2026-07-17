namespace Coordinator.Commands;

public sealed record CreateExperimentCommand(
    Guid CommandId,
    DateTimeOffset OccurredAtUtc,
    Guid ExperimentId,
    Guid EventId,
    string Name,
    string Algorithm,
    string Environment,
    int Seed,
    int MaxSteps,
    int Priority,
    int TimeoutSeconds,
    bool SimulateFailure)
    : CoordinatorCommand(
        CommandId,
        OccurredAtUtc);