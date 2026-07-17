namespace Coordinator.Commands;

public abstract record CoordinatorCommand(
    Guid CommandId,
    DateTimeOffset OccurredAtUtc);