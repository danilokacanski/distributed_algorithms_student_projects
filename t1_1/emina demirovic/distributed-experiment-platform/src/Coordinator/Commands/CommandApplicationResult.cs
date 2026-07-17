namespace Coordinator.Commands;

public sealed record CommandApplicationResult<T>(
    T Value,
    bool WasAlreadyApplied);