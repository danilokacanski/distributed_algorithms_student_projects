namespace Coordinator.Raft;

public sealed record RaftApplyResult(
    int AppliedCount,
    long LastApplied);