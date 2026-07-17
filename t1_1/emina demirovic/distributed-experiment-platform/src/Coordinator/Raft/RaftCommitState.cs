namespace Coordinator.Raft;

public sealed record RaftCommitState(
    long CommitIndex,
    long LastApplied);