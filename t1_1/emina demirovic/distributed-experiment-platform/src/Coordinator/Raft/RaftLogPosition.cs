namespace Coordinator.Raft;

public sealed record RaftLogPosition(
    long LogIndex,
    long Term);