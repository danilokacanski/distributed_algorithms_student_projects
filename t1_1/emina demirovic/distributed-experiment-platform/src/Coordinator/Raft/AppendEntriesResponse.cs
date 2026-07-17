namespace Coordinator.Raft;

public sealed record AppendEntriesResponse(
    long Term,
    bool Success);