namespace Coordinator.Raft;

public sealed record RaftPersistentState(
    long CurrentTerm,
    string? VotedFor);