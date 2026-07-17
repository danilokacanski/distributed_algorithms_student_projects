namespace Coordinator.Raft;

public sealed record AppendEntriesRequest(
    long Term,
    string LeaderId,
    long PrevLogIndex,
    long PrevLogTerm,
    long LeaderCommit,
    IReadOnlyList<RaftLogEntry> Entries);