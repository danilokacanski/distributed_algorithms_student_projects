namespace Coordinator.Raft;

public sealed record RequestVoteRequest(
    long Term,
    string CandidateId,
    long LastLogIndex,
    long LastLogTerm);