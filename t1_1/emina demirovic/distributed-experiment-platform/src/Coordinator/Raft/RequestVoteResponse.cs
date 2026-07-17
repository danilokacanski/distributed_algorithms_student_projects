namespace Coordinator.Raft;

public sealed record RequestVoteResponse(
    long Term,
    bool VoteGranted);