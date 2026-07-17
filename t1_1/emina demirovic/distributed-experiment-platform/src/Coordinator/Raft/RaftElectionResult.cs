namespace Coordinator.Raft;

public sealed record RaftElectionResult(
    long CurrentTerm,
    RaftNodeRole Role,
    int VotesGranted,
    int QuorumSize,
    bool Won);