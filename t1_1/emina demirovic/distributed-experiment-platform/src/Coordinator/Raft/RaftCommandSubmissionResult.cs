namespace Coordinator.Raft;

public sealed record RaftCommandSubmissionResult(
    RaftCommandSubmissionStatus Status,
    long? LogIndex,
    string? LeaderId,
    int AppliedCount);