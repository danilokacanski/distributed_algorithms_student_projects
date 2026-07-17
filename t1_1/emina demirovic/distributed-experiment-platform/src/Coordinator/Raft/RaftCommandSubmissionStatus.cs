namespace Coordinator.Raft;

public enum RaftCommandSubmissionStatus
{
    Committed,
    NotLeader,
    TimedOut
}