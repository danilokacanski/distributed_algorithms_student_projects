namespace Coordinator.Raft;

public sealed record RaftPeerReplicationSnapshot(
    string PeerId,
    long NextIndex,
    long MatchIndex);