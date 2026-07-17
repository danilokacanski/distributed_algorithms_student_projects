namespace Coordinator.Raft;

public sealed record RaftPeerSnapshot(
    string NodeId,
    string BaseUrl);

public sealed record RaftNodeSnapshot(
    string NodeId,
    RaftNodeRole Role,
    long CurrentTerm,
    string? VotedFor,
    string? LeaderId,
    IReadOnlyList<RaftPeerSnapshot> Peers);