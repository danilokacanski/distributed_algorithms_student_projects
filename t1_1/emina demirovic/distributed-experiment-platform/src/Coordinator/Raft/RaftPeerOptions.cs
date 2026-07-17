namespace Coordinator.Raft;

public sealed class RaftPeerOptions
{
    public string NodeId { get; set; } = string.Empty;

    public string BaseUrl { get; set; } = string.Empty;
}