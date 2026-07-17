namespace Coordinator.Data;

public sealed class RaftPersistentStateEntity
{
    public string NodeId { get; set; } = string.Empty;

    public long CurrentTerm { get; set; }

    public string? VotedFor { get; set; }
}