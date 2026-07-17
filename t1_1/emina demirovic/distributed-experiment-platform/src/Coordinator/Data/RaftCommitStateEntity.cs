namespace Coordinator.Data;

public sealed class RaftCommitStateEntity
{
    public string NodeId { get; set; } = string.Empty;

    public long CommitIndex { get; set; }

    public long LastApplied { get; set; }
}