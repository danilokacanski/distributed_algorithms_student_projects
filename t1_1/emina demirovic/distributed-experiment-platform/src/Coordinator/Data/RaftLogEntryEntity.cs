namespace Coordinator.Data;

public sealed class RaftLogEntryEntity
{
    public string NodeId { get; set; } = string.Empty;

    public long LogIndex { get; set; }

    public long Term { get; set; }

    public Guid CommandId { get; set; }

    public string CommandType { get; set; } = string.Empty;

    public string CommandPayloadJson { get; set; } = string.Empty;
}