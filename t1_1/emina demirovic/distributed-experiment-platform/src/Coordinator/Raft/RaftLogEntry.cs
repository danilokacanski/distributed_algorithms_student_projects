namespace Coordinator.Raft;

public sealed record RaftLogEntry(
    long LogIndex,
    long Term,
    Guid CommandId,
    string CommandType,
    string CommandPayloadJson);