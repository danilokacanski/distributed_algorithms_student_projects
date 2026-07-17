namespace Coordinator.Raft;

public sealed record RaftSerializedCommand(
    string CommandType,
    string PayloadJson);