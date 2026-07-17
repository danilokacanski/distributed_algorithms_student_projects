namespace Coordinator.Raft;

public interface IRaftLogStore
{
    RaftLogEntry? Get(long logIndex);

    RaftLogEntry? GetLast();

    IReadOnlyList<RaftLogEntry> GetFrom(
        long startIndex);

    void Append(RaftLogEntry entry);

    int DeleteFrom(long startIndex);
}