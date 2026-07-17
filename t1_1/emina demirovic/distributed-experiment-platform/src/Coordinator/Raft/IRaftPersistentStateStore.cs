namespace Coordinator.Raft;

public interface IRaftPersistentStateStore
{
    RaftPersistentState LoadOrCreate(
        string nodeId);

    void Save(
        string nodeId,
        long currentTerm,
        string? votedFor);
}