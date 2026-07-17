namespace Coordinator.Raft;

public interface IRaftCommitStateStore
{
    RaftCommitState LoadOrCreate();

    void Save(
        long commitIndex,
        long lastApplied);
}