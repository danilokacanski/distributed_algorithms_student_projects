namespace Coordinator.Raft;

public interface IRaftStateMachineApplier
{
    Task<RaftApplyResult> ApplyCommittedEntriesAsync(
        CancellationToken cancellationToken = default);
}