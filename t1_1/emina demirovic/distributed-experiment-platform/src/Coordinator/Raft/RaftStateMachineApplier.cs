using Coordinator.Commands;

namespace Coordinator.Raft;

public sealed class RaftStateMachineApplier(
    RaftLogManager logManager,
    RaftCommitManager commitManager,
    CoordinatorCommandProcessor commandProcessor)
    : IRaftStateMachineApplier
{
    private readonly SemaphoreSlim _applyLock =
        new(1, 1);

    public async Task<RaftApplyResult>
        ApplyCommittedEntriesAsync(
            CancellationToken cancellationToken = default)
    {
        await _applyLock.WaitAsync(
            cancellationToken);

        try
        {
            var appliedCount = 0;

            while (true)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();

                var state =
                    commitManager.GetState();

                if (state.LastApplied >=
                    state.CommitIndex)
                {
                    return new RaftApplyResult(
                        appliedCount,
                        state.LastApplied);
                }

                var nextIndex =
                    checked(state.LastApplied + 1);

                var entry =
                    logManager.GetEntry(nextIndex)
                    ?? throw new InvalidOperationException(
                        $"Committed Raft log entry " +
                        $"{nextIndex} does not exist.");

                var command =
                    logManager.DeserializeCommand(
                        entry);

                commandProcessor.Apply(command);

                commitManager.MarkApplied(
                    nextIndex);

                appliedCount++;
            }
        }
        finally
        {
            _applyLock.Release();
        }
    }
}