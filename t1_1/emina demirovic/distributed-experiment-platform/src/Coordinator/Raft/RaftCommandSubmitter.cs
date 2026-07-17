using Coordinator.Commands;
using Microsoft.Extensions.Options;

namespace Coordinator.Raft;

public sealed class RaftCommandSubmitter(
    RaftNodeState nodeState,
    RaftLogManager logManager,
    RaftHeartbeatSender heartbeatSender,
    RaftCommitManager commitManager,
    IRaftStateMachineApplier applier,
    IOptions<RaftOptions> options)
{
    private readonly SemaphoreSlim _submitLock =
        new(1, 1);

    private readonly TimeSpan _timeout =
        TimeSpan.FromMilliseconds(
            options.Value
                .CommandReplicationTimeoutMilliseconds);

    private readonly TimeSpan _pollInterval =
        TimeSpan.FromMilliseconds(
            options.Value
                .CommandReplicationPollMilliseconds);

    public async Task<RaftCommandSubmissionResult>
        SubmitAsync(
            CoordinatorCommand command,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);

        await _submitLock.WaitAsync(
            cancellationToken);

        try
        {
            var leaderSnapshot =
                nodeState.GetSnapshot();

            if (leaderSnapshot.Role !=
                RaftNodeRole.Leader)
            {
                return new RaftCommandSubmissionResult(
                    RaftCommandSubmissionStatus.NotLeader,
                    LogIndex: null,
                    LeaderId: leaderSnapshot.LeaderId,
                    AppliedCount: 0);
            }

            var entry =
                logManager.AppendCommand(
                    command,
                    leaderSnapshot.CurrentTerm);

            var deadline =
                DateTimeOffset.UtcNow + _timeout;

            while (DateTimeOffset.UtcNow < deadline)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();

                await heartbeatSender
                    .SendHeartbeatAsync(
                        cancellationToken);

                var currentSnapshot =
                    nodeState.GetSnapshot();

                if (currentSnapshot.Role !=
                        RaftNodeRole.Leader ||
                    currentSnapshot.CurrentTerm !=
                        leaderSnapshot.CurrentTerm)
                {
                    return new RaftCommandSubmissionResult(
                        RaftCommandSubmissionStatus.NotLeader,
                        entry.LogIndex,
                        currentSnapshot.LeaderId,
                        AppliedCount: 0);
                }

                var commitState =
                    commitManager.GetState();

                if (commitState.CommitIndex >=
                    entry.LogIndex)
                {
                    var applyResult =
                        await applier
                            .ApplyCommittedEntriesAsync(
                                cancellationToken);

                    return new RaftCommandSubmissionResult(
                        RaftCommandSubmissionStatus.Committed,
                        entry.LogIndex,
                        currentSnapshot.LeaderId,
                        applyResult.AppliedCount);
                }

                var remaining =
                    deadline - DateTimeOffset.UtcNow;

                if (remaining <= TimeSpan.Zero)
                {
                    break;
                }

                var delay =
                    remaining < _pollInterval
                        ? remaining
                        : _pollInterval;

                await Task.Delay(
                    delay,
                    cancellationToken);
            }

            return new RaftCommandSubmissionResult(
                RaftCommandSubmissionStatus.TimedOut,
                entry.LogIndex,
                LeaderId:
                    nodeState.GetSnapshot().LeaderId,
                AppliedCount: 0);
        }
        finally
        {
            _submitLock.Release();
        }
    }
}