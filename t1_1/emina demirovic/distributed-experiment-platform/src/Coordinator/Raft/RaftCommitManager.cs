namespace Coordinator.Raft;

public sealed class RaftCommitManager
{
    private readonly object _sync = new();

    private readonly RaftLogManager _logManager;

    private readonly IRaftCommitStateStore
        _stateStore;

    private long _commitIndex;

    private long _lastApplied;

    public RaftCommitManager(
        RaftLogManager logManager,
        IRaftCommitStateStore stateStore)
    {
        _logManager = logManager;
        _stateStore = stateStore;

        var state =
            _stateStore.LoadOrCreate();

        _commitIndex = state.CommitIndex;
        _lastApplied = state.LastApplied;
    }

    public RaftCommitState GetState()
    {
        lock (_sync)
        {
            return new RaftCommitState(
                _commitIndex,
                _lastApplied);
        }
    }

    public long TryAdvanceLeaderCommit(
        long currentTerm,
        int clusterSize,
        IReadOnlyCollection<long>
            followerMatchIndexes)
    {
        if (currentTerm <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(currentTerm));
        }

        if (clusterSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(clusterSize));
        }

        ArgumentNullException.ThrowIfNull(
            followerMatchIndexes);

        if (followerMatchIndexes.Count !=
            clusterSize - 1)
        {
            throw new ArgumentException(
                "A match index is required for every follower.",
                nameof(followerMatchIndexes));
        }

        lock (_sync)
        {
            var lastLogIndex =
                _logManager
                    .GetLastPosition()
                    .LogIndex;

            var quorumSize =
                clusterSize / 2 + 1;

            for (var candidateIndex =
                    lastLogIndex;
                 candidateIndex >
                    _commitIndex;
                 candidateIndex--)
            {
                var entry =
                    _logManager.GetEntry(
                        candidateIndex)
                    ?? throw new InvalidOperationException(
                        $"Raft log entry " +
                        $"{candidateIndex} does not exist.");

                // Raft lider neposredno commit-uje
                // samo zapise iz svog trenutnog termina.
                if (entry.Term != currentTerm)
                {
                    continue;
                }

                var replicationCount =
                    1 + followerMatchIndexes.Count(
                        matchIndex =>
                            matchIndex >=
                            candidateIndex);

                if (replicationCount <
                    quorumSize)
                {
                    continue;
                }

                _stateStore.Save(
                    candidateIndex,
                    _lastApplied);

                _commitIndex =
                    candidateIndex;

                break;
            }

            return _commitIndex;
        }
    }

    public long AdvanceFollowerCommit(
        long leaderCommit)
    {
        if (leaderCommit < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(leaderCommit));
        }

        lock (_sync)
        {
            var localLastLogIndex =
                _logManager
                    .GetLastPosition()
                    .LogIndex;

            var targetCommitIndex =
                Math.Min(
                    leaderCommit,
                    localLastLogIndex);

            if (targetCommitIndex <=
                _commitIndex)
            {
                return _commitIndex;
            }

            _stateStore.Save(
                targetCommitIndex,
                _lastApplied);

            _commitIndex =
                targetCommitIndex;

            return _commitIndex;
        }
    }

    public void MarkApplied(long logIndex)
    {
        lock (_sync)
        {
            var expectedIndex =
                checked(_lastApplied + 1);

            if (logIndex != expectedIndex)
            {
                throw new InvalidOperationException(
                    $"Expected applied log index " +
                    $"{expectedIndex}, but received " +
                    $"{logIndex}.");
            }

            if (logIndex > _commitIndex)
            {
                throw new InvalidOperationException(
                    $"Raft log index {logIndex} " +
                    "is not committed.");
            }

            _stateStore.Save(
                _commitIndex,
                logIndex);

            _lastApplied = logIndex;
        }
    }
}