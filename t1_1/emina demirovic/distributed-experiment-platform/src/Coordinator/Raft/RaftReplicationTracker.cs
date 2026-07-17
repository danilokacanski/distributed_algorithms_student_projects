namespace Coordinator.Raft;

public sealed class RaftReplicationTracker
{
    private readonly object _sync = new();

    private readonly Dictionary<
        string,
        PeerReplicationState> _states =
            new(StringComparer.Ordinal);

    private long _term = -1;

    public void EnsureInitialized(
        long term,
        IReadOnlyList<RaftPeerSnapshot> peers,
        long leaderLastLogIndex)
    {
        if (term <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(term));
        }

        ArgumentNullException.ThrowIfNull(peers);

        if (leaderLastLogIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(leaderLastLogIndex));
        }

        lock (_sync)
        {
            var peerIds = peers
                .Select(peer => peer.NodeId)
                .OrderBy(peerId => peerId)
                .ToArray();

            var existingPeerIds = _states.Keys
                .OrderBy(peerId => peerId)
                .ToArray();

            if (_term == term &&
                peerIds.SequenceEqual(
                    existingPeerIds,
                    StringComparer.Ordinal))
            {
                return;
            }

            _states.Clear();

            foreach (var peer in peers)
            {
                _states.Add(
                    peer.NodeId,
                    new PeerReplicationState
                    {
                        NextIndex =
                            checked(leaderLastLogIndex + 1),
                        MatchIndex = 0
                    });
            }

            _term = term;
        }
    }

    public RaftPeerReplicationSnapshot Get(
        string peerId)
    {
        lock (_sync)
        {
            var state = GetState(peerId);

            return new RaftPeerReplicationSnapshot(
                peerId,
                state.NextIndex,
                state.MatchIndex);
        }
    }

    public IReadOnlyList<RaftPeerReplicationSnapshot> GetAll()
    {
        lock (_sync)
        {
            return _states
                .Select(pair =>
                    new RaftPeerReplicationSnapshot(
                        pair.Key,
                        pair.Value.NextIndex,
                        pair.Value.MatchIndex))
                .OrderBy(state =>
                    state.PeerId)
                .ToArray();
        }
    }

    public void RecordSuccess(
        string peerId,
        long replicatedThroughIndex,
        long term)
    {
        if (replicatedThroughIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(replicatedThroughIndex));
        }

        lock (_sync)
        {
            if (term != _term)
            {
                return;
            }

            var state = GetState(peerId);

            if (replicatedThroughIndex >
                state.MatchIndex)
            {
                state.MatchIndex =
                    replicatedThroughIndex;
            }

            state.NextIndex =
                checked(state.MatchIndex + 1);
        }
    }

    public void RecordFailure(
        string peerId,
        long term)
    {
        lock (_sync)
        {
            if (term != _term)
            {
                return;
            }

            var state = GetState(peerId);

            if (state.NextIndex > 1)
            {
                state.NextIndex--;
            }
        }
    }

    private PeerReplicationState GetState(
        string peerId)
    {
        if (!_states.TryGetValue(
                peerId,
                out var state))
        {
            throw new InvalidOperationException(
                $"Replication state for peer " +
                $"'{peerId}' is not initialized.");
        }

        return state;
    }

    private sealed class PeerReplicationState
    {
        public long NextIndex { get; set; }

        public long MatchIndex { get; set; }
    }
}