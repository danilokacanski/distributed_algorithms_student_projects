namespace Coordinator.Raft;

public sealed class RaftHeartbeatSender
{
    private readonly RaftNodeState _nodeState;

    private readonly IRaftPeerClient _peerClient;

    private readonly RaftLogManager? _logManager;

    private readonly RaftReplicationTracker? _replicationTracker;

    private readonly RaftCommitManager? _commitManager;

    private readonly SemaphoreSlim _sendLock =
        new(1, 1);

    public RaftHeartbeatSender(
        RaftNodeState nodeState,
        IRaftPeerClient peerClient)
        : this(
            nodeState,
            peerClient,
            logManager: null,
            replicationTracker: null,
            commitManager: null)
    {
    }

    public RaftHeartbeatSender(
        RaftNodeState nodeState,
        IRaftPeerClient peerClient,
        RaftLogManager? logManager,
        RaftReplicationTracker? replicationTracker)
        : this(
            nodeState,
            peerClient,
            logManager,
            replicationTracker,
            commitManager: null)
    {
    }

    public RaftHeartbeatSender(
        RaftNodeState nodeState,
        IRaftPeerClient peerClient,
        RaftLogManager? logManager,
        RaftReplicationTracker? replicationTracker,
        RaftCommitManager? commitManager)
    {
        _nodeState = nodeState;
        _peerClient = peerClient;
        _logManager = logManager;
        _replicationTracker = replicationTracker;
        _commitManager = commitManager;
    }

    public async Task SendHeartbeatAsync(
        CancellationToken cancellationToken = default)
    {
        await _sendLock.WaitAsync(
            cancellationToken);

        try
        {
            var leaderSnapshot =
                _nodeState.GetSnapshot();

            if (leaderSnapshot.Role !=
                RaftNodeRole.Leader)
            {
                return;
            }

            if (_logManager is null ||
                _replicationTracker is null)
            {
                await SendEmptyHeartbeatAsync(
                    leaderSnapshot,
                    cancellationToken);

                return;
            }

            var lastPosition =
                _logManager.GetLastPosition();

            _replicationTracker.EnsureInitialized(
                leaderSnapshot.CurrentTerm,
                leaderSnapshot.Peers,
                lastPosition.LogIndex);

            var tasks = leaderSnapshot.Peers
                .Select(peer =>
                    ReplicateToPeerSafelyAsync(
                        peer,
                        leaderSnapshot,
                        cancellationToken))
                .ToArray();

            await Task.WhenAll(tasks);

            var currentSnapshot =
                _nodeState.GetSnapshot();

            if (_commitManager is not null &&
                currentSnapshot.Role ==
                    RaftNodeRole.Leader &&
                currentSnapshot.CurrentTerm ==
                    leaderSnapshot.CurrentTerm)
            {
                var followerMatchIndexes =
                    _replicationTracker
                        .GetAll()
                        .Select(state =>
                            state.MatchIndex)
                        .ToArray();

                _commitManager.TryAdvanceLeaderCommit(
                    currentTerm:
                        leaderSnapshot.CurrentTerm,
                    clusterSize:
                        leaderSnapshot.Peers.Count + 1,
                    followerMatchIndexes:
                        followerMatchIndexes);
            }
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private async Task ReplicateToPeerSafelyAsync(
        RaftPeerSnapshot peer,
        RaftNodeSnapshot leaderSnapshot,
        CancellationToken cancellationToken)
    {
        try
        {
            var replicationState =
                _replicationTracker!.Get(
                    peer.NodeId);

            var prevLogIndex =
                replicationState.NextIndex - 1;

            var prevLogTerm =
                _logManager!.GetTermAt(
                    prevLogIndex);

            var entries =
                _logManager.GetEntriesFrom(
                    replicationState.NextIndex);

            var request =
                new AppendEntriesRequest(
                    Term:
                        leaderSnapshot.CurrentTerm,
                    LeaderId:
                        leaderSnapshot.NodeId,
                    PrevLogIndex:
                        prevLogIndex,
                    PrevLogTerm:
                        prevLogTerm,
                    LeaderCommit:
                        _commitManager?
                            .GetState()
                            .CommitIndex ?? 0,
                    Entries: entries);

            var response =
                await _peerClient.AppendEntriesAsync(
                    peer,
                    request,
                    cancellationToken);

            if (response.Term >
                leaderSnapshot.CurrentTerm)
            {
                _nodeState.ObserveHigherTerm(
                    response.Term);

                return;
            }

            var currentSnapshot =
                _nodeState.GetSnapshot();

            if (currentSnapshot.Role !=
                    RaftNodeRole.Leader ||
                currentSnapshot.CurrentTerm !=
                    leaderSnapshot.CurrentTerm)
            {
                return;
            }

            if (response.Success)
            {
                var replicatedThroughIndex =
                    checked(
                        prevLogIndex +
                        entries.Count);

                _replicationTracker.RecordSuccess(
                    peer.NodeId,
                    replicatedThroughIndex,
                    leaderSnapshot.CurrentTerm);
            }
            else
            {
                _replicationTracker.RecordFailure(
                    peer.NodeId,
                    leaderSnapshot.CurrentTerm);
            }
        }
        catch (HttpRequestException)
        {
            // Nedostupan peer ne potvrđuje replikaciju.
        }
        catch (OperationCanceledException)
            when (!cancellationToken
                .IsCancellationRequested)
        {
            // Timeout pojedinačnog peer zahteva.
        }
    }

    private async Task SendEmptyHeartbeatAsync(
        RaftNodeSnapshot leaderSnapshot,
        CancellationToken cancellationToken)
    {
        var lastLogPosition =
            _nodeState.GetLastLogPosition();

        var request =
            new AppendEntriesRequest(
                Term:
                    leaderSnapshot.CurrentTerm,
                LeaderId:
                    leaderSnapshot.NodeId,
                PrevLogIndex:
                    lastLogPosition.LogIndex,
                PrevLogTerm:
                    lastLogPosition.Term,
                LeaderCommit:
                    _commitManager?
                        .GetState()
                        .CommitIndex ?? 0,
                Entries: []);

        var tasks = leaderSnapshot.Peers
            .Select(peer =>
                SendEmptyHeartbeatSafelyAsync(
                    peer,
                    request,
                    leaderSnapshot.CurrentTerm,
                    cancellationToken))
            .ToArray();

        await Task.WhenAll(tasks);
    }

    private async Task SendEmptyHeartbeatSafelyAsync(
        RaftPeerSnapshot peer,
        AppendEntriesRequest request,
        long leaderTerm,
        CancellationToken cancellationToken)
    {
        try
        {
            var response =
                await _peerClient.AppendEntriesAsync(
                    peer,
                    request,
                    cancellationToken);

            if (response.Term > leaderTerm)
            {
                _nodeState.ObserveHigherTerm(
                    response.Term);
            }
        }
        catch (HttpRequestException)
        {
        }
        catch (OperationCanceledException)
            when (!cancellationToken
                .IsCancellationRequested)
        {
        }
    }
}