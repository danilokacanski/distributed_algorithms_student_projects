using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Tests.Raft;

public sealed class RaftElectionTests
{
    [Fact]
    public async Task MajorityVote_MakesNodeLeader()
    {
        var state = CreateNodeState();

        var peerClient = new FakeRaftPeerClient(
            (peer, request) =>
                peer.NodeId == "coordinator-peer-1"
                    ? new RequestVoteResponse(
                        request.Term,
                        VoteGranted: true)
                    : new RequestVoteResponse(
                        request.Term,
                        VoteGranted: false));

        var heartbeatSender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        var service =
            new RaftElectionService(
                state,
                peerClient,
                heartbeatSender);

        var result =
            await service.StartElectionAsync();

        Assert.True(result.Won);
        Assert.Equal(2, result.VotesGranted);
        Assert.Equal(2, result.QuorumSize);

        Assert.Equal(
            RaftNodeRole.Leader,
            result.Role);

        var snapshot = state.GetSnapshot();

        Assert.Equal(
            "coordinator-test",
            snapshot.LeaderId);
    }

    [Fact]
    public async Task NoMajority_NodeRemainsCandidate()
    {
        var state = CreateNodeState();

        var peerClient = new FakeRaftPeerClient(
            (_, request) =>
                new RequestVoteResponse(
                    request.Term,
                    VoteGranted: false));

        var heartbeatSender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        var service =
            new RaftElectionService(
                state,
                peerClient,
                heartbeatSender);

        var result =
            await service.StartElectionAsync();

        Assert.False(result.Won);
        Assert.Equal(1, result.VotesGranted);

        Assert.Equal(
            RaftNodeRole.Candidate,
            result.Role);
    }

    [Fact]
    public async Task HigherTermResponse_MakesNodeFollower()
    {
        var state = CreateNodeState();

        var peerClient = new FakeRaftPeerClient(
            (peer, request) =>
                peer.NodeId == "coordinator-peer-1"
                    ? new RequestVoteResponse(
                        Term: request.Term + 1,
                        VoteGranted: false)
                    : new RequestVoteResponse(
                        Term: request.Term,
                        VoteGranted: true));

        var heartbeatSender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        var service =
            new RaftElectionService(
                state,
                peerClient,
                heartbeatSender);

        var result =
            await service.StartElectionAsync();

        Assert.False(result.Won);

        Assert.Equal(
            RaftNodeRole.Follower,
            result.Role);

        Assert.Equal(2, result.CurrentTerm);

        var snapshot = state.GetSnapshot();

        Assert.Null(snapshot.VotedFor);
        Assert.Null(snapshot.LeaderId);
    }

    [Fact]
    public async Task OneUnavailablePeer_QuorumCanStillWin()
    {
        var state = CreateNodeState();

        var peerClient = new FakeRaftPeerClient(
            (peer, request) =>
            {
                if (peer.NodeId ==
                    "coordinator-peer-2")
                {
                    throw new HttpRequestException(
                        "Peer unavailable.");
                }

                return new RequestVoteResponse(
                    request.Term,
                    VoteGranted: true);
            });

        var heartbeatSender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        var service =
            new RaftElectionService(
                state,
                peerClient,
                heartbeatSender);

        var result =
            await service.StartElectionAsync();

        Assert.True(result.Won);
        Assert.Equal(2, result.VotesGranted);

        Assert.Equal(
            RaftNodeRole.Leader,
            result.Role);
    }

    [Fact]
    public async Task WonElection_SendsHeartbeatToAllPeers()
    {
        var state = CreateNodeState();

        var peerClient =
            new FakeRaftPeerClient(
                (_, request) =>
                    new RequestVoteResponse(
                        request.Term,
                        VoteGranted: true));

        var heartbeatSender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        var service =
            new RaftElectionService(
                state,
                peerClient,
                heartbeatSender);

        var result =
            await service.StartElectionAsync();

        Assert.True(result.Won);

        Assert.Equal(
            2,
            peerClient.AppendEntriesRequestCount);
    }

    private static RaftNodeState CreateNodeState()
    {
        return new RaftNodeState(
            Options.Create(
                new RaftOptions
                {
                    NodeId = "coordinator-test",
                    Peers =
                    [
                        new RaftPeerOptions
                        {
                            NodeId =
                                "coordinator-peer-1",
                            BaseUrl =
                                "http://localhost:6002"
                        },
                        new RaftPeerOptions
                        {
                            NodeId =
                                "coordinator-peer-2",
                            BaseUrl =
                                "http://localhost:6003"
                        }
                    ]
                }),
            new TestPersistentStateStore());
    }

    private sealed class FakeRaftPeerClient(
        Func<
            RaftPeerSnapshot,
            RequestVoteRequest,
            RequestVoteResponse> handler)
        : IRaftPeerClient
    {
        public Task<RequestVoteResponse>
            RequestVoteAsync(
                RaftPeerSnapshot peer,
                RequestVoteRequest request,
                CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            return Task.FromResult(
                handler(peer, request));
        }

        private int _appendEntriesRequestCount;

        public int AppendEntriesRequestCount =>
            Volatile.Read(
                ref _appendEntriesRequestCount);

        public Task<AppendEntriesResponse> AppendEntriesAsync(
            RaftPeerSnapshot peer,
            AppendEntriesRequest request,
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            Interlocked.Increment(
                ref _appendEntriesRequestCount);

            return Task.FromResult(
                new AppendEntriesResponse(
                    Term: request.Term,
                    Success: true));
        }
    }

    private sealed class TestPersistentStateStore
        : IRaftPersistentStateStore
    {
        private readonly Dictionary<
            string,
            RaftPersistentState> _states = [];

        public RaftPersistentState LoadOrCreate(
            string nodeId)
        {
            if (_states.TryGetValue(
                nodeId,
                out var state))
            {
                return state;
            }

            state = new RaftPersistentState(
                CurrentTerm: 0,
                VotedFor: null);

            _states[nodeId] = state;

            return state;
        }

        public void Save(
            string nodeId,
            long currentTerm,
            string? votedFor)
        {
            _states[nodeId] =
                new RaftPersistentState(
                    currentTerm,
                    votedFor);
        }
    }
}