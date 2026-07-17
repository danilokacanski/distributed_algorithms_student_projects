using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Tests.Raft;

public sealed class RaftHeartbeatTests
{
    [Fact]
    public async Task Follower_DoesNotSendHeartbeat()
    {
        var state = CreateNodeState();

        var peerClient =
            new FakeRaftPeerClient(
                (_, request) =>
                    new AppendEntriesResponse(
                        request.Term,
                        Success: true));

        var sender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        await sender.SendHeartbeatAsync();

        Assert.Equal(
            0,
            peerClient.RequestCount);
    }

    [Fact]
    public async Task Leader_SendsHeartbeatToAllPeers()
    {
        var state = CreateLeaderState();

        var peerClient =
            new FakeRaftPeerClient(
                (_, request) =>
                    new AppendEntriesResponse(
                        request.Term,
                        Success: true));

        var sender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        await sender.SendHeartbeatAsync();

        Assert.Equal(
            2,
            peerClient.RequestCount);
    }

    [Fact]
    public async Task HigherTermResponse_MakesLeaderFollower()
    {
        var state = CreateLeaderState();

        var peerClient =
            new FakeRaftPeerClient(
                (peer, request) =>
                    peer.NodeId ==
                    "coordinator-peer-1"
                        ? new AppendEntriesResponse(
                            Term: request.Term + 1,
                            Success: false)
                        : new AppendEntriesResponse(
                            Term: request.Term,
                            Success: true));

        var sender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        await sender.SendHeartbeatAsync();

        var snapshot =
            state.GetSnapshot();

        Assert.Equal(
            RaftNodeRole.Follower,
            snapshot.Role);

        Assert.Equal(
            2,
            snapshot.CurrentTerm);

        Assert.Null(snapshot.VotedFor);
        Assert.Null(snapshot.LeaderId);
    }

    private static RaftNodeState
        CreateLeaderState()
    {
        var state = CreateNodeState();

        var election =
            state.BeginElection();

        Assert.True(
            state.TryBecomeLeader(
                election.CurrentTerm));

        return state;
    }

    private static RaftNodeState
        CreateNodeState()
    {
        return new RaftNodeState(
            Options.Create(
                new RaftOptions
                {
                    NodeId =
                        "coordinator-test",
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
            AppendEntriesRequest,
            AppendEntriesResponse> handler)
        : IRaftPeerClient
    {
        private int _requestCount;

        public int RequestCount =>
            Volatile.Read(
                ref _requestCount);

        public Task<RequestVoteResponse>
            RequestVoteAsync(
                RaftPeerSnapshot peer,
                RequestVoteRequest request,
                CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public Task<AppendEntriesResponse>
            AppendEntriesAsync(
                RaftPeerSnapshot peer,
                AppendEntriesRequest request,
                CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            Interlocked.Increment(
                ref _requestCount);

            return Task.FromResult(
                handler(peer, request));
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