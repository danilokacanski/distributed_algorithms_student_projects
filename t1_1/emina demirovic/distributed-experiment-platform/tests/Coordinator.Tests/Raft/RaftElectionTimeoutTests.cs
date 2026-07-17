using Coordinator.Raft;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Coordinator.Tests.Raft;

public sealed class RaftElectionTimeoutTests
{
    [Fact]
    public void Follower_TimesOutAfterConfiguredPeriod()
    {
        var timeProvider =
            new ManualTimeProvider(
                DateTimeOffset.UtcNow);

        var state =
            CreateNodeState(timeProvider);

        timeProvider.Advance(
            TimeSpan.FromSeconds(2));

        Assert.True(
            state.HasElectionTimedOut(
                TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void AppendEntries_ResetsElectionTimeout()
    {
        var timeProvider =
            new ManualTimeProvider(
                DateTimeOffset.UtcNow);

        var state =
            CreateNodeState(timeProvider);

        timeProvider.Advance(
            TimeSpan.FromMilliseconds(900));

        state.HandleAppendEntries(
            new AppendEntriesRequest(
                Term: 1,
                LeaderId: "coordinator-peer-1",
                PrevLogIndex: 0,
                PrevLogTerm: 0,
                LeaderCommit: 0,
                Entries: []));

        timeProvider.Advance(
            TimeSpan.FromMilliseconds(200));

        Assert.False(
            state.HasElectionTimedOut(
                TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void Leader_DoesNotTimeOut()
    {
        var timeProvider =
            new ManualTimeProvider(
                DateTimeOffset.UtcNow);

        var state =
            CreateNodeState(timeProvider);

        var election =
            state.BeginElection();

        Assert.True(
            state.TryBecomeLeader(
                election.CurrentTerm));

        timeProvider.Advance(
            TimeSpan.FromMinutes(1));

        Assert.False(
            state.HasElectionTimedOut(
                TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task BackgroundService_AutomaticallyElectsLeader()
    {
        var options =
            Options.Create(
                CreateOptions(
                    automaticElectionEnabled: true,
                    electionMinMilliseconds: 20,
                    electionMaxMilliseconds: 30));

        var state =
            new RaftNodeState(
                options,
                new TestPersistentStateStore());

        var peerClient =
            new GrantingPeerClient();

        var heartbeatSender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        var electionService =
            new RaftElectionService(
                state,
                peerClient,
                heartbeatSender);

        using var backgroundService =
            new RaftElectionBackgroundService(
                state,
                electionService,
                options,
                NullLogger<
                    RaftElectionBackgroundService>.Instance);

        await backgroundService.StartAsync(
            CancellationToken.None);

        try
        {
            var deadline =
                DateTime.UtcNow.AddSeconds(2);

            while (
                state.GetSnapshot().Role !=
                    RaftNodeRole.Leader &&
                DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }

            Assert.Equal(
                RaftNodeRole.Leader,
                state.GetSnapshot().Role);
        }
        finally
        {
            await backgroundService.StopAsync(
                CancellationToken.None);
        }
    }

    private static RaftNodeState CreateNodeState(
        TimeProvider timeProvider)
    {
        return new RaftNodeState(
            Options.Create(CreateOptions()),
            new TestPersistentStateStore(),
            timeProvider);
    }

    private static RaftOptions CreateOptions(
        bool automaticElectionEnabled = false,
        int electionMinMilliseconds = 1500,
        int electionMaxMilliseconds = 3000)
    {
        return new RaftOptions
        {
            NodeId = "coordinator-test",
            AutomaticElectionEnabled =
                automaticElectionEnabled,
            HeartbeatIntervalMilliseconds = 500,
            ElectionTimeoutMinMilliseconds =
                electionMinMilliseconds,
            ElectionTimeoutMaxMilliseconds =
                electionMaxMilliseconds,
            Peers =
            [
                new RaftPeerOptions
                {
                    NodeId = "coordinator-peer-1",
                    BaseUrl = "http://localhost:6002"
                },
                new RaftPeerOptions
                {
                    NodeId = "coordinator-peer-2",
                    BaseUrl = "http://localhost:6003"
                }
            ]
        };
    }

    private sealed class ManualTimeProvider(
        DateTimeOffset initialTime)
        : TimeProvider
    {
        private DateTimeOffset _utcNow =
            initialTime;

        public override DateTimeOffset GetUtcNow()
        {
            return _utcNow;
        }

        public void Advance(TimeSpan duration)
        {
            _utcNow =
                _utcNow.Add(duration);
        }
    }

    private sealed class GrantingPeerClient
        : IRaftPeerClient
    {
        public Task<RequestVoteResponse>
            RequestVoteAsync(
                RaftPeerSnapshot peer,
                RequestVoteRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new RequestVoteResponse(
                    request.Term,
                    VoteGranted: true));
        }

        public Task<AppendEntriesResponse>
            AppendEntriesAsync(
                RaftPeerSnapshot peer,
                AppendEntriesRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new AppendEntriesResponse(
                    request.Term,
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