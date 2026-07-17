using System.Net;
using System.Net.Http.Json;
using Coordinator.Raft;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Coordinator.Tests.Raft;

public sealed class RaftFoundationTests
{
    [Fact]
    public void NodeState_InitializesAsFollower()
    {
        var state = new RaftNodeState(
            Options.Create(CreateValidOptions()),
            new InMemoryRaftPersistentStateStore());

        var snapshot = state.GetSnapshot();

        Assert.Equal(
            "coordinator-test",
            snapshot.NodeId);

        Assert.Equal(
            RaftNodeRole.Follower,
            snapshot.Role);

        Assert.Equal(0, snapshot.CurrentTerm);
        Assert.Null(snapshot.VotedFor);
        Assert.Null(snapshot.LeaderId);
        Assert.Equal(2, snapshot.Peers.Count);
    }

    [Fact]
    public void OptionsValidator_SelfPeer_IsRejected()
    {
        var options = CreateValidOptions();

        options.Peers[0].NodeId =
            options.NodeId;

        var validator =
            new RaftOptionsValidator();

        var result =
            validator.Validate(null, options);

        Assert.True(result.Failed);

        Assert.Contains(
            "cannot include itself",
            result.FailureMessage ?? string.Empty);
    }

    [Fact]
    public async Task StatusEndpoint_ReturnsConfiguredNode()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var response = await client.GetAsync(
            "/api/raft/status");

        Assert.Equal(
            HttpStatusCode.OK,
            response.StatusCode);

        var snapshot =
            await response.Content
                .ReadFromJsonAsync<RaftNodeSnapshot>();

        Assert.NotNull(snapshot);

        Assert.Equal(
            "coordinator-1",
            snapshot.NodeId);

        Assert.Equal(
            RaftNodeRole.Follower,
            snapshot.Role);

        Assert.Equal(0, snapshot.CurrentTerm);
        Assert.Null(snapshot.VotedFor);
        Assert.Null(snapshot.LeaderId);
        Assert.Equal(2, snapshot.Peers.Count);
    }

    [Fact]
    public void BeginElection_IncrementsTermAndVotesForSelf()
    {
        var store =
            new InMemoryRaftPersistentStateStore();

        var state = new RaftNodeState(
            Options.Create(CreateValidOptions()),
            store);

        var snapshot =
            state.BeginElection();

        Assert.Equal(
            RaftNodeRole.Candidate,
            snapshot.Role);

        Assert.Equal(1, snapshot.CurrentTerm);

        Assert.Equal(
            "coordinator-test",
            snapshot.VotedFor);

        Assert.Null(snapshot.LeaderId);

        var persistedState =
            store.LoadOrCreate(
                "coordinator-test");

        Assert.Equal(
            1,
            persistedState.CurrentTerm);

        Assert.Equal(
            "coordinator-test",
            persistedState.VotedFor);
    }

    [Fact]
    public void PersistentState_SurvivesCoordinatorRestart()
    {
        var databaseDirectory = Path.Combine(
            Path.GetTempPath(),
            "distributed-experiment-platform-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(
            databaseDirectory);

        var databasePath = Path.Combine(
            databaseDirectory,
            "coordinator-tests.db");

        try
        {
            using (var firstFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var firstClient =
                    firstFactory.CreateClient();

                var firstState =
                    firstFactory.Services
                        .GetRequiredService<
                            RaftNodeState>();

                var electionSnapshot =
                    firstState.BeginElection();

                Assert.Equal(
                    RaftNodeRole.Candidate,
                    electionSnapshot.Role);

                Assert.Equal(
                    1,
                    electionSnapshot.CurrentTerm);
            }

            using (var secondFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var secondClient =
                    secondFactory.CreateClient();

                var restoredState =
                    secondFactory.Services
                        .GetRequiredService<
                            RaftNodeState>();

                var restoredSnapshot =
                    restoredState.GetSnapshot();

                Assert.Equal(
                    RaftNodeRole.Follower,
                    restoredSnapshot.Role);

                Assert.Equal(
                    1,
                    restoredSnapshot.CurrentTerm);

                Assert.Equal(
                    "coordinator-1",
                    restoredSnapshot.VotedFor);

                Assert.Null(
                    restoredSnapshot.LeaderId);
            }
        }
        finally
        {
            if (Directory.Exists(databaseDirectory))
            {
                Directory.Delete(
                    databaseDirectory,
                    recursive: true);
            }
        }
    }

    [Fact]
    public void RequestVote_RejectsStaleTerm()
    {
        var state = new RaftNodeState(
            Options.Create(CreateValidOptions()),
            new InMemoryRaftPersistentStateStore());

        state.BeginElection();

        var response =
            state.HandleRequestVote(
                new RequestVoteRequest(
                    Term: 0,
                    CandidateId:
                        "coordinator-peer-1",
                    LastLogIndex: 0,
                    LastLogTerm: 0));

        Assert.False(response.VoteGranted);
        Assert.Equal(1, response.Term);

        var snapshot = state.GetSnapshot();

        Assert.Equal(
            RaftNodeRole.Candidate,
            snapshot.Role);

        Assert.Equal(
            "coordinator-test",
            snapshot.VotedFor);
    }

    [Fact]
    public void RequestVote_HigherTermIsGrantedAndNodeBecomesFollower()
    {
        var state = new RaftNodeState(
            Options.Create(CreateValidOptions()),
            new InMemoryRaftPersistentStateStore());

        state.BeginElection();

        var response =
            state.HandleRequestVote(
                new RequestVoteRequest(
                    Term: 2,
                    CandidateId:
                        "coordinator-peer-1",
                    LastLogIndex: 0,
                    LastLogTerm: 0));

        Assert.True(response.VoteGranted);
        Assert.Equal(2, response.Term);

        var snapshot = state.GetSnapshot();

        Assert.Equal(
            RaftNodeRole.Follower,
            snapshot.Role);

        Assert.Equal(2, snapshot.CurrentTerm);

        Assert.Equal(
            "coordinator-peer-1",
            snapshot.VotedFor);

        Assert.Null(snapshot.LeaderId);
    }

    [Fact]
    public void RequestVote_RejectsSecondCandidateInSameTerm()
    {
        var state = new RaftNodeState(
            Options.Create(CreateValidOptions()),
            new InMemoryRaftPersistentStateStore());

        var firstResponse =
            state.HandleRequestVote(
                new RequestVoteRequest(
                    Term: 1,
                    CandidateId:
                        "coordinator-peer-1",
                    LastLogIndex: 0,
                    LastLogTerm: 0));

        var secondResponse =
            state.HandleRequestVote(
                new RequestVoteRequest(
                    Term: 1,
                    CandidateId:
                        "coordinator-peer-2",
                    LastLogIndex: 0,
                    LastLogTerm: 0));

        Assert.True(firstResponse.VoteGranted);
        Assert.False(secondResponse.VoteGranted);

        var snapshot = state.GetSnapshot();

        Assert.Equal(
            "coordinator-peer-1",
            snapshot.VotedFor);
    }

    [Fact]
    public void RequestVote_RepeatedRequestFromSameCandidateIsIdempotent()
    {
        var state = new RaftNodeState(
            Options.Create(CreateValidOptions()),
            new InMemoryRaftPersistentStateStore());

        var request =
            new RequestVoteRequest(
                Term: 1,
                CandidateId:
                    "coordinator-peer-1",
                LastLogIndex: 0,
                LastLogTerm: 0);

        var firstResponse =
            state.HandleRequestVote(request);

        var repeatedResponse =
            state.HandleRequestVote(request);

        Assert.True(firstResponse.VoteGranted);
        Assert.True(repeatedResponse.VoteGranted);
        Assert.Equal(1, repeatedResponse.Term);

        Assert.Equal(
            "coordinator-peer-1",
            state.GetSnapshot().VotedFor);
    }

    [Fact]
    public async Task RequestVoteEndpoint_ReturnsVoteDecision()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var response =
            await client.PostAsJsonAsync(
                "/api/raft/request-vote",
                new RequestVoteRequest(
                    Term: 1,
                    CandidateId:
                        "coordinator-2",
                    LastLogIndex: 0,
                    LastLogTerm: 0));

        Assert.Equal(
            HttpStatusCode.OK,
            response.StatusCode);

        var result =
            await response.Content
                .ReadFromJsonAsync<
                    RequestVoteResponse>();

        Assert.NotNull(result);
        Assert.True(result.VoteGranted);
        Assert.Equal(1, result.Term);
    }

    [Fact]
    public void AppendEntries_RejectsStaleTerm()
    {
        var state = new RaftNodeState(
            Options.Create(
                CreateValidOptions()),
            new InMemoryRaftPersistentStateStore());

        var election =
            state.BeginElection();

        Assert.True(
            state.TryBecomeLeader(
                election.CurrentTerm));

        var response =
            state.HandleAppendEntries(
                new AppendEntriesRequest(
                    Term: 0,
                    LeaderId:
                        "coordinator-peer-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    LeaderCommit: 0,
                    Entries: []));

        Assert.False(response.Success);
        Assert.Equal(1, response.Term);

        Assert.Equal(
            RaftNodeRole.Leader,
            state.GetSnapshot().Role);
    }

    [Fact]
    public void AppendEntries_HigherTermMakesLeaderFollower()
    {
        var state = new RaftNodeState(
            Options.Create(
                CreateValidOptions()),
            new InMemoryRaftPersistentStateStore());

        var election =
            state.BeginElection();

        Assert.True(
            state.TryBecomeLeader(
                election.CurrentTerm));

        var response =
            state.HandleAppendEntries(
                new AppendEntriesRequest(
                    Term: 2,
                    LeaderId:
                        "coordinator-peer-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    LeaderCommit: 0,
                    Entries: []));

        Assert.True(response.Success);
        Assert.Equal(2, response.Term);

        var snapshot =
            state.GetSnapshot();

        Assert.Equal(
            RaftNodeRole.Follower,
            snapshot.Role);

        Assert.Equal(2, snapshot.CurrentTerm);

        Assert.Null(snapshot.VotedFor);

        Assert.Equal(
            "coordinator-peer-1",
            snapshot.LeaderId);
    }

    [Fact]
    public void AppendEntries_SameTermMakesCandidateFollower()
    {
        var state = new RaftNodeState(
            Options.Create(
                CreateValidOptions()),
            new InMemoryRaftPersistentStateStore());

        var election =
            state.BeginElection();

        var response =
            state.HandleAppendEntries(
                new AppendEntriesRequest(
                    Term:
                        election.CurrentTerm,
                    LeaderId:
                        "coordinator-peer-1",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    LeaderCommit: 0,
                    Entries: []));

        Assert.True(response.Success);

        var snapshot =
            state.GetSnapshot();

        Assert.Equal(
            RaftNodeRole.Follower,
            snapshot.Role);

        Assert.Equal(
            "coordinator-peer-1",
            snapshot.LeaderId);

        Assert.Equal(
            "coordinator-test",
            snapshot.VotedFor);
    }

    [Fact]
    public async Task AppendEntriesEndpoint_RecordsLeader()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var response =
            await client.PostAsJsonAsync(
                "/api/raft/append-entries",
                new AppendEntriesRequest(
                    Term: 1,
                    LeaderId: "coordinator-2",
                    PrevLogIndex: 0,
                    PrevLogTerm: 0,
                    LeaderCommit: 0,
                    Entries: []));

        Assert.Equal(
            HttpStatusCode.OK,
            response.StatusCode);

        var result =
            await response.Content
                .ReadFromJsonAsync<
                    AppendEntriesResponse>();

        Assert.NotNull(result);
        Assert.True(result.Success);
        Assert.Equal(1, result.Term);

        var status =
            await client.GetFromJsonAsync<
                RaftNodeSnapshot>(
                    "/api/raft/status");

        Assert.NotNull(status);

        Assert.Equal(
            RaftNodeRole.Follower,
            status.Role);

        Assert.Equal(
            "coordinator-2",
            status.LeaderId);
    }

    [Fact]
    public void OptionsValidator_InvalidHeartbeatInterval_IsRejected()
    {
        var options =
            CreateValidOptions();

        options.HeartbeatIntervalMilliseconds = 0;

        var validator =
            new RaftOptionsValidator();

        var result =
            validator.Validate(
                null,
                options);

        Assert.True(result.Failed);

        Assert.Contains(
            "HeartbeatIntervalMilliseconds",
            result.FailureMessage ??
                string.Empty);
    }

    private static RaftOptions CreateValidOptions()
    {
        return new RaftOptions
        {
            NodeId = "coordinator-test",
            Peers = new System.Collections.Generic.List<RaftPeerOptions>
            {
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
            }
        };
    }

    private sealed class
    InMemoryRaftPersistentStateStore
    : IRaftPersistentStateStore
    {
        private readonly System.Collections.Generic.Dictionary<
            string,
            RaftPersistentState> _states = new();

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