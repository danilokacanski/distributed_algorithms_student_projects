using System.Collections.Concurrent;
using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Tests.Raft;

public sealed class RaftVotingLogTests
{
    [Fact]
    public void RequestVote_RejectsCandidateWithOlderLogTerm()
    {
        var state =
            CreateNodeState(
                (1, 3),
                (2, 3));

        var response =
            state.HandleRequestVote(
                new RequestVoteRequest(
                    Term: 1,
                    CandidateId: "candidate-1",
                    LastLogIndex: 10,
                    LastLogTerm: 2));

        Assert.False(response.VoteGranted);
        Assert.Null(
            state.GetSnapshot().VotedFor);
    }

    [Fact]
    public void RequestVote_RejectsShorterLogWithSameTerm()
    {
        var state =
            CreateNodeState(
                (1, 3),
                (2, 3));

        var response =
            state.HandleRequestVote(
                new RequestVoteRequest(
                    Term: 1,
                    CandidateId: "candidate-1",
                    LastLogIndex: 1,
                    LastLogTerm: 3));

        Assert.False(response.VoteGranted);
        Assert.Null(
            state.GetSnapshot().VotedFor);
    }

    [Fact]
    public void RequestVote_GrantsCandidateWithNewerLogTerm()
    {
        var state =
            CreateNodeState(
                (1, 2),
                (2, 2),
                (3, 2));

        var response =
            state.HandleRequestVote(
                new RequestVoteRequest(
                    Term: 1,
                    CandidateId: "candidate-1",
                    LastLogIndex: 1,
                    LastLogTerm: 3));

        Assert.True(response.VoteGranted);

        Assert.Equal(
            "candidate-1",
            state.GetSnapshot().VotedFor);
    }

    [Fact]
    public async Task ElectionRequest_ContainsLocalLastLogPosition()
    {
        var state =
            CreateNodeState(
                (1, 2),
                (2, 3));

        state.ObserveHigherTerm(3);

        var peerClient =
            new CapturingPeerClient();

        var heartbeatSender =
            new RaftHeartbeatSender(
                state,
                peerClient);

        var electionService =
            new RaftElectionService(
                state,
                peerClient,
                heartbeatSender);

        var result =
            await electionService.StartElectionAsync();

        Assert.True(result.Won);

        Assert.Equal(
            2,
            peerClient.VoteRequests.Count);

        Assert.All(
            peerClient.VoteRequests,
            request =>
            {
                Assert.Equal(
                    2,
                    request.LastLogIndex);

                Assert.Equal(
                    3,
                    request.LastLogTerm);
            });
    }

    private static RaftNodeState CreateNodeState(
        params (long LogIndex, long Term)[] entries)
    {
        var logStore =
            new InMemoryRaftLogStore();

        foreach (var entry in entries)
        {
            logStore.Append(
                new RaftLogEntry(
                    LogIndex: entry.LogIndex,
                    Term: entry.Term,
                    CommandId: Guid.NewGuid(),
                    CommandType:
                        "CreateExperimentCommand",
                    CommandPayloadJson:
                        $"{{\"index\":{entry.LogIndex}}}"));
        }

        var logManager =
            new RaftLogManager(
                logStore,
                new CoordinatorCommandSerializer());

        return new RaftNodeState(
            Options.Create(
                new RaftOptions
                {
                    NodeId = "coordinator-test",
                    Peers =
                    [
                        new RaftPeerOptions
                        {
                            NodeId = "peer-1",
                            BaseUrl =
                                "http://localhost:6002"
                        },
                        new RaftPeerOptions
                        {
                            NodeId = "peer-2",
                            BaseUrl =
                                "http://localhost:6003"
                        }
                    ]
                }),
            new TestPersistentStateStore(),
            timeProvider: null,
            logManager: logManager);
    }

    private sealed class CapturingPeerClient
        : IRaftPeerClient
    {
        public ConcurrentBag<RequestVoteRequest>
            VoteRequests { get; } = [];

        public Task<RequestVoteResponse>
            RequestVoteAsync(
                RaftPeerSnapshot peer,
                RequestVoteRequest request,
                CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            VoteRequests.Add(request);

            return Task.FromResult(
                new RequestVoteResponse(
                    Term: request.Term,
                    VoteGranted: true));
        }

        public Task<AppendEntriesResponse>
            AppendEntriesAsync(
                RaftPeerSnapshot peer,
                AppendEntriesRequest request,
                CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();

            return Task.FromResult(
                new AppendEntriesResponse(
                    Term: request.Term,
                    Success: true));
        }
    }

    private sealed class InMemoryRaftLogStore
        : IRaftLogStore
    {
        private readonly List<RaftLogEntry>
            _entries = [];

        public RaftLogEntry? Get(long logIndex)
        {
            return _entries.SingleOrDefault(
                entry =>
                    entry.LogIndex == logIndex);
        }

        public RaftLogEntry? GetLast()
        {
            return _entries
                .OrderByDescending(entry =>
                    entry.LogIndex)
                .FirstOrDefault();
        }

        public IReadOnlyList<RaftLogEntry> GetFrom(
            long startIndex)
        {
            return _entries
                .Where(entry =>
                    entry.LogIndex >= startIndex)
                .OrderBy(entry =>
                    entry.LogIndex)
                .ToArray();
        }

        public void Append(RaftLogEntry entry)
        {
            _entries.Add(entry);
        }

        public int DeleteFrom(long startIndex)
        {
            return _entries.RemoveAll(
                entry =>
                    entry.LogIndex >= startIndex);
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