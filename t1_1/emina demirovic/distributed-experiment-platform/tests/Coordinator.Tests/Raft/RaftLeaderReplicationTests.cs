using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Tests.Raft;

public sealed class RaftLeaderReplicationTests
{
    [Fact]
    public async Task Leader_BacktracksUntilFollowerLogMatches()
    {
        var store =
            new InMemoryRaftLogStore();

        store.Append(CreateEntry(1, 1));
        store.Append(CreateEntry(2, 2));
        store.Append(CreateEntry(3, 2));

        var logManager =
            new RaftLogManager(
                store,
                new CoordinatorCommandSerializer());

        var state =
            new RaftNodeState(
                Options.Create(
                    new RaftOptions
                    {
                        NodeId = "leader",
                        Peers =
                        [
                            new RaftPeerOptions
                            {
                                NodeId = "follower",
                                BaseUrl =
                                    "http://localhost:6002"
                            }
                        ]
                    }),
                new TestPersistentStateStore(),
                timeProvider: null,
                logManager: logManager);

        var election =
            state.BeginElection();

        Assert.True(
            state.TryBecomeLeader(
                election.CurrentTerm));

        var peerClient =
            new MatchingAtIndexOnePeerClient();

        var tracker =
            new RaftReplicationTracker();

        var sender =
            new RaftHeartbeatSender(
                state,
                peerClient,
                logManager,
                tracker);

        await sender.SendHeartbeatAsync();
        await sender.SendHeartbeatAsync();
        await sender.SendHeartbeatAsync();

        Assert.Equal(
            3,
            peerClient.Requests.Count);

        Assert.Equal(
            3,
            peerClient.Requests[0].PrevLogIndex);

        Assert.Equal(
            2,
            peerClient.Requests[1].PrevLogIndex);

        var successfulRequest =
            peerClient.Requests[2];

        Assert.Equal(
            1,
            successfulRequest.PrevLogIndex);

        Assert.Equal(
            [2L, 3L],
            successfulRequest.Entries
                .Select(entry => entry.LogIndex)
                .ToArray());

        var replicationState =
            tracker.Get("follower");

        Assert.Equal(
            3,
            replicationState.MatchIndex);

        Assert.Equal(
            4,
            replicationState.NextIndex);
    }

    private static RaftLogEntry CreateEntry(
        long index,
        long term)
    {
        return new RaftLogEntry(
            LogIndex: index,
            Term: term,
            CommandId: Guid.NewGuid(),
            CommandType:
                "CreateExperimentCommand",
            CommandPayloadJson:
                $"{{\"index\":{index}}}");
    }

    private sealed class
        MatchingAtIndexOnePeerClient
        : IRaftPeerClient
    {
        public List<AppendEntriesRequest>
            Requests { get; } = [];

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
            Requests.Add(request);

            return Task.FromResult(
                new AppendEntriesResponse(
                    request.Term,
                    Success:
                        request.PrevLogIndex == 1));
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
            return _entries.LastOrDefault();
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
        private RaftPersistentState _state =
            new(0, null);

        public RaftPersistentState LoadOrCreate(
            string nodeId)
        {
            return _state;
        }

        public void Save(
            string nodeId,
            long currentTerm,
            string? votedFor)
        {
            _state =
                new RaftPersistentState(
                    currentTerm,
                    votedFor);
        }
    }
}