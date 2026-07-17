using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Tests.Raft;

public sealed class RaftAppendEntriesLogTests
{
    [Fact]
    public void AppendEntries_AppendsEntriesAfterMatchingPreviousEntry()
    {
        var setup =
            CreateNodeState(
                CreateEntry(1, 1));

        var second =
            CreateEntry(2, 2);

        var third =
            CreateEntry(3, 2);

        var response =
            setup.State.HandleAppendEntries(
                new AppendEntriesRequest(
                    Term: 2,
                    LeaderId: "peer-1",
                    PrevLogIndex: 1,
                    PrevLogTerm: 1,
                    LeaderCommit: 0,
                    Entries:
                    [
                        second,
                        third
                    ]));

        Assert.True(response.Success);

        Assert.Equal(
            second,
            setup.Store.Get(2));

        Assert.Equal(
            third,
            setup.Store.Get(3));

        Assert.Equal(
            "peer-1",
            setup.State
                .GetSnapshot()
                .LeaderId);
    }

    [Fact]
    public void AppendEntries_RejectsMissingPreviousEntry()
    {
        var setup =
            CreateNodeState();

        var response =
            setup.State.HandleAppendEntries(
                new AppendEntriesRequest(
                    Term: 1,
                    LeaderId: "peer-1",
                    PrevLogIndex: 1,
                    PrevLogTerm: 1,
                    LeaderCommit: 0,
                    Entries: []));

        Assert.False(response.Success);
        Assert.Empty(setup.Store.GetFrom(1));
    }

    [Fact]
    public void AppendEntries_RejectsDifferentPreviousTerm()
    {
        var setup =
            CreateNodeState(
                CreateEntry(1, 1));

        var response =
            setup.State.HandleAppendEntries(
                new AppendEntriesRequest(
                    Term: 2,
                    LeaderId: "peer-1",
                    PrevLogIndex: 1,
                    PrevLogTerm: 2,
                    LeaderCommit: 0,
                    Entries: []));

        Assert.False(response.Success);

        Assert.Equal(
            1,
            setup.Store.GetLast()!.Term);
    }

    [Fact]
    public void AppendEntries_ReplacesConflictingSuffix()
    {
        var first =
            CreateEntry(1, 1);

        var oldSecond =
            CreateEntry(2, 1);

        var oldThird =
            CreateEntry(3, 1);

        var setup =
            CreateNodeState(
                first,
                oldSecond,
                oldThird);

        var newSecond =
            CreateEntry(2, 2);

        var newThird =
            CreateEntry(3, 2);

        var response =
            setup.State.HandleAppendEntries(
                new AppendEntriesRequest(
                    Term: 2,
                    LeaderId: "peer-1",
                    PrevLogIndex: 1,
                    PrevLogTerm: 1,
                    LeaderCommit: 0,
                    Entries:
                    [
                        newSecond,
                        newThird
                    ]));

        Assert.True(response.Success);

        var entries =
            setup.Store.GetFrom(1);

        Assert.Equal(3, entries.Count);
        Assert.Equal(first, entries[0]);
        Assert.Equal(newSecond, entries[1]);
        Assert.Equal(newThird, entries[2]);
    }

    [Fact]
    public void AppendEntries_RepeatedRequestIsIdempotent()
    {
        var first =
            CreateEntry(1, 1);

        var second =
            CreateEntry(2, 1);

        var setup =
            CreateNodeState(first);

        var request =
            new AppendEntriesRequest(
                Term: 1,
                LeaderId: "peer-1",
                PrevLogIndex: 1,
                PrevLogTerm: 1,
                LeaderCommit: 0,
                Entries: [second]);

        var firstResponse =
            setup.State.HandleAppendEntries(
                request);

        var repeatedResponse =
            setup.State.HandleAppendEntries(
                request);

        Assert.True(firstResponse.Success);
        Assert.True(repeatedResponse.Success);

        Assert.Equal(
            2,
            setup.Store.GetFrom(1).Count);
    }

    private static TestSetup CreateNodeState(
        params RaftLogEntry[] entries)
    {
        var store =
            new InMemoryRaftLogStore();

        foreach (var entry in entries)
        {
            store.Append(entry);
        }

        var logManager =
            new RaftLogManager(
                store,
                new CoordinatorCommandSerializer());

        var state =
            new RaftNodeState(
                Options.Create(
                    new RaftOptions
                    {
                        NodeId =
                            "coordinator-test",
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

        return new TestSetup(
            state,
            store);
    }

    private static RaftLogEntry CreateEntry(
        long logIndex,
        long term)
    {
        return new RaftLogEntry(
            LogIndex: logIndex,
            Term: term,
            CommandId: Guid.NewGuid(),
            CommandType:
                "CreateExperimentCommand",
            CommandPayloadJson:
                $"{{\"index\":{logIndex}," +
                $"\"term\":{term}}}");
    }

    private sealed record TestSetup(
        RaftNodeState State,
        InMemoryRaftLogStore Store);

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
            var existing =
                Get(entry.LogIndex);

            if (existing == entry)
            {
                return;
            }

            if (existing is not null)
            {
                throw new InvalidOperationException(
                    "Conflicting entry.");
            }

            var expectedIndex =
                (GetLast()?.LogIndex ?? 0) + 1;

            if (entry.LogIndex != expectedIndex)
            {
                throw new InvalidOperationException(
                    "Log gap.");
            }

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