using Coordinator.Raft;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Raft;

public sealed class RaftCommitTests
{
    [Fact]
    public void LeaderCommit_AdvancesWithQuorum()
    {
        var setup =
            CreateSetup(
                CreateEntry(1, 1),
                CreateEntry(2, 2));

        var commitIndex =
            setup.CommitManager
                .TryAdvanceLeaderCommit(
                    currentTerm: 2,
                    clusterSize: 3,
                    followerMatchIndexes:
                        [2, 0]);

        Assert.Equal(2, commitIndex);

        Assert.Equal(
            2,
            setup.CommitManager
                .GetState()
                .CommitIndex);
    }

    [Fact]
    public void LeaderCommit_DoesNotAdvanceWithoutQuorum()
    {
        var setup =
            CreateSetup(
                CreateEntry(1, 1));

        var commitIndex =
            setup.CommitManager
                .TryAdvanceLeaderCommit(
                    currentTerm: 1,
                    clusterSize: 3,
                    followerMatchIndexes:
                        [0, 0]);

        Assert.Equal(0, commitIndex);
    }

    [Fact]
    public void LeaderCannotDirectlyCommitEntryFromOlderTerm()
    {
        var setup =
            CreateSetup(
                CreateEntry(1, 1));

        var commitIndex =
            setup.CommitManager
                .TryAdvanceLeaderCommit(
                    currentTerm: 2,
                    clusterSize: 3,
                    followerMatchIndexes:
                        [1, 1]);

        Assert.Equal(0, commitIndex);
    }

    [Fact]
    public void FollowerCommit_IsLimitedByLocalLog()
    {
        var setup =
            CreateSetup(
                CreateEntry(1, 1),
                CreateEntry(2, 1));

        var commitIndex =
            setup.CommitManager
                .AdvanceFollowerCommit(
                    leaderCommit: 10);

        Assert.Equal(2, commitIndex);
    }

    [Fact]
    public void LastApplied_MustAdvanceSequentially()
    {
        var setup =
            CreateSetup(
                CreateEntry(1, 1),
                CreateEntry(2, 1));

        setup.CommitManager
            .AdvanceFollowerCommit(2);

        Assert.Throws<InvalidOperationException>(
            () => setup.CommitManager
                .MarkApplied(2));

        setup.CommitManager.MarkApplied(1);
        setup.CommitManager.MarkApplied(2);

        Assert.Equal(
            new RaftCommitState(
                CommitIndex: 2,
                LastApplied: 2),
            setup.CommitManager.GetState());
    }

    [Fact]
    public void CommitState_SurvivesCoordinatorRestart()
    {
        var directory = Path.Combine(
            Path.GetTempPath(),
            "distributed-experiment-platform-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(directory);

        var databasePath = Path.Combine(
            directory,
            "raft-commit-tests.db");

        try
        {
            using (var firstFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var client =
                    firstFactory.CreateClient();

                var logStore =
                    firstFactory.Services
                        .GetRequiredService<
                            IRaftLogStore>();

                logStore.Append(
                    CreateEntry(1, 1));

                var commitManager =
                    firstFactory.Services
                        .GetRequiredService<
                            RaftCommitManager>();

                commitManager
                    .TryAdvanceLeaderCommit(
                        currentTerm: 1,
                        clusterSize: 3,
                        followerMatchIndexes:
                            [1, 0]);

                commitManager.MarkApplied(1);
            }

            using (var secondFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var client =
                    secondFactory.CreateClient();

                var restored =
                    secondFactory.Services
                        .GetRequiredService<
                            RaftCommitManager>()
                        .GetState();

                Assert.Equal(
                    new RaftCommitState(1, 1),
                    restored);
            }
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(
                    directory,
                    recursive: true);
            }
        }
    }

    private static TestSetup CreateSetup(
        params RaftLogEntry[] entries)
    {
        var logStore =
            new InMemoryLogStore();

        foreach (var entry in entries)
        {
            logStore.Append(entry);
        }

        var logManager =
            new RaftLogManager(
                logStore,
                new CoordinatorCommandSerializer());

        var commitManager =
            new RaftCommitManager(
                logManager,
                new InMemoryCommitStateStore());

        return new TestSetup(
            commitManager);
    }

    private static RaftLogEntry CreateEntry(
        long index,
        long term)
    {
        return new RaftLogEntry(
            index,
            term,
            Guid.NewGuid(),
            "CreateExperimentCommand",
            $"{{\"index\":{index}}}");
    }

    private sealed record TestSetup(
        RaftCommitManager CommitManager);

    private sealed class
        InMemoryCommitStateStore
        : IRaftCommitStateStore
    {
        private RaftCommitState _state =
            new(0, 0);

        public RaftCommitState LoadOrCreate()
        {
            return _state;
        }

        public void Save(
            long commitIndex,
            long lastApplied)
        {
            _state = new RaftCommitState(
                commitIndex,
                lastApplied);
        }
    }

    private sealed class InMemoryLogStore
        : IRaftLogStore
    {
        private readonly List<RaftLogEntry>
            _entries = [];

        public RaftLogEntry? Get(long logIndex) =>
            _entries.SingleOrDefault(
                entry =>
                    entry.LogIndex == logIndex);

        public RaftLogEntry? GetLast() =>
            _entries.LastOrDefault();

        public IReadOnlyList<RaftLogEntry> GetFrom(
            long startIndex) =>
            _entries
                .Where(entry =>
                    entry.LogIndex >= startIndex)
                .ToArray();

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
}