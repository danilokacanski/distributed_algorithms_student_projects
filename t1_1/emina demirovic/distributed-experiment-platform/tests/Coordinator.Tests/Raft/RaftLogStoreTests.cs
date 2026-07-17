using Coordinator.Raft;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Raft;

public sealed class RaftLogStoreTests
{
    [Fact]
    public void EmptyStore_HasNoEntries()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var store =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Null(store.GetLast());
        Assert.Null(store.Get(0));
        Assert.Null(store.Get(1));
        Assert.Empty(store.GetFrom(1));
    }

    [Fact]
    public void Append_StoresEntriesInLogOrder()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var store =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        var first = CreateEntry(1);
        var second = CreateEntry(2);
        var third = CreateEntry(3);

        store.Append(first);
        store.Append(second);
        store.Append(third);

        Assert.Equal(first, store.Get(1));
        Assert.Equal(third, store.GetLast());

        var suffix =
            store.GetFrom(2);

        Assert.Equal(2, suffix.Count);
        Assert.Equal(second, suffix[0]);
        Assert.Equal(third, suffix[1]);
    }

    [Fact]
    public void Append_RepeatedIdenticalEntry_IsIdempotent()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var store =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        var entry = CreateEntry(1);

        store.Append(entry);
        store.Append(entry);

        var entries = store.GetFrom(1);

        Assert.Single(entries);
        Assert.Equal(entry, entries[0]);
    }

    [Fact]
    public void Append_RejectsGapAndConflictingEntry()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var store =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Throws<InvalidOperationException>(
            () => store.Append(
                CreateEntry(2)));

        store.Append(CreateEntry(1));

        Assert.Throws<InvalidOperationException>(
            () => store.Append(
                CreateEntry(
                    logIndex: 1,
                    term: 2)));
    }

    [Fact]
    public void DeleteFrom_RemovesLogSuffix()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var store =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        store.Append(CreateEntry(1));
        store.Append(CreateEntry(2));
        store.Append(CreateEntry(3));

        var deletedCount =
            store.DeleteFrom(2);

        Assert.Equal(2, deletedCount);

        var remaining =
            store.GetFrom(1);

        Assert.Single(remaining);
        Assert.Equal(1, remaining[0].LogIndex);
        Assert.Equal(1, store.GetLast()!.LogIndex);
    }

    [Fact]
    public void LogEntries_SurviveCoordinatorRestart()
    {
        var databaseDirectory = Path.Combine(
            Path.GetTempPath(),
            "distributed-experiment-platform-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(
            databaseDirectory);

        var databasePath = Path.Combine(
            databaseDirectory,
            "raft-log-tests.db");

        var entry = CreateEntry(1);

        try
        {
            using (var firstFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var firstClient =
                    firstFactory.CreateClient();

                var firstStore =
                    firstFactory.Services
                        .GetRequiredService<
                            IRaftLogStore>();

                firstStore.Append(entry);
            }

            using (var secondFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var secondClient =
                    secondFactory.CreateClient();

                var restoredStore =
                    secondFactory.Services
                        .GetRequiredService<
                            IRaftLogStore>();

                Assert.Equal(
                    entry,
                    restoredStore.GetLast());
            }
        }
        finally
        {
            if (Directory.Exists(
                databaseDirectory))
            {
                Directory.Delete(
                    databaseDirectory,
                    recursive: true);
            }
        }
    }

    private static RaftLogEntry CreateEntry(
        long logIndex,
        long term = 1,
        Guid? commandId = null)
    {
        return new RaftLogEntry(
            LogIndex: logIndex,
            Term: term,
            CommandId:
                commandId ?? Guid.NewGuid(),
            CommandType:
                "CreateExperimentCommand",
            CommandPayloadJson:
                $"{{\"logIndex\":{logIndex}}}");
    }
}