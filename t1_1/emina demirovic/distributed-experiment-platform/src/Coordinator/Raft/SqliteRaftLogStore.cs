using Coordinator.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Coordinator.Raft;

public sealed class SqliteRaftLogStore(
    IDbContextFactory<CoordinatorDbContext> dbContextFactory,
    IOptions<RaftOptions> options)
    : IRaftLogStore
{
    private readonly object _sync = new();

    private readonly string _nodeId =
        options.Value.NodeId;

    public RaftLogEntry? Get(long logIndex)
    {
        if (logIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(logIndex));
        }

        if (logIndex == 0)
        {
            return null;
        }

        using var dbContext =
            dbContextFactory.CreateDbContext();

        var entity = dbContext.RaftLogEntries
            .AsNoTracking()
            .SingleOrDefault(entry =>
                entry.NodeId == _nodeId &&
                entry.LogIndex == logIndex);

        return entity is null
            ? null
            : ToModel(entity);
    }

    public RaftLogEntry? GetLast()
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        var entity = dbContext.RaftLogEntries
            .AsNoTracking()
            .Where(entry =>
                entry.NodeId == _nodeId)
            .OrderByDescending(entry =>
                entry.LogIndex)
            .FirstOrDefault();

        return entity is null
            ? null
            : ToModel(entity);
    }

    public IReadOnlyList<RaftLogEntry> GetFrom(
        long startIndex)
    {
        if (startIndex <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(startIndex));
        }

        using var dbContext =
            dbContextFactory.CreateDbContext();

        return dbContext.RaftLogEntries
            .AsNoTracking()
            .Where(entry =>
                entry.NodeId == _nodeId &&
                entry.LogIndex >= startIndex)
            .OrderBy(entry =>
                entry.LogIndex)
            .Select(entry =>
                new RaftLogEntry(
                    entry.LogIndex,
                    entry.Term,
                    entry.CommandId,
                    entry.CommandType,
                    entry.CommandPayloadJson))
            .ToArray();
    }

    public void Append(RaftLogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        ValidateEntry(entry);

        lock (_sync)
        {
            using var dbContext =
                dbContextFactory.CreateDbContext();

            using var transaction =
                dbContext.Database.BeginTransaction();

            var existingAtIndex =
                dbContext.RaftLogEntries
                    .AsNoTracking()
                    .SingleOrDefault(existing =>
                        existing.NodeId == _nodeId &&
                        existing.LogIndex ==
                            entry.LogIndex);

            if (existingAtIndex is not null)
            {
                if (ContainsSameEntry(
                    existingAtIndex,
                    entry))
                {
                    return;
                }

                throw new InvalidOperationException(
                    $"Raft log index {entry.LogIndex} " +
                    "already contains a different entry.");
            }

            var commandAlreadyExists =
                dbContext.RaftLogEntries.Any(existing =>
                    existing.NodeId == _nodeId &&
                    existing.CommandId ==
                        entry.CommandId);

            if (commandAlreadyExists)
            {
                throw new InvalidOperationException(
                    $"Raft command '{entry.CommandId}' " +
                    "already exists at another log index.");
            }

            var lastIndex =
                dbContext.RaftLogEntries
                    .Where(existing =>
                        existing.NodeId == _nodeId)
                    .Select(existing =>
                        (long?)existing.LogIndex)
                    .Max() ?? 0;

            var expectedIndex =
                checked(lastIndex + 1);

            if (entry.LogIndex != expectedIndex)
            {
                throw new InvalidOperationException(
                    $"Expected Raft log index " +
                    $"{expectedIndex}, but received " +
                    $"{entry.LogIndex}.");
            }

            dbContext.RaftLogEntries.Add(
                new RaftLogEntryEntity
                {
                    NodeId = _nodeId,
                    LogIndex = entry.LogIndex,
                    Term = entry.Term,
                    CommandId = entry.CommandId,
                    CommandType = entry.CommandType,
                    CommandPayloadJson =
                        entry.CommandPayloadJson
                });

            dbContext.SaveChanges();
            transaction.Commit();
        }
    }

    public int DeleteFrom(long startIndex)
    {
        if (startIndex <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(startIndex));
        }

        lock (_sync)
        {
            using var dbContext =
                dbContextFactory.CreateDbContext();

            return dbContext.RaftLogEntries
                .Where(entry =>
                    entry.NodeId == _nodeId &&
                    entry.LogIndex >= startIndex)
                .ExecuteDelete();
        }
    }

    private static void ValidateEntry(
        RaftLogEntry entry)
    {
        if (entry.LogIndex <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(entry),
                "Raft log index must be positive.");
        }

        if (entry.Term <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(entry),
                "Raft log term must be positive.");
        }

        if (entry.CommandId == Guid.Empty)
        {
            throw new ArgumentException(
                "CommandId is required.",
                nameof(entry));
        }

        if (string.IsNullOrWhiteSpace(
            entry.CommandType))
        {
            throw new ArgumentException(
                "CommandType is required.",
                nameof(entry));
        }

        if (string.IsNullOrWhiteSpace(
            entry.CommandPayloadJson))
        {
            throw new ArgumentException(
                "CommandPayloadJson is required.",
                nameof(entry));
        }
    }

    private static bool ContainsSameEntry(
        RaftLogEntryEntity existing,
        RaftLogEntry requested)
    {
        return
            existing.Term == requested.Term &&
            existing.CommandId ==
                requested.CommandId &&
            string.Equals(
                existing.CommandType,
                requested.CommandType,
                StringComparison.Ordinal) &&
            string.Equals(
                existing.CommandPayloadJson,
                requested.CommandPayloadJson,
                StringComparison.Ordinal);
    }

    private static RaftLogEntry ToModel(
        RaftLogEntryEntity entity)
    {
        return new RaftLogEntry(
            entity.LogIndex,
            entity.Term,
            entity.CommandId,
            entity.CommandType,
            entity.CommandPayloadJson);
    }
}