using Coordinator.Commands;

namespace Coordinator.Raft;

public sealed class RaftLogManager(
    IRaftLogStore logStore,
    CoordinatorCommandSerializer commandSerializer)
{
    private readonly object _sync = new();

    public RaftLogPosition GetLastPosition()
    {
        var lastEntry =
            logStore.GetLast();

        return lastEntry is null
            ? new RaftLogPosition(
                LogIndex: 0,
                Term: 0)
            : new RaftLogPosition(
                lastEntry.LogIndex,
                lastEntry.Term);
    }

    public RaftLogEntry? GetEntry(long logIndex)
    {
        if (logIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(logIndex));
        }

        return logStore.Get(logIndex);
    }

    public IReadOnlyList<RaftLogEntry> GetEntriesFrom(
        long startIndex)
    {
        if (startIndex <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(startIndex));
        }

        return logStore.GetFrom(startIndex);
    }

    public long GetTermAt(long logIndex)
    {
        if (logIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(logIndex));
        }

        if (logIndex == 0)
        {
            return 0;
        }

        return logStore.Get(logIndex)?.Term
            ?? throw new InvalidOperationException(
                $"Raft log entry {logIndex} does not exist.");
    }

    public RaftLogEntry AppendCommand(
        CoordinatorCommand command,
        long term)
    {
        ArgumentNullException.ThrowIfNull(command);

        if (term <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(term),
                "Raft log term must be positive.");
        }

        lock (_sync)
        {
            var serializedCommand =
                commandSerializer.Serialize(command);

            var lastEntry =
                logStore.GetLast();

            var nextIndex =
                checked(
                    (lastEntry?.LogIndex ?? 0) + 1);

            var entry =
                new RaftLogEntry(
                    LogIndex: nextIndex,
                    Term: term,
                    CommandId: command.CommandId,
                    CommandType:
                        serializedCommand.CommandType,
                    CommandPayloadJson:
                        serializedCommand.PayloadJson);

            logStore.Append(entry);

            return entry;
        }
    }

    public bool TryAppendEntries(
        long prevLogIndex,
        long prevLogTerm,
        IReadOnlyList<RaftLogEntry> entries,
        long committedThroughIndex = 0)
    {
        ArgumentNullException.ThrowIfNull(entries);

        if (prevLogIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(prevLogIndex));
        }

        if (prevLogTerm < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(prevLogTerm));
        }

        ValidateReplicatedEntries(
            prevLogIndex,
            entries);

        if (committedThroughIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(committedThroughIndex));
        }

        lock (_sync)
        {
            if (prevLogIndex == 0)
            {
                if (prevLogTerm != 0)
                {
                    return false;
                }
            }
            else
            {
                var previousEntry =
                    logStore.Get(prevLogIndex);

                if (previousEntry is null ||
                    previousEntry.Term != prevLogTerm)
                {
                    return false;
                }
            }

            foreach (var incomingEntry in entries)
            {
                var existingEntry =
                    logStore.Get(
                        incomingEntry.LogIndex);

                if (existingEntry == incomingEntry)
                {
                    continue;
                }

                if (existingEntry is not null)
                {
                    if (incomingEntry.LogIndex <=
                        committedThroughIndex)
                    {
                        return false;
                    }

                    logStore.DeleteFrom(
                        incomingEntry.LogIndex);
                }

                logStore.Append(incomingEntry);
            }

            return true;
        }
    }

    public CoordinatorCommand DeserializeCommand(
        RaftLogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var command =
            commandSerializer.Deserialize(
                entry.CommandType,
                entry.CommandPayloadJson);

        if (command.CommandId != entry.CommandId)
        {
            throw new InvalidOperationException(
                $"Raft log entry {entry.LogIndex} " +
                "contains a CommandId that does not " +
                "match its serialized payload.");
        }

        return command;
    }

    private static void ValidateReplicatedEntries(
        long prevLogIndex,
        IReadOnlyList<RaftLogEntry> entries)
    {
        var expectedIndex =
            checked(prevLogIndex + 1);

        foreach (var entry in entries)
        {
            if (entry.LogIndex != expectedIndex)
            {
                throw new InvalidOperationException(
                    $"Expected replicated log index " +
                    $"{expectedIndex}, but received " +
                    $"{entry.LogIndex}.");
            }

            if (entry.Term <= 0)
            {
                throw new InvalidOperationException(
                    "Replicated entry term must be positive.");
            }

            if (entry.CommandId == Guid.Empty ||
                string.IsNullOrWhiteSpace(
                    entry.CommandType) ||
                string.IsNullOrWhiteSpace(
                    entry.CommandPayloadJson))
            {
                throw new InvalidOperationException(
                    $"Replicated entry {entry.LogIndex} " +
                    "does not contain a valid command.");
            }

            expectedIndex =
                checked(expectedIndex + 1);
        }
    }
}