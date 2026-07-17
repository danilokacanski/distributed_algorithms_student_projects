using Coordinator.Data;
using Microsoft.EntityFrameworkCore;

namespace Coordinator.Raft;

public sealed class SqliteRaftPersistentStateStore(
    IDbContextFactory<CoordinatorDbContext> dbContextFactory)
    : IRaftPersistentStateStore
{
    public RaftPersistentState LoadOrCreate(
        string nodeId)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        var entity = dbContext.RaftPersistentStates
            .SingleOrDefault(state =>
                state.NodeId == nodeId);

        if (entity is null)
        {
            entity = new RaftPersistentStateEntity
            {
                NodeId = nodeId,
                CurrentTerm = 0,
                VotedFor = null
            };

            dbContext.RaftPersistentStates.Add(entity);
            dbContext.SaveChanges();
        }

        return new RaftPersistentState(
            entity.CurrentTerm,
            entity.VotedFor);
    }

    public void Save(
        string nodeId,
        long currentTerm,
        string? votedFor)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        var entity = dbContext.RaftPersistentStates
            .SingleOrDefault(state =>
                state.NodeId == nodeId);

        if (entity is null)
        {
            entity = new RaftPersistentStateEntity
            {
                NodeId = nodeId,
                CurrentTerm = currentTerm,
                VotedFor = votedFor
            };

            dbContext.RaftPersistentStates.Add(entity);
        }
        else
        {
            if (currentTerm < entity.CurrentTerm)
            {
                throw new InvalidOperationException(
                    $"Raft term cannot move backwards from " +
                    $"{entity.CurrentTerm} to {currentTerm}.");
            }

            if (currentTerm == entity.CurrentTerm &&
                entity.VotedFor is not null &&
                !string.Equals(
                    entity.VotedFor,
                    votedFor,
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Node '{nodeId}' has already voted for " +
                    $"'{entity.VotedFor}' in term " +
                    $"{currentTerm}.");
            }

            entity.CurrentTerm = currentTerm;
            entity.VotedFor = votedFor;
        }

        dbContext.SaveChanges();
    }
}