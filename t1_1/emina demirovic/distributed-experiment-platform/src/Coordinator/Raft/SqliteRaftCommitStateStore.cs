using Coordinator.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Coordinator.Raft;

public sealed class SqliteRaftCommitStateStore(
    IDbContextFactory<CoordinatorDbContext> dbContextFactory,
    IOptions<RaftOptions> options)
    : IRaftCommitStateStore
{
    private readonly object _sync = new();

    private readonly string _nodeId =
        options.Value.NodeId;

    public RaftCommitState LoadOrCreate()
    {
        lock (_sync)
        {
            using var dbContext =
                dbContextFactory.CreateDbContext();

            var entity =
                dbContext.RaftCommitStates
                    .SingleOrDefault(state =>
                        state.NodeId == _nodeId);

            if (entity is null)
            {
                entity = new RaftCommitStateEntity
                {
                    NodeId = _nodeId,
                    CommitIndex = 0,
                    LastApplied = 0
                };

                dbContext.RaftCommitStates.Add(entity);
                dbContext.SaveChanges();
            }

            return new RaftCommitState(
                entity.CommitIndex,
                entity.LastApplied);
        }
    }

    public void Save(
        long commitIndex,
        long lastApplied)
    {
        if (commitIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(commitIndex));
        }

        if (lastApplied < 0 ||
            lastApplied > commitIndex)
        {
            throw new ArgumentOutOfRangeException(
                nameof(lastApplied));
        }

        lock (_sync)
        {
            using var dbContext =
                dbContextFactory.CreateDbContext();

            var entity =
                dbContext.RaftCommitStates
                    .SingleOrDefault(state =>
                        state.NodeId == _nodeId);

            if (entity is null)
            {
                entity = new RaftCommitStateEntity
                {
                    NodeId = _nodeId,
                    CommitIndex = commitIndex,
                    LastApplied = lastApplied
                };

                dbContext.RaftCommitStates.Add(entity);
            }
            else
            {
                if (commitIndex <
                    entity.CommitIndex)
                {
                    throw new InvalidOperationException(
                        "Raft CommitIndex cannot move backwards.");
                }

                if (lastApplied <
                    entity.LastApplied)
                {
                    throw new InvalidOperationException(
                        "Raft LastApplied cannot move backwards.");
                }

                entity.CommitIndex = commitIndex;
                entity.LastApplied = lastApplied;
            }

            dbContext.SaveChanges();
        }
    }
}