using Contracts;
using Coordinator.Commands;
using Coordinator.Data;
using Coordinator.Raft;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Raft;

public sealed class
    RaftStateMachineApplicationTests
{
    [Fact]
    public async Task
        AppliesCommittedCreateExperimentCommand()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var experimentId =
            Guid.NewGuid();

        var command =
            CreateExperimentCommand(
                experimentId);

        var logManager =
            factory.Services
                .GetRequiredService<
                    RaftLogManager>();

        var commitManager =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>();

        var applier =
            factory.Services
                .GetRequiredService<
                    RaftStateMachineApplier>();

        logManager.AppendCommand(
            command,
            term: 1);

        commitManager.TryAdvanceLeaderCommit(
            currentTerm: 1,
            clusterSize: 3,
            followerMatchIndexes:
                [1, 0]);

        var result =
            await applier
                .ApplyCommittedEntriesAsync();

        Assert.Equal(1, result.AppliedCount);
        Assert.Equal(1, result.LastApplied);

        var dbContextFactory =
            factory.Services
                .GetRequiredService<
                    IDbContextFactory<
                        CoordinatorDbContext>>();

        using var dbContext =
            dbContextFactory.CreateDbContext();

        var experiment =
            dbContext.Experiments
                .AsNoTracking()
                .Single(existing =>
                    existing.Id == experimentId);

        Assert.Equal(
            ExperimentStatus.Pending,
            experiment.Status);

        Assert.Equal(
            "Raft applied experiment",
            experiment.Name);

        Assert.Equal(
            new RaftCommitState(
                CommitIndex: 1,
                LastApplied: 1),
            commitManager.GetState());
    }

    [Fact]
    public async Task
        RepeatedApply_DoesNotApplySameEntryTwice()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var experimentId =
            Guid.NewGuid();

        var command =
            CreateExperimentCommand(
                experimentId);

        var logManager =
            factory.Services
                .GetRequiredService<
                    RaftLogManager>();

        var commitManager =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>();

        var applier =
            factory.Services
                .GetRequiredService<
                    RaftStateMachineApplier>();

        logManager.AppendCommand(
            command,
            term: 1);

        commitManager.TryAdvanceLeaderCommit(
            currentTerm: 1,
            clusterSize: 3,
            followerMatchIndexes:
                [1, 0]);

        var firstResult =
            await applier
                .ApplyCommittedEntriesAsync();

        var secondResult =
            await applier
                .ApplyCommittedEntriesAsync();

        Assert.Equal(1, firstResult.AppliedCount);
        Assert.Equal(0, secondResult.AppliedCount);
        Assert.Equal(1, secondResult.LastApplied);

        var dbContextFactory =
            factory.Services
                .GetRequiredService<
                    IDbContextFactory<
                        CoordinatorDbContext>>();

        using var dbContext =
            dbContextFactory.CreateDbContext();

        var experimentCount =
            dbContext.Experiments
                .Count(experiment =>
                    experiment.Id == experimentId);

        Assert.Equal(1, experimentCount);

        Assert.Equal(
            new RaftCommitState(
                CommitIndex: 1,
                LastApplied: 1),
            commitManager.GetState());
    }

    [Fact]
    public async Task
        AppliesCommittedEntriesInOrder()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var firstExperimentId =
            Guid.NewGuid();

        var secondExperimentId =
            Guid.NewGuid();

        var firstCommand =
            CreateExperimentCommand(
                firstExperimentId,
                "First Raft experiment");

        var secondCommand =
            CreateExperimentCommand(
                secondExperimentId,
                "Second Raft experiment");

        var logManager =
            factory.Services
                .GetRequiredService<
                    RaftLogManager>();

        var commitManager =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>();

        var applier =
            factory.Services
                .GetRequiredService<
                    RaftStateMachineApplier>();

        logManager.AppendCommand(
            firstCommand,
            term: 1);

        logManager.AppendCommand(
            secondCommand,
            term: 1);

        commitManager.TryAdvanceLeaderCommit(
            currentTerm: 1,
            clusterSize: 3,
            followerMatchIndexes:
                [2, 0]);

        var result =
            await applier
                .ApplyCommittedEntriesAsync();

        Assert.Equal(2, result.AppliedCount);
        Assert.Equal(2, result.LastApplied);

        var dbContextFactory =
            factory.Services
                .GetRequiredService<
                    IDbContextFactory<
                        CoordinatorDbContext>>();

        using var dbContext =
            dbContextFactory.CreateDbContext();

        var experimentCount =
            dbContext.Experiments.Count(
                experiment =>
                    experiment.Id == firstExperimentId ||
                    experiment.Id == secondExperimentId);

        Assert.Equal(2, experimentCount);

        Assert.Equal(
            new RaftCommitState(
                CommitIndex: 2,
                LastApplied: 2),
            commitManager.GetState());
    }

    private static CreateExperimentCommand
        CreateExperimentCommand(
            Guid experimentId,
            string name =
                "Raft applied experiment")
    {
        return new CreateExperimentCommand(
            CommandId: Guid.NewGuid(),
            OccurredAtUtc:
                new DateTimeOffset(
                    2026,
                    7,
                    3,
                    12,
                    0,
                    0,
                    TimeSpan.Zero),
            ExperimentId: experimentId,
            EventId: Guid.NewGuid(),
            Name: name,
            Algorithm: "PPO",
            Environment: "CartPole-v1",
            Seed: 42,
            MaxSteps: 10_000,
            Priority: 1,
            TimeoutSeconds: 300,
            SimulateFailure: false);
    }
}