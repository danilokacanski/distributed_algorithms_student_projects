using Contracts;
using Coordinator.Commands;
using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Services;

public sealed class ExperimentStartupRecoveryService(
    ILogger<ExperimentStartupRecoveryService> logger,
    ExperimentRegistry experimentRegistry,
    CoordinatorCommandProcessor commandProcessor,
    RaftCommandSubmitter raftCommandSubmitter,
    RaftNodeState raftNodeState,
    IOptions<RaftOptions> raftOptions)
    : BackgroundService
{
    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var startupExperimentIds =
            experimentRegistry
                .GetRunning()
                .Select(experiment => experiment.Id)
                .ToArray();

        while (!stoppingToken.IsCancellationRequested)
        {
            if (startupExperimentIds.Length == 0)
            {
                return;
            }

            if (raftOptions.Value.ClientCommandReplicationEnabled &&
                raftNodeState.GetSnapshot().Role !=
                RaftNodeRole.Leader)
            {
                await DelayAsync(stoppingToken);
                continue;
            }

            await RecoverStartupExperimentsAsync(
                startupExperimentIds,
                stoppingToken);

            return;
        }
    }

    public async Task<int> RunStartupRecoveryCycleAsync(
        CancellationToken stoppingToken)
    {
        var startupExperimentIds =
            experimentRegistry
                .GetRunning()
                .Select(experiment => experiment.Id)
                .ToArray();

        return await RecoverStartupExperimentsAsync(
            startupExperimentIds,
            stoppingToken);
    }

    private async Task<int> RecoverStartupExperimentsAsync(
        IReadOnlyCollection<Guid> startupExperimentIds,
        CancellationToken stoppingToken)
    {
        if (raftOptions.Value.ClientCommandReplicationEnabled &&
            raftNodeState.GetSnapshot().Role != RaftNodeRole.Leader)
        {
            return 0;
        }

        var recoveredExperimentCount = 0;

        foreach (var experimentId in startupExperimentIds)
        {
            var experiment =
                experimentRegistry.GetById(experimentId);

            if (experiment is null ||
                experiment.Status != ExperimentStatus.Running)
            {
                continue;
            }

            var command =
                new RecoverExperimentOnStartupCommand(
                    CommandId: Guid.NewGuid(),
                    OccurredAtUtc: DateTimeOffset.UtcNow,
                    ExperimentId: experiment.Id,
                    EventId: Guid.NewGuid(),
                    PreviousWorkerId:
                        experiment.AssignedWorkerId,
                    Attempt: experiment.Attempt);

            if (!raftOptions.Value.ClientCommandReplicationEnabled)
            {
                if (TryRecoverLocally(command, experiment.Id))
                {
                    recoveredExperimentCount++;
                }

                continue;
            }

            var submission =
                await raftCommandSubmitter.SubmitAsync(
                    command,
                    stoppingToken);

            if (submission.Status ==
                RaftCommandSubmissionStatus.Committed)
            {
                recoveredExperimentCount++;
                continue;
            }

            if (submission.Status ==
                RaftCommandSubmissionStatus.NotLeader)
            {
                logger.LogDebug(
                    "Startup recovery skipped because this " +
                    "Coordinator is not the Raft leader. " +
                    "LeaderId: {LeaderId}.",
                    submission.LeaderId);

                return recoveredExperimentCount;
            }

            logger.LogWarning(
                "Startup recovery of experiment {ExperimentId} " +
                "was not committed before the timeout.",
                experiment.Id);
        }

        if (recoveredExperimentCount > 0)
        {
            logger.LogWarning(
                "{ExperimentCount} interrupted experiment(s) " +
                "returned to Pending during Coordinator startup.",
                recoveredExperimentCount);
        }

        return recoveredExperimentCount;
    }

    private bool TryRecoverLocally(
        RecoverExperimentOnStartupCommand command,
        Guid experimentId)
    {
        try
        {
            commandProcessor.Apply(command);
            return true;
        }
        catch (InvalidOperationException)
        {
            logger.LogWarning(
                "Experiment {ExperimentId} could not be " +
                "recovered during Coordinator startup.",
                experimentId);

            return false;
        }
    }

    private static async Task DelayAsync(
        CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(
                TimeSpan.FromSeconds(2),
                stoppingToken);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}