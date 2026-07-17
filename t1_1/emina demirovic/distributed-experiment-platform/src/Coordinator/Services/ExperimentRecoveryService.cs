using Contracts;
using Coordinator.Commands;
using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Services;

public sealed class ExperimentRecoveryService(
    ILogger<ExperimentRecoveryService> logger,
    WorkerRegistry workerRegistry,
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
        while (!stoppingToken.IsCancellationRequested)
        {
            await RunRecoveryCycleAsync(stoppingToken);

            await DelayAsync(stoppingToken);
        }
    }

    public async Task RunRecoveryCycleAsync(
        CancellationToken stoppingToken)
    {
        if (raftOptions.Value.ClientCommandReplicationEnabled &&
            raftNodeState.GetSnapshot().Role != RaftNodeRole.Leader)
        {
            return;
        }

        var runningExperiments =
            experimentRegistry.GetRunning();

        foreach (var experiment in runningExperiments)
        {
            var workerId =
                experiment.AssignedWorkerId;

            if (string.IsNullOrWhiteSpace(workerId))
            {
                continue;
            }

            if (workerRegistry.IsOnline(workerId))
            {
                continue;
            }

            var command =
                new RequeueExperimentCommand(
                    CommandId: Guid.NewGuid(),
                    OccurredAtUtc:
                        DateTimeOffset.UtcNow,
                    ExperimentId: experiment.Id,
                    EventId: Guid.NewGuid(),
                    WorkerId: workerId,
                    Attempt: experiment.Attempt,
                    CancelInsteadOfRequeue:
                        experiment.CancellationRequested);

            if (!raftOptions.Value.ClientCommandReplicationEnabled)
            {
                TryRecoverLocally(
                    command,
                    experiment.Id,
                    workerId);

                continue;
            }

            var submission =
                await raftCommandSubmitter.SubmitAsync(
                    command,
                    stoppingToken);

            if (submission.Status ==
                RaftCommandSubmissionStatus.Committed)
            {
                LogRecoveryResult(
                    experiment.Id,
                    workerId);

                continue;
            }

            if (submission.Status ==
                RaftCommandSubmissionStatus.NotLeader)
            {
                logger.LogDebug(
                    "Recovery skipped because this Coordinator " +
                    "is not the Raft leader. LeaderId: {LeaderId}.",
                    submission.LeaderId);

                return;
            }

            logger.LogDebug(
                "Recovery of experiment {ExperimentId} was not " +
                "committed before the timeout.",
                experiment.Id);
        }
    }

    private void TryRecoverLocally(
        RequeueExperimentCommand command,
        Guid experimentId,
        string workerId)
    {
        try
        {
            var result =
                commandProcessor.Apply(command);

            LogRecoveryResult(
                result.Value.Id,
                workerId);
        }
        catch (InvalidOperationException exception)
        {
            logger.LogDebug(
                exception,
                "Recovery of experiment {ExperimentId} " +
                "was skipped.",
                experimentId);
        }
    }

    private void LogRecoveryResult(
        Guid experimentId,
        string workerId)
    {
        var experiment =
            experimentRegistry.GetById(experimentId);

        if (experiment is null)
        {
            return;
        }

        if (experiment.Status ==
            ExperimentStatus.Cancelled)
        {
            logger.LogWarning(
                "Experiment {ExperimentId} was cancelled " +
                "because worker {WorkerId} is offline.",
                experimentId,
                workerId);

            return;
        }

        logger.LogWarning(
            "Experiment {ExperimentId} returned to Pending " +
            "because worker {WorkerId} is offline.",
            experimentId,
            workerId);
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