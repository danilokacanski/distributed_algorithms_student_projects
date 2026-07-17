using Coordinator.Commands;
using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Services;

public sealed class ExperimentSchedulerService(
    ILogger<ExperimentSchedulerService> logger,
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
            await RunSchedulingCycleAsync(stoppingToken);

            await DelayAsync(stoppingToken);
        }
    }

    public async Task RunSchedulingCycleAsync(
        CancellationToken stoppingToken)
    {
        if (raftOptions.Value.ClientCommandReplicationEnabled &&
            raftNodeState.GetSnapshot().Role != RaftNodeRole.Leader)
        {
            return;
        }

        var onlineWorkers =
            workerRegistry.GetOnlineWorkers();

        foreach (var worker in onlineWorkers)
        {
            if (experimentRegistry.HasRunningExperiment(
                worker.WorkerId))
            {
                continue;
            }

            while (true)
            {
                var pendingExperiment =
                    experimentRegistry.GetNextPending();

                if (pendingExperiment is null)
                {
                    break;
                }

                var command =
                    new AssignExperimentCommand(
                        CommandId: Guid.NewGuid(),
                        OccurredAtUtc:
                            DateTimeOffset.UtcNow,
                        ExperimentId:
                            pendingExperiment.Id,
                        EventId: Guid.NewGuid(),
                        WorkerId: worker.WorkerId,
                        Attempt:
                            pendingExperiment.Attempt + 1);

                if (!raftOptions.Value
                        .ClientCommandReplicationEnabled)
                {
                    if (TryAssignLocally(
                            command,
                            pendingExperiment.Id,
                            worker.WorkerId))
                    {
                        break;
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
                    logger.LogInformation(
                        "Experiment {ExperimentId} " +
                        "automatically assigned to worker " +
                        "{WorkerId} through Raft.",
                        pendingExperiment.Id,
                        worker.WorkerId);

                    break;
                }

                if (submission.Status ==
                    RaftCommandSubmissionStatus.NotLeader)
                {
                    logger.LogDebug(
                        "Automatic assignment skipped because " +
                        "this Coordinator is not the Raft leader. " +
                        "LeaderId: {LeaderId}.",
                        submission.LeaderId);

                    break;
                }

                logger.LogDebug(
                    "Automatic assignment of experiment " +
                    "{ExperimentId} was not committed before " +
                    "the timeout.",
                    pendingExperiment.Id);

                break;
            }
        }
    }

    private bool TryAssignLocally(
        AssignExperimentCommand command,
        Guid experimentId,
        string workerId)
    {
        try
        {
            var result =
                commandProcessor.Apply(command);

            logger.LogInformation(
                "Experiment {ExperimentId} " +
                "automatically assigned to worker " +
                "{WorkerId}.",
                result.Value.Id,
                workerId);

            return true;
        }
        catch (InvalidOperationException exception)
        {
            logger.LogDebug(
                exception,
                "Automatic assignment of experiment " +
                "{ExperimentId} was skipped.",
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