namespace Coordinator.Raft;

public sealed class RaftStateMachineBackgroundService(
    RaftStateMachineApplier applier,
    ILogger<RaftStateMachineBackgroundService> logger)
    : BackgroundService
{
    private static readonly TimeSpan Interval =
        TimeSpan.FromMilliseconds(200);

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        using var timer =
            new PeriodicTimer(Interval);

        try
        {
            while (await timer.WaitForNextTickAsync(
                stoppingToken))
            {
                try
                {
                    await applier
                        .ApplyCommittedEntriesAsync(
                            stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken
                        .IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    logger.LogError(
                        exception,
                        "Applying committed Raft entries failed.");
                }
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken
                .IsCancellationRequested)
        {
            // Normal shutdown.
        }
    }
}