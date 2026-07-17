using Microsoft.Extensions.Options;

namespace Coordinator.Raft;

public sealed class RaftHeartbeatBackgroundService(
    RaftHeartbeatSender heartbeatSender,
    IOptions<RaftOptions> options,
    ILogger<RaftHeartbeatBackgroundService> logger)
    : BackgroundService
{
    private readonly TimeSpan _interval =
        TimeSpan.FromMilliseconds(
            options.Value
                .HeartbeatIntervalMilliseconds);

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        using var timer =
            new PeriodicTimer(_interval);

        try
        {
            while (await timer.WaitForNextTickAsync(
                stoppingToken))
            {
                try
                {
                    await heartbeatSender
                        .SendHeartbeatAsync(
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
                        "Periodic Raft heartbeat failed.");
                }
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken
                .IsCancellationRequested)
        {
            // Normal application shutdown.
        }
    }
}