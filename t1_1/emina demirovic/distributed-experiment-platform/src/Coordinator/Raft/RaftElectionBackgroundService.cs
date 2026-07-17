using Microsoft.Extensions.Options;

namespace Coordinator.Raft;

public sealed class RaftElectionBackgroundService(
    RaftNodeState nodeState,
    RaftElectionService electionService,
    IOptions<RaftOptions> options,
    ILogger<RaftElectionBackgroundService> logger)
    : BackgroundService
{
    private readonly RaftOptions _options =
        options.Value;

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        if (!_options.AutomaticElectionEnabled)
        {
            logger.LogInformation(
                "Automatic Raft elections are disabled.");

            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var timeoutMilliseconds =
                Random.Shared.Next(
                    _options.ElectionTimeoutMinMilliseconds,
                    _options.ElectionTimeoutMaxMilliseconds + 1);

            var timeout =
                TimeSpan.FromMilliseconds(
                    timeoutMilliseconds);

            try
            {
                await Task.Delay(
                    timeout,
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            if (!nodeState.HasElectionTimedOut(timeout))
            {
                continue;
            }

            try
            {
                var result =
                    await electionService.StartElectionAsync(
                        stoppingToken);

                logger.LogInformation(
                    "Raft election completed. Term: {Term}, " +
                    "Role: {Role}, Votes: {Votes}/{Quorum}.",
                    result.CurrentTerm,
                    result.Role,
                    result.VotesGranted,
                    result.QuorumSize);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(
                    exception,
                    "Automatic Raft election failed.");
            }
        }
    }
}