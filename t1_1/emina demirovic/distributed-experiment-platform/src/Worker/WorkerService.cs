using System.Net;
using System.Diagnostics;
using System.Net.Http.Json;
using Contracts;
using Worker.Execution;
namespace Worker;

public sealed class WorkerService(
    ILogger<WorkerService> logger,
    IHttpClientFactory httpClientFactory,
    IConfiguration configuration,
    IExperimentExecutor experimentExecutor
    ) : BackgroundService
{
    private readonly string WorkerId =
        configuration["Worker:Id"]
        ?? throw new InvalidOperationException(
            "Worker ID is not configured.");

    private readonly string CoordinatorBaseUrl =
        configuration["Worker:CoordinatorBaseUrl"]
        ?? throw new InvalidOperationException(
            "Coordinator URL is not configured.");

    private static readonly TimeSpan HeartbeatInterval =
        TimeSpan.FromSeconds(5);

    private static readonly TimeSpan ExperimentPollingInterval =
        TimeSpan.FromSeconds(2);

    private static readonly TimeSpan RegistrationRetryInterval =
        TimeSpan.FromSeconds(5);


    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var client = httpClientFactory.CreateClient();

        var registered = await WaitForRegistrationAsync(
            client,
            stoppingToken);

        if (!registered)
        {
            return;
        }

        await Task.WhenAll(
            RunHeartbeatLoopAsync(client, stoppingToken),
            RunExecutionLoopAsync(client, stoppingToken));
    }

    private async Task<bool> RegisterAsync(
        HttpClient client,
        CancellationToken stoppingToken)
    {
        var request = new WorkerRegistrationRequest
        {
            WorkerId = WorkerId
        };

        try
        {
            var response = await client.PostAsJsonAsync(
                $"{CoordinatorBaseUrl}/api/workers/register",
                request,
                stoppingToken);

            response.EnsureSuccessStatusCode();

            logger.LogInformation(
                "Worker {WorkerId} successfully registered.",
                WorkerId);

            return true;
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            return false;
        }
        catch (Exception exception)
        {
            logger.LogError(
                exception,
                "Worker registration failed.");

            return false;
        }
    }

    private async Task<bool> WaitForRegistrationAsync(
        HttpClient client,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (await RegisterAsync(client, stoppingToken))
            {
                return true;
            }

            logger.LogWarning(
                "Worker {WorkerId} will retry registration.",
                WorkerId);

            if (!await WaitAsync(
                    RegistrationRetryInterval,
                    stoppingToken))
            {
                return false;
            }
        }

        return false;
    }

    private async Task RunHeartbeatLoopAsync(
        HttpClient client,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var response = await client.PostAsync(
                    $"{CoordinatorBaseUrl}/api/workers/{WorkerId}/heartbeat",
                    content: null,
                    stoppingToken);

                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    logger.LogWarning(
                        "Worker {WorkerId} is no longer registered. Registering again.",
                        WorkerId);

                    var registered = await WaitForRegistrationAsync(
                        client,
                        stoppingToken);

                    if (!registered)
                    {
                        break;
                    }
                }
                else
                {
                    response.EnsureSuccessStatusCode();

                    logger.LogDebug(
                        "Heartbeat sent by {WorkerId}.",
                        WorkerId);
                }
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogWarning(
                    exception,
                    "Heartbeat failed for {WorkerId}.",
                    WorkerId);
            }

            if (!await WaitAsync(
                    HeartbeatInterval,
                    stoppingToken))
            {
                break;
            }
        }
    }

    private async Task RunExecutionLoopAsync(
        HttpClient client,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var response = await client.GetAsync(
                    $"{CoordinatorBaseUrl}/api/experiments/worker/{WorkerId}/next",
                    stoppingToken);

                if (response.StatusCode == HttpStatusCode.NoContent)
                {
                    if (!await WaitAsync(
                            ExperimentPollingInterval,
                            stoppingToken))
                    {
                        break;
                    }

                    continue;
                }

                response.EnsureSuccessStatusCode();

                var experiment =
                    await response.Content
                        .ReadFromJsonAsync<ExperimentResponse>(
                            cancellationToken: stoppingToken);

                if (experiment is not null)
                {
                    await ExecuteExperimentAsync(
                        client,
                        experiment,
                        stoppingToken);
                }
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
                    "Experiment processing failed.");
            }

            if (!await WaitAsync(
                    ExperimentPollingInterval,
                    stoppingToken))
            {
                break;
            }
        }
    }

    private async Task ExecuteExperimentAsync(
        HttpClient client,
        ExperimentResponse experiment,
        CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "Starting experiment {ExperimentId}: {ExperimentName}. " +
            "Algorithm: {Algorithm}, Environment: {Environment}, " +
            "Seed: {Seed}, MaxSteps: {MaxSteps}, Priority: {Priority}.",
            experiment.Id,
            experiment.Name,
            experiment.Algorithm,
            experiment.Environment,
            experiment.Seed,
            experiment.MaxSteps,
            experiment.Priority);

        using var manualCancellation =
            new CancellationTokenSource();

        using var monitorCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken);

        using var executionCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken,
                manualCancellation.Token);

        executionCancellation.CancelAfter(
            TimeSpan.FromSeconds(
                experiment.TimeoutSeconds));

        var cancellationMonitorTask =
            MonitorCancellationAsync(
                client,
                experiment.Id,
                manualCancellation,
                monitorCancellation.Token);

        var stopwatch = Stopwatch.StartNew();

        ExperimentExecutionResult executionResult;
        var wasCancelled = false;

        try
        {
            executionResult =
                await experimentExecutor.ExecuteAsync(
                    experiment,
                    (progress, cancellationToken) =>
                        ReportProgressAsync(
                            client,
                            experiment,
                            progress,
                            cancellationToken),
                    executionCancellation.Token);

            stopwatch.Stop();
        }
        catch (OperationCanceledException)
            when (!stoppingToken.IsCancellationRequested &&
                manualCancellation.IsCancellationRequested)
        {
            stopwatch.Stop();

            wasCancelled = true;

            executionResult = new ExperimentExecutionResult(
                false,
                "Experiment was cancelled by request.",
                null,
                stopwatch.ElapsedMilliseconds);
        }
        catch (OperationCanceledException)
            when (!stoppingToken.IsCancellationRequested &&
                executionCancellation.IsCancellationRequested)
        {
            stopwatch.Stop();

            executionResult = new ExperimentExecutionResult(
                false,
                $"Execution timed out after " +
                $"{experiment.TimeoutSeconds} second(s).",
                null,
                stopwatch.ElapsedMilliseconds);
        }
        finally
        {
            monitorCancellation.Cancel();

            try
            {
                await cancellationMonitorTask;
            }
            catch (OperationCanceledException)
                when (monitorCancellation.IsCancellationRequested)
            {
                // Očekivano gašenje monitora.
            }
        }

        var completionRequest = new CompleteExperimentRequest
        {
            WorkerId = WorkerId,
            Attempt = experiment.Attempt,
            Succeeded = executionResult.Succeeded,
            WasCancelled = wasCancelled,
            ResultMessage = executionResult.ResultMessage,
            MetricsJson = executionResult.MetricsJson,
            ExecutionDurationMs =
                executionResult.ExecutionDurationMs
        };

        var response = await client.PostAsJsonAsync(
            $"{CoordinatorBaseUrl}/api/experiments/" +
            $"{experiment.Id}/complete",
            completionRequest,
            stoppingToken);

        response.EnsureSuccessStatusCode();

        if (wasCancelled)
        {
            logger.LogWarning(
                "Experiment {ExperimentId} was cancelled.",
                experiment.Id);
        }
        else if (executionResult.Succeeded)
        {
            logger.LogInformation(
                "Experiment {ExperimentId} completed successfully.",
                experiment.Id);
        }
        else
        {
            logger.LogWarning(
                "Experiment {ExperimentId} failed.",
                experiment.Id);
        }
    }

    private static async Task<bool> WaitAsync(
        TimeSpan delay,
        CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(delay, stoppingToken);
            return true;
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            return false;
        }
    }

    private async Task ReportProgressAsync(
        HttpClient client,
        ExperimentResponse experiment,
        ExperimentProgressUpdate progress,
        CancellationToken cancellationToken)
    {
        var request = new ReportExperimentProgressRequest
        {
            WorkerId = WorkerId,
            Attempt = experiment.Attempt,
            CurrentStep = progress.CurrentStep,
            ProgressMetricsJson = progress.MetricsJson
        };

        try
        {
            var response = await client.PostAsJsonAsync(
                $"{CoordinatorBaseUrl}/api/experiments/" +
                $"{experiment.Id}/progress",
                request,
                cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                logger.LogWarning(
                    "Progress update for experiment {ExperimentId} " +
                    "was rejected with status {StatusCode}.",
                    experiment.Id,
                    response.StatusCode);

                return;
            }

            logger.LogDebug(
                "Progress reported for experiment {ExperimentId}: " +
                "{CurrentStep}/{MaxSteps}.",
                experiment.Id,
                progress.CurrentStep,
                experiment.MaxSteps);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Progress update failed for experiment {ExperimentId}.",
                experiment.Id);
        }
    }

    private async Task MonitorCancellationAsync(
        HttpClient client,
        Guid experimentId,
        CancellationTokenSource manualCancellation,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested &&
            !manualCancellation.IsCancellationRequested)
        {
            try
            {
                var response = await client.GetAsync(
                    $"{CoordinatorBaseUrl}/api/experiments/{experimentId}",
                    cancellationToken);

                response.EnsureSuccessStatusCode();

                var experiment =
                    await response.Content
                        .ReadFromJsonAsync<ExperimentResponse>(
                            cancellationToken:
                                cancellationToken);

                if (experiment?.CancellationRequested == true)
                {
                    logger.LogWarning(
                        "Cancellation requested for experiment {ExperimentId}.",
                        experimentId);

                    manualCancellation.Cancel();
                    return;
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogWarning(
                    exception,
                    "Could not check cancellation state for experiment {ExperimentId}.",
                    experimentId);
            }

            if (!await WaitAsync(
                    TimeSpan.FromSeconds(1),
                    cancellationToken))
            {
                return;
            }
        }
    }

}