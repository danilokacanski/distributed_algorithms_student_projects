using System.Net;
using System.Net.Http.Json;
using Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Worker;
using Worker.Execution;

namespace Coordinator.Tests.WorkerIntegration;

public sealed class WorkerServiceIntegrationTests
{
    [Fact]
    public async Task WorkerService_SuccessfulExecutor_CompletesExperiment()
    {
        const string workerId = "worker-service-success-test";
        const string resultMessage =
            "Executor completed successfully.";

        const string metricsJson =
            """{"reward":42.5,"episodes":10}""";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var coordinatorClient =
            factory.CreateClient();

        using var workerClient =
            factory.CreateClient();

        var assignedExperiment =
            await CreateAssignedExperimentAsync(
                coordinatorClient,
                workerId,
                "Worker successful execution test",
                timeoutSeconds: 30);

        var executor = new SuccessfulExperimentExecutor(
            resultMessage,
            metricsJson,
            executionDurationMs: 321);

        using var workerService = CreateWorkerService(
            workerId,
            workerClient,
            executor);

        await workerService.StartAsync(
            CancellationToken.None);

        try
        {
            var completedExperiment =
                await WaitForStatusAsync(
                    coordinatorClient,
                    assignedExperiment.Id,
                    ExperimentStatus.Completed,
                    TimeSpan.FromSeconds(10));

            Assert.Equal(
                resultMessage,
                completedExperiment.ResultMessage);

            Assert.Equal(
                metricsJson,
                completedExperiment.MetricsJson);

            Assert.Equal(
                321,
                completedExperiment.ExecutionDurationMs);

            Assert.Equal(
                workerId,
                completedExperiment.AssignedWorkerId);

            Assert.Equal(
                assignedExperiment.Attempt,
                completedExperiment.Attempt);

            Assert.NotNull(
                completedExperiment.FinishedAtUtc);

            var events =
                await coordinatorClient.GetFromJsonAsync<
                    ExperimentEventResponse[]>(
                    $"/api/experiments/" +
                    $"{assignedExperiment.Id}/events");

            Assert.NotNull(events);

            Assert.Equal(
                new[] { "Created", "Assigned", "Completed" },
                events.Select(experimentEvent =>
                    experimentEvent.Type).ToArray());
        }
        finally
        {
            await workerService.StopAsync(
                CancellationToken.None);
        }
    }

    [Fact]
    public async Task WorkerService_ExecutionTimeout_FailsExperiment()
    {
        const string workerId = "worker-service-timeout-test";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var coordinatorClient =
            factory.CreateClient();

        using var workerClient =
            factory.CreateClient();

        var assignedExperiment =
            await CreateAssignedExperimentAsync(
                coordinatorClient,
                workerId,
                "Worker timeout test",
                timeoutSeconds: 1);

        var executor =
            new BlockingExperimentExecutor();

        using var workerService = CreateWorkerService(
            workerId,
            workerClient,
            executor);

        await workerService.StartAsync(
            CancellationToken.None);

        try
        {
            await executor.Started.Task.WaitAsync(
                TimeSpan.FromSeconds(5));

            var failedExperiment =
                await WaitForStatusAsync(
                    coordinatorClient,
                    assignedExperiment.Id,
                    ExperimentStatus.Failed,
                    TimeSpan.FromSeconds(10));

            Assert.Contains(
                "Execution timed out after 1 second(s).",
                failedExperiment.ResultMessage);

            Assert.Null(failedExperiment.MetricsJson);

            Assert.NotNull(
                failedExperiment.ExecutionDurationMs);

            Assert.True(
                failedExperiment.ExecutionDurationMs > 0);

            Assert.NotNull(
                failedExperiment.FinishedAtUtc);

            Assert.False(
                failedExperiment.CancellationRequested);

            var events =
                await coordinatorClient.GetFromJsonAsync<
                    ExperimentEventResponse[]>(
                    $"/api/experiments/" +
                    $"{assignedExperiment.Id}/events");

            Assert.NotNull(events);

            Assert.Equal(
                new[] { "Created", "Assigned", "Failed" },
                events.Select(experimentEvent =>
                    experimentEvent.Type).ToArray());
        }
        finally
        {
            await workerService.StopAsync(
                CancellationToken.None);
        }
    }

    [Fact]
    public async Task WorkerService_CancellationRequested_CancelsExperiment()
    {
        const string workerId =
            "worker-service-cancellation-test";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var coordinatorClient =
            factory.CreateClient();

        using var workerClient =
            factory.CreateClient();

        var assignedExperiment =
            await CreateAssignedExperimentAsync(
                coordinatorClient,
                workerId,
                "Worker cancellation test",
                timeoutSeconds: 30);

        var executor =
            new BlockingExperimentExecutor();

        using var workerService = CreateWorkerService(
            workerId,
            workerClient,
            executor);

        await workerService.StartAsync(
            CancellationToken.None);

        try
        {
            await executor.Started.Task.WaitAsync(
                TimeSpan.FromSeconds(5));

            var cancellationResponse =
                await coordinatorClient.PostAsync(
                    $"/api/experiments/" +
                    $"{assignedExperiment.Id}/cancel",
                    content: null);

            Assert.Equal(
                HttpStatusCode.OK,
                cancellationResponse.StatusCode);

            var cancelledExperiment =
                await WaitForStatusAsync(
                    coordinatorClient,
                    assignedExperiment.Id,
                    ExperimentStatus.Cancelled,
                    TimeSpan.FromSeconds(10));

            Assert.True(
                cancelledExperiment.CancellationRequested);

            Assert.Equal(
                "Experiment was cancelled by request.",
                cancelledExperiment.ResultMessage);

            Assert.Null(cancelledExperiment.MetricsJson);

            Assert.NotNull(
                cancelledExperiment.ExecutionDurationMs);

            Assert.True(
                cancelledExperiment.ExecutionDurationMs > 0);

            Assert.NotNull(
                cancelledExperiment.FinishedAtUtc);

            var events =
                await coordinatorClient.GetFromJsonAsync<
                    ExperimentEventResponse[]>(
                    $"/api/experiments/" +
                    $"{assignedExperiment.Id}/events");

            Assert.NotNull(events);

            Assert.Equal(
                new[]
                {
                    "Created",
                    "Assigned",
                    "CancelRequested",
                    "Cancelled"
                },
                events.Select(experimentEvent =>
                    experimentEvent.Type).ToArray());
        }
        finally
        {
            await workerService.StopAsync(
                CancellationToken.None);
        }
    }

    [Fact]
    public async Task WorkerService_ExecutorProgress_PersistsLatestSnapshot()
    {
        const string workerId =
            "worker-service-progress-test";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var coordinatorClient =
            factory.CreateClient();

        using var workerClient =
            factory.CreateClient();

        var assignedExperiment =
            await CreateAssignedExperimentAsync(
                coordinatorClient,
                workerId,
                "Worker progress integration test",
                timeoutSeconds: 30);

        var executor =
            new ProgressReportingExperimentExecutor();

        using var workerService = CreateWorkerService(
            workerId,
            workerClient,
            executor);

        await workerService.StartAsync(
            CancellationToken.None);

        try
        {
            var completedExperiment =
                await WaitForStatusAsync(
                    coordinatorClient,
                    assignedExperiment.Id,
                    ExperimentStatus.Completed,
                    TimeSpan.FromSeconds(10));

            Assert.Equal(
                7_500,
                completedExperiment.CurrentStep);

            Assert.Equal(
                """{"meanReward":35.5,"episodes":8}""",
                completedExperiment.ProgressMetricsJson);

            Assert.NotNull(
                completedExperiment.LastProgressAtUtc);

            var events =
                await coordinatorClient.GetFromJsonAsync<
                    ExperimentEventResponse[]>(
                    $"/api/experiments/" +
                    $"{assignedExperiment.Id}/events");

            Assert.NotNull(events);

            Assert.Equal(
                new[] { "Created", "Assigned", "Completed" },
                events.Select(experimentEvent =>
                    experimentEvent.Type).ToArray());
        }
        finally
        {
            await workerService.StopAsync(
                CancellationToken.None);
        }
    }

    private static async Task<ExperimentResponse>
        CreateAssignedExperimentAsync(
            HttpClient client,
            string workerId,
            string experimentName,
            int timeoutSeconds)
    {
        var registrationResponse =
            await client.PostAsJsonAsync(
                "/api/workers/register",
                new WorkerRegistrationRequest
                {
                    WorkerId = workerId
                });

        Assert.Equal(
            HttpStatusCode.OK,
            registrationResponse.StatusCode);

        var createResponse =
            await client.PostAsJsonAsync(
                "/api/experiments",
                new CreateExperimentRequest
                {
                    Name = experimentName,
                    Algorithm = "PPO",
                    Environment = "CartPole-v1",
                    Seed = 42,
                    MaxSteps = 10_000,
                    Priority = 5,
                    TimeoutSeconds = timeoutSeconds,
                    SimulateFailure = false
                });

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createdExperiment =
            await createResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(createdExperiment);

        var assignmentResponse =
            await client.PostAsync(
                $"/api/experiments/" +
                $"{createdExperiment.Id}/assign",
                content: null);

        Assert.Equal(
            HttpStatusCode.OK,
            assignmentResponse.StatusCode);

        var assignedExperiment =
            await assignmentResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(assignedExperiment);

        Assert.Equal(
            ExperimentStatus.Running,
            assignedExperiment.Status);

        Assert.Equal(
            workerId,
            assignedExperiment.AssignedWorkerId);

        Assert.Equal(
            1,
            assignedExperiment.Attempt);

        return assignedExperiment;
    }

    private static WorkerService CreateWorkerService(
        string workerId,
        HttpClient workerClient,
        IExperimentExecutor executor)
    {
        var configuration =
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                    new Dictionary<string, string?>
                    {
                        ["Worker:Id"] = workerId,
                        ["Worker:CoordinatorBaseUrl"] =
                            "http://localhost"
                    })
                .Build();

        return new WorkerService(
            NullLogger<WorkerService>.Instance,
            new FixedHttpClientFactory(workerClient),
            configuration,
            executor);
    }

    private static async Task<ExperimentResponse>
        WaitForStatusAsync(
            HttpClient client,
            Guid experimentId,
            ExperimentStatus expectedStatus,
            TimeSpan timeout)
    {
        var deadline =
            DateTimeOffset.UtcNow.Add(timeout);

        ExperimentResponse? lastExperiment = null;

        while (DateTimeOffset.UtcNow < deadline)
        {
            lastExperiment =
                await client.GetFromJsonAsync<ExperimentResponse>(
                    $"/api/experiments/{experimentId}");

            if (lastExperiment?.Status == expectedStatus)
            {
                return lastExperiment;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException(
            $"Experiment {experimentId} did not reach " +
            $"{expectedStatus}. Last status: " +
            $"{lastExperiment?.Status}.");
    }

    private sealed class FixedHttpClientFactory(
        HttpClient client)
        : IHttpClientFactory
    {
        public HttpClient CreateClient(string name)
        {
            return client;
        }
    }

    private sealed class SuccessfulExperimentExecutor(
        string resultMessage,
        string metricsJson,
        long executionDurationMs)
        : IExperimentExecutor
    {
        public Task<ExperimentExecutionResult> ExecuteAsync(
            ExperimentResponse experiment,
            Func<
                ExperimentProgressUpdate,
                CancellationToken,
                Task> reportProgressAsync,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new ExperimentExecutionResult(
                    true,
                    resultMessage,
                    metricsJson,
                    executionDurationMs));
        }
    }

    private sealed class BlockingExperimentExecutor
        : IExperimentExecutor
    {
        public TaskCompletionSource<bool> Started { get; } =
            new(
                TaskCreationOptions
                    .RunContinuationsAsynchronously);

        public async Task<ExperimentExecutionResult> ExecuteAsync(
            ExperimentResponse experiment,
            Func<
                ExperimentProgressUpdate,
                CancellationToken,
                Task> reportProgressAsync,
            CancellationToken cancellationToken)
        {
            Started.TrySetResult(true);

            await Task.Delay(
                Timeout.InfiniteTimeSpan,
                cancellationToken);

            throw new InvalidOperationException(
                "The blocking executor should have been cancelled.");
        }
    }

        private sealed class ProgressReportingExperimentExecutor
        : IExperimentExecutor
    {
        public async Task<ExperimentExecutionResult> ExecuteAsync(
            ExperimentResponse experiment,
            Func<
                ExperimentProgressUpdate,
                CancellationToken,
                Task> reportProgressAsync,
            CancellationToken cancellationToken)
        {
            await reportProgressAsync(
                new ExperimentProgressUpdate(
                    2_500,
                    """{"meanReward":15.0,"episodes":4}"""),
                cancellationToken);

            await reportProgressAsync(
                new ExperimentProgressUpdate(
                    7_500,
                    """{"meanReward":35.5,"episodes":8}"""),
                cancellationToken);

            return new ExperimentExecutionResult(
                true,
                "Progress reporting completed.",
                """{"totalReward":100}""",
                250);
        }
    }
}