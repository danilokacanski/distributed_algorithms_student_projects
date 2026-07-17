using System.Net;
using System.Net.Http.Json;
using Contracts;

namespace Coordinator.Tests.Experiments;

public sealed class ExperimentLifecycleTests
{
    [Fact]
    public async Task Complete_SuccessfulExecution_PersistsCompletedLifecycle()
    {
        const string workerId = "worker-lifecycle-test";
        const string resultMessage =
            "Experiment completed successfully.";

        const string metricsJson =
            """{"reward":125.5,"episodes":20}""";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client = factory.CreateClient();

        var createRequest = new CreateExperimentRequest
        {
            Name = "Successful lifecycle test",
            Algorithm = "PPO",
            Environment = "CartPole-v1",
            Seed = 42,
            MaxSteps = 10_000,
            Priority = 5,
            TimeoutSeconds = 120,
            SimulateFailure = false
        };

        var createResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            createRequest);

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createdExperiment =
            await createResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(createdExperiment);

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

        var assignResponse = await client.PostAsync(
            $"/api/experiments/{createdExperiment.Id}/assign",
            null);

        Assert.Equal(
            HttpStatusCode.OK,
            assignResponse.StatusCode);

        var assignedExperiment =
            await assignResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(assignedExperiment);

        Assert.Equal(
            ExperimentStatus.Running,
            assignedExperiment.Status);

        Assert.Equal(
            workerId,
            assignedExperiment.AssignedWorkerId);

        Assert.Equal(1, assignedExperiment.Attempt);
        Assert.Null(assignedExperiment.FinishedAtUtc);

        var nextResponse = await client.GetAsync(
            $"/api/experiments/worker/{workerId}/next");

        Assert.Equal(
            HttpStatusCode.OK,
            nextResponse.StatusCode);

        var workerExperiment =
            await nextResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(workerExperiment);
        Assert.Equal(
            createdExperiment.Id,
            workerExperiment.Id);

        var completionRequest =
            new CompleteExperimentRequest
            {
                WorkerId = workerId,
                Attempt = assignedExperiment.Attempt,
                Succeeded = true,
                WasCancelled = false,
                ResultMessage = resultMessage,
                MetricsJson = metricsJson,
                ExecutionDurationMs = 1_250
            };

        var completionResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/{createdExperiment.Id}/complete",
                completionRequest);

        Assert.Equal(
            HttpStatusCode.OK,
            completionResponse.StatusCode);

        var completedExperiment =
            await completionResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(completedExperiment);

        Assert.Equal(
            ExperimentStatus.Completed,
            completedExperiment.Status);

        Assert.Equal(1, completedExperiment.Attempt);
        Assert.Equal(workerId, completedExperiment.AssignedWorkerId);
        Assert.Equal(resultMessage, completedExperiment.ResultMessage);
        Assert.Equal(metricsJson, completedExperiment.MetricsJson);
        Assert.Equal(1_250, completedExperiment.ExecutionDurationMs);
        Assert.NotNull(completedExperiment.FinishedAtUtc);
        Assert.False(completedExperiment.CancellationRequested);

        var persistedExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{createdExperiment.Id}");

        Assert.NotNull(persistedExperiment);

        Assert.Equal(
            ExperimentStatus.Completed,
            persistedExperiment.Status);

        Assert.Equal(
            completedExperiment.FinishedAtUtc,
            persistedExperiment.FinishedAtUtc);

        var events =
            await client.GetFromJsonAsync<ExperimentEventResponse[]>(
                $"/api/experiments/{createdExperiment.Id}/events");

        Assert.NotNull(events);
        Assert.Equal(3, events.Length);

        Assert.Equal(
            new[] { "Created", "Assigned", "Completed" },
            events.Select(experimentEvent =>
                experimentEvent.Type).ToArray());

        Assert.Equal(workerId, events[1].WorkerId);
        Assert.Equal(1, events[1].Attempt);

        Assert.Equal(workerId, events[2].WorkerId);
        Assert.Equal(1, events[2].Attempt);
    }

    [Fact]
    public async Task Complete_FailedExecution_PersistsFailedLifecycle()
    {
        const string workerId = "worker-failed-lifecycle-test";
        const string failureMessage = "Python process exited with code 1.";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client = factory.CreateClient();

        var createResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            new CreateExperimentRequest
            {
                Name = "Failed lifecycle test",
                Algorithm = "DQN",
                Environment = "LunarLander-v3",
                Seed = 15,
                MaxSteps = 5_000,
                Priority = 4,
                TimeoutSeconds = 90,
                SimulateFailure = true
            });

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createdExperiment =
            await createResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(createdExperiment);

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

        var assignResponse = await client.PostAsync(
            $"/api/experiments/{createdExperiment.Id}/assign",
            null);

        Assert.Equal(
            HttpStatusCode.OK,
            assignResponse.StatusCode);

        var assignedExperiment =
            await assignResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(assignedExperiment);
        Assert.Equal(ExperimentStatus.Running, assignedExperiment.Status);
        Assert.Equal(1, assignedExperiment.Attempt);

        var completionResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/{createdExperiment.Id}/complete",
                new CompleteExperimentRequest
                {
                    WorkerId = workerId,
                    Attempt = assignedExperiment.Attempt,
                    Succeeded = false,
                    WasCancelled = false,
                    ResultMessage = failureMessage,
                    MetricsJson = null,
                    ExecutionDurationMs = 850
                });

        Assert.Equal(
            HttpStatusCode.OK,
            completionResponse.StatusCode);

        var failedExperiment =
            await completionResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(failedExperiment);

        Assert.Equal(
            ExperimentStatus.Failed,
            failedExperiment.Status);

        Assert.Equal(workerId, failedExperiment.AssignedWorkerId);
        Assert.Equal(1, failedExperiment.Attempt);
        Assert.Equal(failureMessage, failedExperiment.ResultMessage);
        Assert.Equal(850, failedExperiment.ExecutionDurationMs);
        Assert.Null(failedExperiment.MetricsJson);
        Assert.NotNull(failedExperiment.FinishedAtUtc);
        Assert.False(failedExperiment.CancellationRequested);

        var persistedExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{createdExperiment.Id}");

        Assert.NotNull(persistedExperiment);

        Assert.Equal(
            ExperimentStatus.Failed,
            persistedExperiment.Status);

        Assert.Equal(
            failureMessage,
            persistedExperiment.ResultMessage);

        Assert.Equal(
            failedExperiment.FinishedAtUtc,
            persistedExperiment.FinishedAtUtc);

        var events =
            await client.GetFromJsonAsync<ExperimentEventResponse[]>(
                $"/api/experiments/{createdExperiment.Id}/events");

        Assert.NotNull(events);
        Assert.Equal(3, events.Length);

        Assert.Equal(
            new[] { "Created", "Assigned", "Failed" },
            events.Select(experimentEvent =>
                experimentEvent.Type).ToArray());

        Assert.Equal(workerId, events[2].WorkerId);
        Assert.Equal(1, events[2].Attempt);

        Assert.NotNull(events[2].Details);
        Assert.Contains(failureMessage, events[2].Details);
    }
}