using System.Net;
using System.Net.Http.Json;
using Contracts;

namespace Coordinator.Tests.Experiments;

public sealed class ExperimentCancellationTests
{
    [Fact]
    public async Task Cancel_PendingExperiment_PersistsCancelledState()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client = factory.CreateClient();

        var createResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            new CreateExperimentRequest
            {
                Name = "Pending cancellation test",
                Algorithm = "PPO",
                Environment = "CartPole-v1",
                Seed = 42,
                MaxSteps = 10_000,
                Priority = 5,
                TimeoutSeconds = 120,
                SimulateFailure = false
            });

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createdExperiment =
            await createResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(createdExperiment);

        Assert.Equal(
            ExperimentStatus.Pending,
            createdExperiment.Status);

        var cancellationResponse = await client.PostAsync(
            $"/api/experiments/{createdExperiment.Id}/cancel",
            null);

        Assert.Equal(
            HttpStatusCode.OK,
            cancellationResponse.StatusCode);

        var cancelledExperiment =
            await cancellationResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(cancelledExperiment);

        Assert.Equal(
            ExperimentStatus.Cancelled,
            cancelledExperiment.Status);

        Assert.True(
            cancelledExperiment.CancellationRequested);

        Assert.Null(
            cancelledExperiment.AssignedWorkerId);

        Assert.Equal(
            0,
            cancelledExperiment.Attempt);

        Assert.NotNull(
            cancelledExperiment.FinishedAtUtc);

        Assert.Equal(
            "Experiment was cancelled before execution.",
            cancelledExperiment.ResultMessage);

        Assert.Null(cancelledExperiment.MetricsJson);
        Assert.Null(cancelledExperiment.ExecutionDurationMs);

        var persistedExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{createdExperiment.Id}");

        Assert.NotNull(persistedExperiment);

        Assert.Equal(
            ExperimentStatus.Cancelled,
            persistedExperiment.Status);

        Assert.True(
            persistedExperiment.CancellationRequested);

        Assert.Equal(
            cancelledExperiment.FinishedAtUtc,
            persistedExperiment.FinishedAtUtc);

        var events =
            await client.GetFromJsonAsync<
                ExperimentEventResponse[]>(
                $"/api/experiments/{createdExperiment.Id}/events");

        Assert.NotNull(events);
        Assert.Equal(2, events.Length);

        Assert.Equal(
            new[] { "Created", "Cancelled" },
            events.Select(experimentEvent =>
                experimentEvent.Type).ToArray());

        Assert.Equal(0, events[1].Attempt);
        Assert.Null(events[1].WorkerId);

        Assert.Equal(
            "Experiment was cancelled before execution.",
            events[1].Details);
    }

    [Fact]
    public async Task Cancel_RunningExperiment_PersistsCancellationLifecycle()
    {
        const string workerId = "worker-running-cancellation-test";
        const string cancellationMessage =
            "Experiment execution was cancelled by request.";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client = factory.CreateClient();

        var createResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            new CreateExperimentRequest
            {
                Name = "Running cancellation test",
                Algorithm = "PPO",
                Environment = "CartPole-v1",
                Seed = 42,
                MaxSteps = 10_000,
                Priority = 5,
                TimeoutSeconds = 120,
                SimulateFailure = false
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
        Assert.Equal(
            ExperimentStatus.Running,
            assignedExperiment.Status);

        Assert.False(
            assignedExperiment.CancellationRequested);

        Assert.Equal(1, assignedExperiment.Attempt);
        Assert.Equal(workerId, assignedExperiment.AssignedWorkerId);

        var cancellationResponse = await client.PostAsync(
            $"/api/experiments/{createdExperiment.Id}/cancel",
            null);

        Assert.Equal(
            HttpStatusCode.OK,
            cancellationResponse.StatusCode);

        var cancellationRequestedExperiment =
            await cancellationResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(cancellationRequestedExperiment);

        Assert.Equal(
            ExperimentStatus.Running,
            cancellationRequestedExperiment.Status);

        Assert.True(
            cancellationRequestedExperiment.CancellationRequested);

        Assert.Equal(
            workerId,
            cancellationRequestedExperiment.AssignedWorkerId);

        Assert.Equal(1, cancellationRequestedExperiment.Attempt);
        Assert.Null(cancellationRequestedExperiment.FinishedAtUtc);

        var nextResponse = await client.GetAsync(
            $"/api/experiments/worker/{workerId}/next");

        Assert.Equal(
            HttpStatusCode.NoContent,
            nextResponse.StatusCode);

        var completionResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/{createdExperiment.Id}/complete",
                new CompleteExperimentRequest
                {
                    WorkerId = workerId,
                    Attempt = assignedExperiment.Attempt,
                    Succeeded = false,
                    WasCancelled = true,
                    ResultMessage = cancellationMessage,
                    MetricsJson = null,
                    ExecutionDurationMs = 350
                });

        Assert.Equal(
            HttpStatusCode.OK,
            completionResponse.StatusCode);

        var cancelledExperiment =
            await completionResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(cancelledExperiment);

        Assert.Equal(
            ExperimentStatus.Cancelled,
            cancelledExperiment.Status);

        Assert.True(cancelledExperiment.CancellationRequested);
        Assert.Equal(workerId, cancelledExperiment.AssignedWorkerId);
        Assert.Equal(1, cancelledExperiment.Attempt);
        Assert.Equal(
            cancellationMessage,
            cancelledExperiment.ResultMessage);

        Assert.Equal(350, cancelledExperiment.ExecutionDurationMs);
        Assert.Null(cancelledExperiment.MetricsJson);
        Assert.NotNull(cancelledExperiment.FinishedAtUtc);

        var persistedExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{createdExperiment.Id}");

        Assert.NotNull(persistedExperiment);

        Assert.Equal(
            ExperimentStatus.Cancelled,
            persistedExperiment.Status);

        Assert.True(persistedExperiment.CancellationRequested);

        var events =
            await client.GetFromJsonAsync<ExperimentEventResponse[]>(
                $"/api/experiments/{createdExperiment.Id}/events");

        Assert.NotNull(events);
        Assert.Equal(4, events.Length);

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

        Assert.Equal(workerId, events[2].WorkerId);
        Assert.Equal(1, events[2].Attempt);

        Assert.Equal(workerId, events[3].WorkerId);
        Assert.Equal(1, events[3].Attempt);
    }
}