using System.Net;
using System.Net.Http.Json;
using Contracts;
using Coordinator.Commands;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Experiments;

public sealed class AttemptFencingTests
{
    [Fact]
    public async Task Complete_WithStaleAttempt_IsRejected()
    {
        const string workerId = "worker-attempt-fencing-test";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client = factory.CreateClient();

        var createResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            new CreateExperimentRequest
            {
                Name = "Attempt fencing test",
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

        var firstAssignmentResponse =
            await client.PostAsync(
                $"/api/experiments/{createdExperiment.Id}/assign",
                null);

        Assert.Equal(
            HttpStatusCode.OK,
            firstAssignmentResponse.StatusCode);

        var firstAssignment =
            await firstAssignmentResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(firstAssignment);
        Assert.Equal(1, firstAssignment.Attempt);
        Assert.Equal(
            ExperimentStatus.Running,
            firstAssignment.Status);

        using (var scope = factory.Services.CreateScope())
        {
            var commandProcessor =
                scope.ServiceProvider
                    .GetRequiredService<
                        CoordinatorCommandProcessor>();

            var requeueResult =
                commandProcessor.Apply(
                    new RequeueExperimentCommand(
                        CommandId: Guid.NewGuid(),
                        OccurredAtUtc: DateTimeOffset.UtcNow,
                        ExperimentId: createdExperiment.Id,
                        EventId: Guid.NewGuid(),
                        WorkerId: workerId,
                        Attempt: firstAssignment.Attempt,
                        CancelInsteadOfRequeue: false));

            var requeuedExperiment =
                requeueResult.Value;

            Assert.Equal(
                ExperimentStatus.Pending,
                requeuedExperiment.Status);

            Assert.Equal(1, requeuedExperiment.Attempt);
            Assert.Null(requeuedExperiment.AssignedWorkerId);
        }

        var secondAssignmentResponse =
            await client.PostAsync(
                $"/api/experiments/{createdExperiment.Id}/assign",
                null);

        Assert.Equal(
            HttpStatusCode.OK,
            secondAssignmentResponse.StatusCode);

        var secondAssignment =
            await secondAssignmentResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(secondAssignment);

        Assert.Equal(
            ExperimentStatus.Running,
            secondAssignment.Status);

        Assert.Equal(2, secondAssignment.Attempt);
        Assert.Equal(
            workerId,
            secondAssignment.AssignedWorkerId);

        var staleCompletionResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/{createdExperiment.Id}/complete",
                new CompleteExperimentRequest
                {
                    WorkerId = workerId,
                    Attempt = firstAssignment.Attempt,
                    Succeeded = true,
                    WasCancelled = false,
                    ResultMessage =
                        "Stale result from the first attempt.",
                    MetricsJson = """{"reward":999}""",
                    ExecutionDurationMs = 500
                });

        Assert.Equal(
            HttpStatusCode.Conflict,
            staleCompletionResponse.StatusCode);

        var persistedExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{createdExperiment.Id}");

        Assert.NotNull(persistedExperiment);

        Assert.Equal(
            ExperimentStatus.Running,
            persistedExperiment.Status);

        Assert.Equal(2, persistedExperiment.Attempt);
        Assert.Equal(workerId, persistedExperiment.AssignedWorkerId);
        Assert.Null(persistedExperiment.ResultMessage);
        Assert.Null(persistedExperiment.MetricsJson);
        Assert.Null(persistedExperiment.ExecutionDurationMs);
        Assert.Null(persistedExperiment.FinishedAtUtc);

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
                "Requeued",
                "Assigned"
            },
            events.Select(experimentEvent =>
                experimentEvent.Type).ToArray());

        Assert.Equal(1, events[1].Attempt);
        Assert.Equal(1, events[2].Attempt);
        Assert.Equal(2, events[3].Attempt);
    }
}