using System.Net;
using System.Net.Http.Json;
using Contracts;

namespace Coordinator.Tests.Experiments;

public sealed class ExperimentSchedulingTests
{
    [Fact]
    public async Task AutomaticScheduler_AssignsHighestPriorityPendingExperiment()
    {
        const string workerId = "worker-priority-scheduling-test";

        using var factory =
            new CoordinatorWebApplicationFactory(
                disableScheduler: false);

        using var client = factory.CreateClient();

        var lowPriorityResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            new CreateExperimentRequest
            {
                Name = "Low priority experiment",
                Algorithm = "PPO",
                Environment = "CartPole-v1",
                Seed = 1,
                MaxSteps = 1_000,
                Priority = 1,
                TimeoutSeconds = 120,
                SimulateFailure = false
            });

        Assert.Equal(
            HttpStatusCode.Created,
            lowPriorityResponse.StatusCode);

        var lowPriorityExperiment =
            await lowPriorityResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(lowPriorityExperiment);

        var highPriorityResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            new CreateExperimentRequest
            {
                Name = "High priority experiment",
                Algorithm = "DQN",
                Environment = "LunarLander-v3",
                Seed = 2,
                MaxSteps = 2_000,
                Priority = 9,
                TimeoutSeconds = 120,
                SimulateFailure = false
            });

        Assert.Equal(
            HttpStatusCode.Created,
            highPriorityResponse.StatusCode);

        var highPriorityExperiment =
            await highPriorityResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(highPriorityExperiment);

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

        ExperimentResponse? scheduledExperiment = null;

        var schedulingDeadline =
            DateTimeOffset.UtcNow.AddSeconds(10);

        while (DateTimeOffset.UtcNow < schedulingDeadline)
        {
            scheduledExperiment =
                await client.GetFromJsonAsync<ExperimentResponse>(
                    $"/api/experiments/{highPriorityExperiment.Id}");

            if (scheduledExperiment?.Status ==
                ExperimentStatus.Running)
            {
                break;
            }

            await Task.Delay(100);
        }

        Assert.NotNull(scheduledExperiment);

        Assert.Equal(
            ExperimentStatus.Running,
            scheduledExperiment.Status);

        Assert.Equal(
            workerId,
            scheduledExperiment.AssignedWorkerId);

        Assert.Equal(1, scheduledExperiment.Attempt);

        var persistedLowPriorityExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{lowPriorityExperiment.Id}");

        Assert.NotNull(persistedLowPriorityExperiment);

        Assert.Equal(
            ExperimentStatus.Pending,
            persistedLowPriorityExperiment.Status);

        Assert.Null(
            persistedLowPriorityExperiment.AssignedWorkerId);

        Assert.Equal(
            0,
            persistedLowPriorityExperiment.Attempt);

        var highPriorityEvents =
            await client.GetFromJsonAsync<
                ExperimentEventResponse[]>(
                $"/api/experiments/{highPriorityExperiment.Id}/events");

        Assert.NotNull(highPriorityEvents);

        Assert.Equal(
            new[] { "Created", "Assigned" },
            highPriorityEvents
                .Select(experimentEvent =>
                    experimentEvent.Type)
                .ToArray());

        var lowPriorityEvents =
            await client.GetFromJsonAsync<
                ExperimentEventResponse[]>(
                $"/api/experiments/{lowPriorityExperiment.Id}/events");

        Assert.NotNull(lowPriorityEvents);

        var lowPriorityCreatedEvent =
            Assert.Single(lowPriorityEvents);

        Assert.Equal(
            "Created",
            lowPriorityCreatedEvent.Type);
    }
}