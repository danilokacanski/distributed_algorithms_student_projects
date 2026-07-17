using System.Net;
using System.Net.Http.Json;
using Contracts;

namespace Coordinator.Tests.Experiments;

public sealed class ExperimentCreationTests
{
    [Fact]
    public async Task Create_WithValidRequest_PersistsPendingExperiment()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client = factory.CreateClient();

        var request = new CreateExperimentRequest
        {
            Name = "Creation integration test",
            Algorithm = "PPO",
            Environment = "CartPole-v1",
            Seed = 42,
            MaxSteps = 10_000,
            Priority = 7,
            TimeoutSeconds = 120,
            SimulateFailure = false
        };

        var createResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            request);

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createdExperiment =
            await createResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(createdExperiment);

        Assert.NotEqual(Guid.Empty, createdExperiment.Id);
        Assert.Equal(request.Name, createdExperiment.Name);
        Assert.Equal(request.Algorithm, createdExperiment.Algorithm);
        Assert.Equal(request.Environment, createdExperiment.Environment);
        Assert.Equal(request.Seed, createdExperiment.Seed);
        Assert.Equal(request.MaxSteps, createdExperiment.MaxSteps);
        Assert.Equal(request.Priority, createdExperiment.Priority);
        Assert.Equal(
            request.TimeoutSeconds,
            createdExperiment.TimeoutSeconds);

        Assert.Equal(
            ExperimentStatus.Pending,
            createdExperiment.Status);

        Assert.Equal(0, createdExperiment.Attempt);
        Assert.False(createdExperiment.CancellationRequested);
        Assert.Null(createdExperiment.AssignedWorkerId);
        Assert.Null(createdExperiment.FinishedAtUtc);
        Assert.NotEqual(
            default,
            createdExperiment.CreatedAtUtc);

        var getResponse = await client.GetAsync(
            $"/api/experiments/{createdExperiment.Id}");

        Assert.Equal(
            HttpStatusCode.OK,
            getResponse.StatusCode);

        var persistedExperiment =
            await getResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(persistedExperiment);
        Assert.Equal(
            createdExperiment.Id,
            persistedExperiment.Id);

        Assert.Equal(
            ExperimentStatus.Pending,
            persistedExperiment.Status);

        Assert.Equal(
            createdExperiment.CreatedAtUtc,
            persistedExperiment.CreatedAtUtc);

        var eventsResponse = await client.GetAsync(
            $"/api/experiments/{createdExperiment.Id}/events");

        Assert.Equal(
            HttpStatusCode.OK,
            eventsResponse.StatusCode);

        var events = await eventsResponse.Content
            .ReadFromJsonAsync<ExperimentEventResponse[]>();

        Assert.NotNull(events);

        var createdEvent = Assert.Single(events);

        Assert.Equal("Created", createdEvent.Type);
        Assert.Equal(0, createdEvent.Attempt);
        Assert.Null(createdEvent.WorkerId);
        Assert.NotEqual(default, createdEvent.OccurredAtUtc);

        Assert.NotNull(createdEvent.Details);
        Assert.Contains(request.Name, createdEvent.Details);
    }
}