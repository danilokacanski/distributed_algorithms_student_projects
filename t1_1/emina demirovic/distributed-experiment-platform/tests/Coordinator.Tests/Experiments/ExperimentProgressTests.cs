using System.Net;
using System.Net.Http.Json;
using Contracts;
using Coordinator.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Experiments;

public sealed class ExperimentProgressTests
{
    [Fact]
    public async Task ReportProgress_ValidRequest_PersistsLatestSnapshot()
    {
        const string workerId =
            "worker-progress-snapshot-test";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var assignedExperiment =
            await CreateAssignedExperimentAsync(
                client,
                workerId,
                "Progress snapshot test");

        const string firstMetrics =
            """{"meanReward":12.5,"episode":4}""";

        var firstResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/" +
                $"{assignedExperiment.Id}/progress",
                new ReportExperimentProgressRequest
                {
                    WorkerId = workerId,
                    Attempt = assignedExperiment.Attempt,
                    CurrentStep = 2_500,
                    ProgressMetricsJson = firstMetrics
                });

        Assert.Equal(
            HttpStatusCode.OK,
            firstResponse.StatusCode);

        var firstSnapshot =
            await firstResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(firstSnapshot);
        Assert.Equal(2_500, firstSnapshot.CurrentStep);
        Assert.Equal(
            firstMetrics,
            firstSnapshot.ProgressMetricsJson);

        Assert.NotNull(
            firstSnapshot.LastProgressAtUtc);

        const string secondMetrics =
            """{"meanReward":25.75,"episode":8}""";

        var secondResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/" +
                $"{assignedExperiment.Id}/progress",
                new ReportExperimentProgressRequest
                {
                    WorkerId = workerId,
                    Attempt = assignedExperiment.Attempt,
                    CurrentStep = 5_000,
                    ProgressMetricsJson = secondMetrics
                });

        Assert.Equal(
            HttpStatusCode.OK,
            secondResponse.StatusCode);

        var secondSnapshot =
            await secondResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(secondSnapshot);
        Assert.Equal(5_000, secondSnapshot.CurrentStep);
        Assert.Equal(
            secondMetrics,
            secondSnapshot.ProgressMetricsJson);

        Assert.NotNull(
            secondSnapshot.LastProgressAtUtc);

        Assert.True(
            secondSnapshot.LastProgressAtUtc.Value >=
            firstSnapshot.LastProgressAtUtc.Value);

        var persistedExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{assignedExperiment.Id}");

        Assert.NotNull(persistedExperiment);
        Assert.Equal(5_000, persistedExperiment.CurrentStep);
        Assert.Equal(
            secondMetrics,
            persistedExperiment.ProgressMetricsJson);

        var events =
            await client.GetFromJsonAsync<
                ExperimentEventResponse[]>(
                $"/api/experiments/" +
                $"{assignedExperiment.Id}/events");

        Assert.NotNull(events);

        Assert.Equal(
            new[] { "Created", "Assigned" },
            events.Select(experimentEvent =>
                experimentEvent.Type).ToArray());
    }

    [Fact]
    public async Task ReportProgress_StaleAttempt_IsRejected()
    {
        const string workerId =
            "worker-stale-progress-test";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var firstAssignment =
            await CreateAssignedExperimentAsync(
                client,
                workerId,
                "Stale progress test");

        using (var scope =
            factory.Services.CreateScope())
        {
            var experimentRegistry =
                scope.ServiceProvider
                    .GetRequiredService<ExperimentRegistry>();

            var requeued =
                experimentRegistry.TryRequeue(
                    firstAssignment.Id,
                    workerId,
                    out _);

            Assert.True(requeued);
        }

        var secondAssignmentResponse =
            await client.PostAsync(
                $"/api/experiments/" +
                $"{firstAssignment.Id}/assign",
                null);

        Assert.Equal(
            HttpStatusCode.OK,
            secondAssignmentResponse.StatusCode);

        var secondAssignment =
            await secondAssignmentResponse.Content
                .ReadFromJsonAsync<ExperimentResponse>();

        Assert.NotNull(secondAssignment);
        Assert.Equal(2, secondAssignment.Attempt);

        var staleResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/" +
                $"{firstAssignment.Id}/progress",
                new ReportExperimentProgressRequest
                {
                    WorkerId = workerId,
                    Attempt = firstAssignment.Attempt,
                    CurrentStep = 1_000,
                    ProgressMetricsJson =
                        """{"meanReward":999}"""
                });

        Assert.Equal(
            HttpStatusCode.Conflict,
            staleResponse.StatusCode);

        var persistedExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{firstAssignment.Id}");

        Assert.NotNull(persistedExperiment);
        Assert.Equal(2, persistedExperiment.Attempt);
        Assert.Null(persistedExperiment.CurrentStep);
        Assert.Null(
            persistedExperiment.ProgressMetricsJson);

        Assert.Null(
            persistedExperiment.LastProgressAtUtc);
    }

    [Fact]
    public async Task ReportProgress_LowerStep_IsRejected()
    {
        const string workerId =
            "worker-progress-order-test";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var assignedExperiment =
            await CreateAssignedExperimentAsync(
                client,
                workerId,
                "Progress ordering test");

        var acceptedResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/" +
                $"{assignedExperiment.Id}/progress",
                new ReportExperimentProgressRequest
                {
                    WorkerId = workerId,
                    Attempt = assignedExperiment.Attempt,
                    CurrentStep = 6_000,
                    ProgressMetricsJson =
                        """{"meanReward":30}"""
                });

        Assert.Equal(
            HttpStatusCode.OK,
            acceptedResponse.StatusCode);

        var rejectedResponse =
            await client.PostAsJsonAsync(
                $"/api/experiments/" +
                $"{assignedExperiment.Id}/progress",
                new ReportExperimentProgressRequest
                {
                    WorkerId = workerId,
                    Attempt = assignedExperiment.Attempt,
                    CurrentStep = 5_000,
                    ProgressMetricsJson =
                        """{"meanReward":20}"""
                });

        Assert.Equal(
            HttpStatusCode.Conflict,
            rejectedResponse.StatusCode);

        var persistedExperiment =
            await client.GetFromJsonAsync<ExperimentResponse>(
                $"/api/experiments/{assignedExperiment.Id}");

        Assert.NotNull(persistedExperiment);
        Assert.Equal(6_000, persistedExperiment.CurrentStep);

        Assert.Equal(
            """{"meanReward":30}""",
            persistedExperiment.ProgressMetricsJson);
    }

    private static async Task<ExperimentResponse>
        CreateAssignedExperimentAsync(
            HttpClient client,
            string workerId,
            string name)
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
                    Name = name,
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

        var assignmentResponse =
            await client.PostAsync(
                $"/api/experiments/" +
                $"{createdExperiment.Id}/assign",
                null);

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

        return assignedExperiment;
    }
}