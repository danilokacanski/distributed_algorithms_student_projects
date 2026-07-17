using System.Net;
using System.Net.Http.Json;
using Contracts;
using Coordinator.Commands;
using Coordinator.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Experiments;

public sealed class ExperimentPersistenceTests
{
    [Fact]
    public async Task CompletedExperiment_PersistsAcrossCoordinatorRestart()
    {
        const string workerId = "worker-persistence-test";
        const string resultMessage = "Persistent result.";
        const string metricsJson =
            """{"reward":150.25,"episodes":30}""";

        var databaseDirectory = CreateDatabaseDirectory();
        var databasePath = Path.Combine(
            databaseDirectory,
            "coordinator-tests.db");

        Guid experimentId;
        DateTimeOffset? finishedAtUtc;

        try
        {
            using (var firstFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var firstClient =
                    firstFactory.CreateClient();

                var createResponse =
                    await firstClient.PostAsJsonAsync(
                        "/api/experiments",
                        CreateRequest(
                            "Persistence restart test"));

                Assert.Equal(
                    HttpStatusCode.Created,
                    createResponse.StatusCode);

                var createdExperiment =
                    await createResponse.Content
                        .ReadFromJsonAsync<ExperimentResponse>();

                Assert.NotNull(createdExperiment);

                experimentId = createdExperiment.Id;

                var registrationResponse =
                    await firstClient.PostAsJsonAsync(
                        "/api/workers/register",
                        new WorkerRegistrationRequest
                        {
                            WorkerId = workerId
                        });

                Assert.Equal(
                    HttpStatusCode.OK,
                    registrationResponse.StatusCode);

                var assignmentResponse =
                    await firstClient.PostAsync(
                        $"/api/experiments/{experimentId}/assign",
                        null);

                Assert.Equal(
                    HttpStatusCode.OK,
                    assignmentResponse.StatusCode);

                var assignedExperiment =
                    await assignmentResponse.Content
                        .ReadFromJsonAsync<ExperimentResponse>();

                Assert.NotNull(assignedExperiment);
                Assert.Equal(1, assignedExperiment.Attempt);

                var completionResponse =
                    await firstClient.PostAsJsonAsync(
                        $"/api/experiments/{experimentId}/complete",
                        new CompleteExperimentRequest
                        {
                            WorkerId = workerId,
                            Attempt = assignedExperiment.Attempt,
                            Succeeded = true,
                            WasCancelled = false,
                            ResultMessage = resultMessage,
                            MetricsJson = metricsJson,
                            ExecutionDurationMs = 2_500
                        });

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

                finishedAtUtc =
                    completedExperiment.FinishedAtUtc;

                Assert.NotNull(finishedAtUtc);
            }

            using (var secondFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var secondClient =
                    secondFactory.CreateClient();

                var startupRecovery =
                    secondFactory.Services
                        .GetRequiredService<
                            ExperimentStartupRecoveryService>();

                await startupRecovery.RunStartupRecoveryCycleAsync(
                    CancellationToken.None);

                var persistedExperiment =
                    await secondClient
                        .GetFromJsonAsync<ExperimentResponse>(
                            $"/api/experiments/{experimentId}");

                Assert.NotNull(persistedExperiment);

                Assert.Equal(
                    ExperimentStatus.Completed,
                    persistedExperiment.Status);

                Assert.Equal(1, persistedExperiment.Attempt);
                Assert.Equal(workerId, persistedExperiment.AssignedWorkerId);
                Assert.Equal(resultMessage, persistedExperiment.ResultMessage);
                Assert.Equal(metricsJson, persistedExperiment.MetricsJson);
                Assert.Equal(2_500, persistedExperiment.ExecutionDurationMs);
                Assert.Equal(
                    finishedAtUtc,
                    persistedExperiment.FinishedAtUtc);

                var events =
                    await secondClient.GetFromJsonAsync<
                        ExperimentEventResponse[]>(
                        $"/api/experiments/{experimentId}/events");

                Assert.NotNull(events);
                Assert.Equal(3, events.Length);

                Assert.Equal(
                    new[] { "Created", "Assigned", "Completed" },
                    events.Select(experimentEvent =>
                        experimentEvent.Type).ToArray());
            }
        }
        finally
        {
            DeleteDatabaseDirectory(databaseDirectory);
        }
    }

    [Fact]
    public async Task CoordinatorRestart_RequeuesRunningExperiment()
    {
        const string workerId =
            "worker-coordinator-restart-test";

        var databaseDirectory = CreateDatabaseDirectory();
        var databasePath = Path.Combine(
            databaseDirectory,
            "coordinator-tests.db");

        Guid experimentId;

        try
        {
            using (var firstFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var firstClient =
                    firstFactory.CreateClient();

                var createResponse =
                    await firstClient.PostAsJsonAsync(
                        "/api/experiments",
                        CreateRequest(
                            "Coordinator startup recovery test"));

                Assert.Equal(
                    HttpStatusCode.Created,
                    createResponse.StatusCode);

                var createdExperiment =
                    await createResponse.Content
                        .ReadFromJsonAsync<ExperimentResponse>();

                Assert.NotNull(createdExperiment);

                experimentId = createdExperiment.Id;

                using var scope =
                    firstFactory.Services.CreateScope();

                var commandProcessor =
                    scope.ServiceProvider
                        .GetRequiredService<
                            CoordinatorCommandProcessor>();

                var assignmentResult =
                    commandProcessor.Apply(
                        new AssignExperimentCommand(
                            CommandId: Guid.NewGuid(),
                            OccurredAtUtc: DateTimeOffset.UtcNow,
                            ExperimentId: experimentId,
                            EventId: Guid.NewGuid(),
                            WorkerId: workerId,
                            Attempt: 1));

                var assignedExperiment =
                    assignmentResult.Value;

                Assert.Equal(
                    ExperimentStatus.Running,
                    assignedExperiment.Status);

                Assert.Equal(
                    workerId,
                    assignedExperiment.AssignedWorkerId);

                Assert.Equal(1, assignedExperiment.Attempt);
            }

            using (var secondFactory =
                new CoordinatorWebApplicationFactory(
                    databasePath: databasePath,
                    deleteDatabaseOnDispose: false))
            {
                using var secondClient =
                    secondFactory.CreateClient();

                var startupRecovery =
                    secondFactory.Services
                        .GetRequiredService<
                            ExperimentStartupRecoveryService>();

                await startupRecovery.RunStartupRecoveryCycleAsync(
                    CancellationToken.None);

                var recoveredExperiment =
                    await secondClient
                        .GetFromJsonAsync<ExperimentResponse>(
                            $"/api/experiments/{experimentId}");

                Assert.NotNull(recoveredExperiment);

                Assert.Equal(
                    ExperimentStatus.Pending,
                    recoveredExperiment.Status);

                Assert.Null(
                    recoveredExperiment.AssignedWorkerId);

                Assert.Equal(1, recoveredExperiment.Attempt);
                Assert.Null(recoveredExperiment.FinishedAtUtc);
                Assert.Null(recoveredExperiment.ResultMessage);

                var events =
                    await secondClient.GetFromJsonAsync<
                        ExperimentEventResponse[]>(
                        $"/api/experiments/{experimentId}/events");

                Assert.NotNull(events);
                Assert.Equal(3, events.Length);

                Assert.Equal(
                    new[]
                    {
                        "Created",
                        "Assigned",
                        "RecoveredOnStartup"
                    },
                    events.Select(experimentEvent =>
                        experimentEvent.Type).ToArray());

                Assert.Equal(workerId, events[2].WorkerId);
                Assert.Equal(1, events[2].Attempt);

                Assert.Equal(
                    "Experiment was returned to Pending after Coordinator restart.",
                    events[2].Details);
            }
        }
        finally
        {
            DeleteDatabaseDirectory(databaseDirectory);
        }
    }

    [Fact]
    public async Task SeparateFactories_UseIsolatedDatabases()
    {
        Guid experimentId;

        using (var firstFactory =
            new CoordinatorWebApplicationFactory())
        {
            using var firstClient =
                firstFactory.CreateClient();

            var createResponse =
                await firstClient.PostAsJsonAsync(
                    "/api/experiments",
                    CreateRequest("Database isolation test"));

            Assert.Equal(
                HttpStatusCode.Created,
                createResponse.StatusCode);

            var createdExperiment =
                await createResponse.Content
                    .ReadFromJsonAsync<ExperimentResponse>();

            Assert.NotNull(createdExperiment);

            experimentId = createdExperiment.Id;

            var firstDatabaseResponse =
                await firstClient.GetAsync(
                    $"/api/experiments/{experimentId}");

            Assert.Equal(
                HttpStatusCode.OK,
                firstDatabaseResponse.StatusCode);
        }

        using var secondFactory =
            new CoordinatorWebApplicationFactory();

        using var secondClient =
            secondFactory.CreateClient();

        var secondDatabaseResponse =
            await secondClient.GetAsync(
                $"/api/experiments/{experimentId}");

        Assert.Equal(
            HttpStatusCode.NotFound,
            secondDatabaseResponse.StatusCode);
    }

    private static CreateExperimentRequest CreateRequest(
        string name)
    {
        return new CreateExperimentRequest
        {
            Name = name,
            Algorithm = "PPO",
            Environment = "CartPole-v1",
            Seed = 42,
            MaxSteps = 10_000,
            Priority = 5,
            TimeoutSeconds = 120,
            SimulateFailure = false
        };
    }

    private static string CreateDatabaseDirectory()
    {
        var databaseDirectory = Path.Combine(
            Path.GetTempPath(),
            "distributed-experiment-platform-tests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(databaseDirectory);

        return databaseDirectory;
    }

    private static void DeleteDatabaseDirectory(
        string databaseDirectory)
    {
        if (Directory.Exists(databaseDirectory))
        {
            Directory.Delete(
                databaseDirectory,
                recursive: true);
        }
    }
}