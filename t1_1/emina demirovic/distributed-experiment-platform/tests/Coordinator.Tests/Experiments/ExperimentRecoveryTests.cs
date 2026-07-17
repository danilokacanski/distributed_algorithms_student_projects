using System.Net;
using System.Net.Http.Json;
using Contracts;
using Coordinator.Commands;
using Coordinator.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Experiments;

public sealed class ExperimentRecoveryTests
{
    [Fact]
    public async Task RecoveryService_OfflineWorker_RequeuesRunningExperiment()
    {
        const string workerId = "offline-worker-recovery-test";

        using var factory =
            new CoordinatorWebApplicationFactory(
                disableScheduler: true,
                disableRecovery: false);

        using var client = factory.CreateClient();

        var createResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            new CreateExperimentRequest
            {
                Name = "Offline worker recovery test",
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

        using (var scope = factory.Services.CreateScope())
        {
            var commandProcessor =
                scope.ServiceProvider
                    .GetRequiredService<
                        CoordinatorCommandProcessor>();

            var assignmentResult =
                commandProcessor.Apply(
                    new AssignExperimentCommand(
                        CommandId: Guid.NewGuid(),
                        OccurredAtUtc: DateTimeOffset.UtcNow,
                        ExperimentId: createdExperiment.Id,
                        EventId: Guid.NewGuid(),
                        WorkerId: workerId,
                        Attempt: 1));

            var assignedExperiment =
                assignmentResult.Value;

            Assert.Equal(
                ExperimentStatus.Running,
                assignedExperiment.Status);

            Assert.Equal(workerId, assignedExperiment.AssignedWorkerId);
            Assert.Equal(1, assignedExperiment.Attempt);
        }

        ExperimentResponse? recoveredExperiment = null;

        var recoveryDeadline =
            DateTimeOffset.UtcNow.AddSeconds(10);

        while (DateTimeOffset.UtcNow < recoveryDeadline)
        {
            recoveredExperiment =
                await client.GetFromJsonAsync<ExperimentResponse>(
                    $"/api/experiments/{createdExperiment.Id}");

            if (recoveredExperiment?.Status ==
                ExperimentStatus.Pending)
            {
                break;
            }

            await Task.Delay(100);
        }

        Assert.NotNull(recoveredExperiment);

        Assert.Equal(
            ExperimentStatus.Pending,
            recoveredExperiment.Status);

        Assert.Null(recoveredExperiment.AssignedWorkerId);
        Assert.Equal(1, recoveredExperiment.Attempt);
        Assert.Null(recoveredExperiment.FinishedAtUtc);
        Assert.Null(recoveredExperiment.ResultMessage);
        Assert.False(recoveredExperiment.CancellationRequested);

        var events =
            await client.GetFromJsonAsync<ExperimentEventResponse[]>(
                $"/api/experiments/{createdExperiment.Id}/events");

        Assert.NotNull(events);
        Assert.Equal(3, events.Length);

        Assert.Equal(
            new[] { "Created", "Assigned", "Requeued" },
            events.Select(experimentEvent =>
                experimentEvent.Type).ToArray());

        Assert.Equal(workerId, events[2].WorkerId);
        Assert.Equal(1, events[2].Attempt);

        Assert.NotNull(events[2].Details);
        Assert.Contains(
            "became unavailable",
            events[2].Details);
    }

    [Fact]
    public async Task WorkerRecovery_CancellationRequested_CancelsExperiment()
    {
        const string workerId =
            "offline-worker-cancellation-recovery-test";

        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client = factory.CreateClient();

        var createResponse = await client.PostAsJsonAsync(
            "/api/experiments",
            new CreateExperimentRequest
            {
                Name = "Cancellation recovery test",
                Algorithm = "DQN",
                Environment = "LunarLander-v3",
                Seed = 15,
                MaxSteps = 5_000,
                Priority = 6,
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

        using var scope = factory.Services.CreateScope();

        var experimentRegistry =
            scope.ServiceProvider
                .GetRequiredService<ExperimentRegistry>();

        var assigned = experimentRegistry.TryAssign(
            createdExperiment.Id,
            workerId,
            out var assignedExperiment);

        Assert.True(assigned);
        Assert.NotNull(assignedExperiment);

        Assert.Equal(
            ExperimentStatus.Running,
            assignedExperiment.Status);

        Assert.Equal(1, assignedExperiment.Attempt);

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

        var commandProcessor =
            scope.ServiceProvider
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        var recoveryResult =
            commandProcessor.Apply(
                new RequeueExperimentCommand(
                    CommandId: Guid.NewGuid(),
                    OccurredAtUtc:
                        DateTimeOffset.UtcNow,
                    ExperimentId: createdExperiment.Id,
                    EventId: Guid.NewGuid(),
                    WorkerId: workerId,
                    Attempt: assignedExperiment.Attempt,
                    CancelInsteadOfRequeue: true));

        var cancelledExperiment =
            recoveryResult.Value;

        Assert.Equal(
            ExperimentStatus.Cancelled,
            cancelledExperiment.Status);

        Assert.True(cancelledExperiment.CancellationRequested);
        Assert.Null(cancelledExperiment.AssignedWorkerId);
        Assert.Equal(1, cancelledExperiment.Attempt);
        Assert.NotNull(cancelledExperiment.FinishedAtUtc);

        Assert.Equal(
            "Experiment was cancelled after the worker became unavailable.",
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

        Assert.True(persistedExperiment.CancellationRequested);
        Assert.Null(persistedExperiment.AssignedWorkerId);

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

        Assert.Equal(workerId, events[3].WorkerId);
        Assert.Equal(1, events[3].Attempt);

        Assert.NotNull(events[3].Details);
        Assert.Contains(
            "became unavailable",
            events[3].Details);
    }
}