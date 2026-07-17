using System.Net.Http.Json;
using Contracts;
using Coordinator.Commands;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Commands;

public sealed class CoordinatorCommandProcessorTests
{
    [Fact]
    public async Task Apply_CreateCommandTwice_IsIdempotent()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        using var scope =
            factory.Services.CreateScope();

        var commandProcessor =
            scope.ServiceProvider.GetRequiredService<
                CoordinatorCommandProcessor>();

        var command = new CreateExperimentCommand(
            CommandId: Guid.NewGuid(),
            OccurredAtUtc:
                new DateTimeOffset(
                    2026,
                    7,
                    1,
                    12,
                    0,
                    0,
                    TimeSpan.Zero),
            ExperimentId: Guid.NewGuid(),
            EventId: Guid.NewGuid(),
            Name: "Idempotent command test",
            Algorithm: "PPO",
            Environment: "CartPole-v1",
            Seed: 42,
            MaxSteps: 10_000,
            Priority: 5,
            TimeoutSeconds: 120,
            SimulateFailure: false);

        var firstResult =
            commandProcessor.Apply(command);

        var secondResult =
            commandProcessor.Apply(command);

        Assert.False(
            firstResult.WasAlreadyApplied);

        Assert.True(
            secondResult.WasAlreadyApplied);

        Assert.Equal(
            command.ExperimentId,
            firstResult.Value.Id);

        Assert.Equal(
            firstResult.Value.Id,
            secondResult.Value.Id);

        Assert.Equal(
            ExperimentStatus.Pending,
            secondResult.Value.Status);

        Assert.Equal(
            command.OccurredAtUtc,
            firstResult.Value.CreatedAtUtc);

        var experiments =
            await client.GetFromJsonAsync<
                ExperimentResponse[]>(
                "/api/experiments");

        Assert.NotNull(experiments);

        var persistedExperiment =
            Assert.Single(experiments);

        Assert.Equal(
            command.ExperimentId,
            persistedExperiment.Id);

        var events =
            await client.GetFromJsonAsync<
                ExperimentEventResponse[]>(
                $"/api/experiments/" +
                $"{command.ExperimentId}/events");

        Assert.NotNull(events);

        var createdEvent =
            Assert.Single(events);

        Assert.Equal(
            command.EventId,
            createdEvent.Id);

        Assert.Equal(
            "Created",
            createdEvent.Type);

    }

    [Fact]
    public void Apply_AssignCommandTwice_IsIdempotent()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var scope =
            factory.Services.CreateScope();

        var processor =
            scope.ServiceProvider.GetRequiredService<
                CoordinatorCommandProcessor>();

        var createCommand =
            CreateCommand("Idempotent assignment test");

        processor.Apply(createCommand);

        var assignCommand = new AssignExperimentCommand(
            CommandId: Guid.NewGuid(),
            OccurredAtUtc: DateTimeOffset.UtcNow,
            ExperimentId: createCommand.ExperimentId,
            EventId: Guid.NewGuid(),
            WorkerId: "worker-command-assignment",
            Attempt: 1);

        var firstResult =
            processor.Apply(assignCommand);

        var secondResult =
            processor.Apply(assignCommand);

        Assert.False(firstResult.WasAlreadyApplied);
        Assert.True(secondResult.WasAlreadyApplied);

        Assert.Equal(
            ExperimentStatus.Running,
            secondResult.Value.Status);

        Assert.Equal(1, secondResult.Value.Attempt);

        Assert.Equal(
            "worker-command-assignment",
            secondResult.Value.AssignedWorkerId);
    }

    [Fact]
    public void Apply_CompleteCommandTwice_IsIdempotent()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var scope =
            factory.Services.CreateScope();

        var processor =
            scope.ServiceProvider.GetRequiredService<
                CoordinatorCommandProcessor>();

        var createCommand =
            CreateCommand("Idempotent completion test");

        processor.Apply(createCommand);

        processor.Apply(
            new AssignExperimentCommand(
                Guid.NewGuid(),
                DateTimeOffset.UtcNow,
                createCommand.ExperimentId,
                Guid.NewGuid(),
                "worker-command-completion",
                1));

        var completeCommand =
            new CompleteExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: createCommand.ExperimentId,
                EventId: Guid.NewGuid(),
                WorkerId: "worker-command-completion",
                Attempt: 1,
                Succeeded: true,
                WasCancelled: false,
                ResultMessage: "Command completed successfully.",
                MetricsJson: """{"reward":50}""",
                ExecutionDurationMs: 500);

        var firstResult =
            processor.Apply(completeCommand);

        var secondResult =
            processor.Apply(completeCommand);

        Assert.False(firstResult.WasAlreadyApplied);
        Assert.True(secondResult.WasAlreadyApplied);

        Assert.Equal(
            ExperimentStatus.Completed,
            secondResult.Value.Status);

        Assert.Equal(
            """{"reward":50}""",
            secondResult.Value.MetricsJson);

        Assert.Equal(
            500,
            secondResult.Value.ExecutionDurationMs);
    }

    [Fact]
    public void Apply_CancellationCommandTwice_IsIdempotent()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var scope =
            factory.Services.CreateScope();

        var processor =
            scope.ServiceProvider.GetRequiredService<
                CoordinatorCommandProcessor>();

        var createCommand =
            CreateCommand("Idempotent cancellation test");

        processor.Apply(createCommand);

        var cancellationCommand =
            new RequestExperimentCancellationCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: createCommand.ExperimentId,
                EventId: Guid.NewGuid());

        var firstResult =
            processor.Apply(cancellationCommand);

        var secondResult =
            processor.Apply(cancellationCommand);

        Assert.False(firstResult.WasAlreadyApplied);
        Assert.True(secondResult.WasAlreadyApplied);

        Assert.Equal(
            ExperimentStatus.Cancelled,
            secondResult.Value.Status);

        Assert.True(
            secondResult.Value.CancellationRequested);

        Assert.NotNull(
            secondResult.Value.FinishedAtUtc);
    }

    [Fact]
    public void Apply_RequeueCommandTwice_IsIdempotent()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var scope =
            factory.Services.CreateScope();

        var processor =
            scope.ServiceProvider.GetRequiredService<
                CoordinatorCommandProcessor>();

        var createCommand =
            CreateCommand("Idempotent requeue test");

        processor.Apply(createCommand);

        processor.Apply(
            new AssignExperimentCommand(
                Guid.NewGuid(),
                DateTimeOffset.UtcNow,
                createCommand.ExperimentId,
                Guid.NewGuid(),
                "worker-requeue-command",
                1));

        var requeueCommand =
            new RequeueExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: createCommand.ExperimentId,
                EventId: Guid.NewGuid(),
                WorkerId: "worker-requeue-command",
                Attempt: 1,
                CancelInsteadOfRequeue: false);

        var firstResult =
            processor.Apply(requeueCommand);

        var secondResult =
            processor.Apply(requeueCommand);

        Assert.False(firstResult.WasAlreadyApplied);
        Assert.True(secondResult.WasAlreadyApplied);

        Assert.Equal(
            ExperimentStatus.Pending,
            secondResult.Value.Status);

        Assert.Null(
            secondResult.Value.AssignedWorkerId);

        Assert.Equal(1, secondResult.Value.Attempt);
    }

    [Fact]
    public void Apply_StartupRecoveryCommandTwice_IsIdempotent()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var scope =
            factory.Services.CreateScope();

        var processor =
            scope.ServiceProvider.GetRequiredService<
                CoordinatorCommandProcessor>();

        var createCommand =
            CreateCommand("Idempotent startup recovery test");

        processor.Apply(createCommand);

        processor.Apply(
            new AssignExperimentCommand(
                Guid.NewGuid(),
                DateTimeOffset.UtcNow,
                createCommand.ExperimentId,
                Guid.NewGuid(),
                "worker-startup-recovery",
                1));

        var recoveryCommand =
            new RecoverExperimentOnStartupCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: createCommand.ExperimentId,
                EventId: Guid.NewGuid(),
                PreviousWorkerId:
                    "worker-startup-recovery",
                Attempt: 1);

        var firstResult =
            processor.Apply(recoveryCommand);

        var secondResult =
            processor.Apply(recoveryCommand);

        Assert.False(firstResult.WasAlreadyApplied);
        Assert.True(secondResult.WasAlreadyApplied);

        Assert.Equal(
            ExperimentStatus.Pending,
            secondResult.Value.Status);

        Assert.Null(
            secondResult.Value.AssignedWorkerId);

        Assert.Equal(1, secondResult.Value.Attempt);
    }

    [Fact]
    public void Apply_ReusedCommandIdForDifferentCommand_IsRejected()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var scope =
            factory.Services.CreateScope();

        var processor =
            scope.ServiceProvider.GetRequiredService<
                CoordinatorCommandProcessor>();

        var createCommand =
            CreateCommand("Reused command id test");

        processor.Apply(createCommand);

        var invalidAssignCommand =
            new AssignExperimentCommand(
                CommandId: createCommand.CommandId,
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: createCommand.ExperimentId,
                EventId: Guid.NewGuid(),
                WorkerId: "worker-reused-command-id",
                Attempt: 1);

        var exception =
            Assert.Throws<InvalidOperationException>(
                () => processor.Apply(
                    invalidAssignCommand));

        Assert.Contains(
            "already been used for a different command",
            exception.Message);
    }

    private static CreateExperimentCommand CreateCommand(
        string name)
    {
        return new CreateExperimentCommand(
            CommandId: Guid.NewGuid(),
            OccurredAtUtc: DateTimeOffset.UtcNow,
            ExperimentId: Guid.NewGuid(),
            EventId: Guid.NewGuid(),
            Name: name,
            Algorithm: "PPO",
            Environment: "CartPole-v1",
            Seed: 42,
            MaxSteps: 10_000,
            Priority: 5,
            TimeoutSeconds: 120,
            SimulateFailure: false);
    }
}