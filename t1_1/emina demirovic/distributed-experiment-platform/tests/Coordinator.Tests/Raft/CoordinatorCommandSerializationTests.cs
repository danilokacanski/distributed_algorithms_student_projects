using Coordinator.Commands;
using Coordinator.Raft;
using Microsoft.Extensions.DependencyInjection;

namespace Coordinator.Tests.Raft;

public sealed class
    CoordinatorCommandSerializationTests
{
    [Fact]
    public void Serializer_RoundTripsAllSupportedCommands()
    {
        var serializer =
            new CoordinatorCommandSerializer();

        foreach (var originalCommand in
            CreateSupportedCommands())
        {
            var serialized =
                serializer.Serialize(
                    originalCommand);

            var restoredCommand =
                serializer.Deserialize(
                    serialized.CommandType,
                    serialized.PayloadJson);

            Assert.Equal(
                originalCommand,
                restoredCommand);
        }
    }

    [Fact]
    public void Serializer_RejectsUnsupportedCommandType()
    {
        var serializer =
            new CoordinatorCommandSerializer();

        var command =
            new UnsupportedCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow);

        var exception =
            Assert.Throws<InvalidOperationException>(
                () => serializer.Serialize(command));

        Assert.Contains(
            "is not supported",
            exception.Message);
    }

    [Fact]
    public void LogManager_AppendsCommandsSequentially()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var manager =
            factory.Services
                .GetRequiredService<
                    RaftLogManager>();

        var commands =
            CreateSupportedCommands();

        var firstEntry =
            manager.AppendCommand(
                commands[0],
                term: 3);

        var secondEntry =
            manager.AppendCommand(
                commands[1],
                term: 3);

        Assert.Equal(
            1,
            firstEntry.LogIndex);

        Assert.Equal(
            2,
            secondEntry.LogIndex);

        Assert.Equal(
            3,
            firstEntry.Term);

        Assert.Equal(
            commands[0].CommandId,
            firstEntry.CommandId);

        Assert.Equal(
            nameof(CreateExperimentCommand),
            firstEntry.CommandType);

        Assert.Equal(
            new RaftLogPosition(
                LogIndex: 2,
                Term: 3),
            manager.GetLastPosition());
    }

    [Fact]
    public void LogManager_DeserializesStoredCommand()
    {
        using var factory =
            new CoordinatorWebApplicationFactory();

        using var client =
            factory.CreateClient();

        var manager =
            factory.Services
                .GetRequiredService<
                    RaftLogManager>();

        var originalCommand =
            CreateSupportedCommands()[0];

        var entry =
            manager.AppendCommand(
                originalCommand,
                term: 1);

        var restoredCommand =
            manager.DeserializeCommand(entry);

        Assert.Equal(
            originalCommand,
            restoredCommand);
    }

    private static IReadOnlyList<
        CoordinatorCommand>
        CreateSupportedCommands()
    {
        var occurredAtUtc =
            new DateTimeOffset(
                2026,
                7,
                3,
                12,
                0,
                0,
                TimeSpan.Zero);

        var experimentId =
            Guid.Parse(
                "11111111-1111-1111-1111-111111111111");

        return
        [
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: occurredAtUtc,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Raft test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 10_000,
                Priority: 2,
                TimeoutSeconds: 300,
                SimulateFailure: false),

            new AssignExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: occurredAtUtc,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: "worker-1",
                Attempt: 1),

            new CompleteExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: occurredAtUtc,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: "worker-1",
                Attempt: 1,
                Succeeded: true,
                WasCancelled: false,
                ResultMessage: "Completed",
                MetricsJson: "{\"reward\":42}",
                ExecutionDurationMs: 1500),

            new RequestExperimentCancellationCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: occurredAtUtc,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid()),

            new RequeueExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: occurredAtUtc,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: "worker-1",
                Attempt: 1,
                CancelInsteadOfRequeue: false),

            new RecoverExperimentOnStartupCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: occurredAtUtc,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                PreviousWorkerId: "worker-1",
                Attempt: 1)
        ];
    }

    private sealed record UnsupportedCommand(
        Guid CommandId,
        DateTimeOffset OccurredAtUtc)
        : CoordinatorCommand(
            CommandId,
            OccurredAtUtc);
}