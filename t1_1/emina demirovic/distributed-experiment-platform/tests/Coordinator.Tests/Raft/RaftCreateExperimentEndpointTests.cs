using System.Net;
using System.Net.Http.Json;
using Contracts;
using Coordinator.Raft;
using Coordinator.Commands;
using Coordinator.Services;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Coordinator.Tests.Raft;

public sealed class RaftCreateExperimentEndpointTests
{
    [Fact]
    public async Task CreateExperiment_WithRaftEnabled_UsesLeaderSubmission()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var nodeState =
            factory.Services
                .GetRequiredService<
                    RaftNodeState>();

        var election =
            nodeState.BeginElection();

        Assert.True(
            nodeState.TryBecomeLeader(
                election.CurrentTerm));

        var response =
            await client.PostAsJsonAsync(
                "/api/experiments",
                new
                {
                    name = "Raft API experiment",
                    algorithm = "PPO",
                    environment = "CartPole-v1",
                    seed = 42,
                    maxSteps = 1000,
                    priority = 1,
                    timeoutSeconds = 300,
                    simulateFailure = false
                });

        Assert.Equal(
            HttpStatusCode.Created,
            response.StatusCode);

        var experiment =
            await response.Content
                .ReadFromJsonAsync<
                    ExperimentResponse>();

        Assert.NotNull(experiment);

        Assert.Equal(
            "Raft API experiment",
            experiment.Name);

        var commitState =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>()
                .GetState();

        Assert.Equal(1, commitState.CommitIndex);
        Assert.Equal(1, commitState.LastApplied);
    }

    [Fact]
    public async Task CreateExperiment_WithRaftEnabledOnFollower_ReturnsConflict()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var response =
            await client.PostAsJsonAsync(
                "/api/experiments",
                new
                {
                    name = "Follower rejected experiment",
                    algorithm = "PPO",
                    environment = "CartPole-v1",
                    seed = 42,
                    maxSteps = 1000,
                    priority = 1,
                    timeoutSeconds = 300,
                    simulateFailure = false
                });

        Assert.Equal(
            HttpStatusCode.Conflict,
            response.StatusCode);

        var logStore =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Null(
            logStore.GetLast());
    }

    [Fact]
    public async Task CancelExperiment_WithRaftEnabled_UsesLeaderSubmission()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var nodeState =
            factory.Services
                .GetRequiredService<
                    RaftNodeState>();

        var election =
            nodeState.BeginElection();

        Assert.True(
            nodeState.TryBecomeLeader(
                election.CurrentTerm));

        var createResponse =
            await client.PostAsJsonAsync(
                "/api/experiments",
                new
                {
                    name = "Raft cancellable experiment",
                    algorithm = "PPO",
                    environment = "CartPole-v1",
                    seed = 42,
                    maxSteps = 1000,
                    priority = 1,
                    timeoutSeconds = 300,
                    simulateFailure = false
                });

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createdExperiment =
            await createResponse.Content
                .ReadFromJsonAsync<
                    ExperimentResponse>();

        Assert.NotNull(createdExperiment);

        var cancelResponse =
            await client.PostAsync(
                $"/api/experiments/{createdExperiment.Id}/cancel",
                content: null);

        Assert.Equal(
            HttpStatusCode.OK,
            cancelResponse.StatusCode);

        var cancelledExperiment =
            await cancelResponse.Content
                .ReadFromJsonAsync<
                    ExperimentResponse>();

        Assert.NotNull(cancelledExperiment);

        Assert.True(
            cancelledExperiment.CancellationRequested ||
            cancelledExperiment.Status ==
                ExperimentStatus.Cancelled);

        var commitState =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>()
                .GetState();

        Assert.Equal(2, commitState.CommitIndex);
        Assert.Equal(2, commitState.LastApplied);
    }

    [Fact]
    public async Task CancelExperiment_WithRaftEnabledOnFollower_ReturnsConflict()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var experimentId =
            Guid.NewGuid();

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Follower cancel test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        var response =
            await client.PostAsync(
                $"/api/experiments/{experimentId}/cancel",
                content: null);

        Assert.Equal(
            HttpStatusCode.Conflict,
            response.StatusCode);

        var logStore =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Null(
            logStore.GetLast());
    }

    [Fact]
    public async Task CompleteExperiment_WithRaftEnabled_UsesLeaderSubmission()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var nodeState =
            factory.Services
                .GetRequiredService<
                    RaftNodeState>();

        var election =
            nodeState.BeginElection();

        Assert.True(
            nodeState.TryBecomeLeader(
                election.CurrentTerm));

        var experimentId =
            Guid.NewGuid();

        var workerId =
            "worker-1";

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Raft complete test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        commandProcessor.Apply(
            new AssignExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: workerId,
                Attempt: 1));

        var response =
            await client.PostAsJsonAsync(
                $"/api/experiments/{experimentId}/complete",
                new
                {
                    workerId,
                    attempt = 1,
                    succeeded = true,
                    wasCancelled = false,
                    resultMessage = "Completed through Raft",
                    metricsJson = "{\"reward\":42}",
                    executionDurationMs = 1500
                });

        Assert.Equal(
            HttpStatusCode.OK,
            response.StatusCode);

        var completedExperiment =
            await response.Content
                .ReadFromJsonAsync<
                    ExperimentResponse>();

        Assert.NotNull(completedExperiment);

        Assert.Equal(
            ExperimentStatus.Completed,
            completedExperiment.Status);

        Assert.Equal(
            "Completed through Raft",
            completedExperiment.ResultMessage);

        var commitState =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>()
                .GetState();

        Assert.Equal(1, commitState.CommitIndex);
        Assert.Equal(1, commitState.LastApplied);
    }

    [Fact]
    public async Task CompleteExperiment_WithRaftEnabledOnFollower_ReturnsConflict()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var experimentId =
            Guid.NewGuid();

        var workerId =
            "worker-1";

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Follower complete test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        commandProcessor.Apply(
            new AssignExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: workerId,
                Attempt: 1));

        var response =
            await client.PostAsJsonAsync(
                $"/api/experiments/{experimentId}/complete",
                new
                {
                    workerId,
                    attempt = 1,
                    succeeded = true,
                    wasCancelled = false,
                    resultMessage = "Should be rejected",
                    metricsJson = "{\"reward\":42}",
                    executionDurationMs = 1500
                });

        Assert.Equal(
            HttpStatusCode.Conflict,
            response.StatusCode);

        var logStore =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Null(
            logStore.GetLast());
    }

    [Fact]
    public async Task AssignExperiment_WithRaftEnabled_UsesLeaderSubmission()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var nodeState =
            factory.Services
                .GetRequiredService<
                    RaftNodeState>();

        var election =
            nodeState.BeginElection();

        Assert.True(
            nodeState.TryBecomeLeader(
                election.CurrentTerm));

        var createResponse =
            await client.PostAsJsonAsync(
                "/api/experiments",
                new
                {
                    name = "Raft assign experiment",
                    algorithm = "PPO",
                    environment = "CartPole-v1",
                    seed = 42,
                    maxSteps = 1000,
                    priority = 1,
                    timeoutSeconds = 300,
                    simulateFailure = false
                });

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createdExperiment =
            await createResponse.Content
                .ReadFromJsonAsync<
                    ExperimentResponse>();

        Assert.NotNull(createdExperiment);

        var workerRegistry =
            factory.Services
                .GetRequiredService<
                    WorkerRegistry>();

        workerRegistry.Register("worker-1");

        var assignResponse =
            await client.PostAsync(
                $"/api/experiments/{createdExperiment.Id}/assign",
                content: null);

        Assert.Equal(
            HttpStatusCode.OK,
            assignResponse.StatusCode);

        var assignedExperiment =
            await assignResponse.Content
                .ReadFromJsonAsync<
                    ExperimentResponse>();

        Assert.NotNull(assignedExperiment);

        Assert.Equal(
            ExperimentStatus.Running,
            assignedExperiment.Status);

        Assert.Equal(
            "worker-1",
            assignedExperiment.AssignedWorkerId);

        var commitState =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>()
                .GetState();

        Assert.Equal(2, commitState.CommitIndex);
        Assert.Equal(2, commitState.LastApplied);
    }

    [Fact]
    public async Task AssignExperiment_WithRaftEnabledOnFollower_ReturnsConflict()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var experimentId =
            Guid.NewGuid();

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Follower assign test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        var workerRegistry =
            factory.Services
                .GetRequiredService<
                    WorkerRegistry>();

        workerRegistry.Register("worker-1");

        var response =
            await client.PostAsync(
                $"/api/experiments/{experimentId}/assign",
                content: null);

        Assert.Equal(
            HttpStatusCode.Conflict,
            response.StatusCode);

        var logStore =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Null(
            logStore.GetLast());
    }

    [Fact]
    public async Task Scheduler_WithRaftEnabledOnLeader_AssignsThroughRaft()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var nodeState =
            factory.Services
                .GetRequiredService<
                    RaftNodeState>();

        var election =
            nodeState.BeginElection();

        Assert.True(
            nodeState.TryBecomeLeader(
                election.CurrentTerm));

        var createResponse =
            await client.PostAsJsonAsync(
                "/api/experiments",
                new
                {
                    name = "Raft scheduled experiment",
                    algorithm = "PPO",
                    environment = "CartPole-v1",
                    seed = 42,
                    maxSteps = 1000,
                    priority = 1,
                    timeoutSeconds = 300,
                    simulateFailure = false
                });

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createdExperiment =
            await createResponse.Content
                .ReadFromJsonAsync<
                    ExperimentResponse>();

        Assert.NotNull(createdExperiment);

        var workerRegistry =
            factory.Services
                .GetRequiredService<
                    WorkerRegistry>();

        workerRegistry.Register("worker-1");

        var experimentRegistry =
            factory.Services
                .GetRequiredService<
                    ExperimentRegistry>();

        var scheduler =
            factory.Services
                .GetRequiredService<
                    ExperimentSchedulerService>();

        await scheduler.RunSchedulingCycleAsync(
            CancellationToken.None);

        var assignedExperiment =
            experimentRegistry.GetById(
                createdExperiment.Id);

        Assert.NotNull(assignedExperiment);

        Assert.Equal(
            "worker-1",
            assignedExperiment.AssignedWorkerId);

        var commitState =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>()
                .GetState();

        Assert.Equal(2, commitState.CommitIndex);
        Assert.Equal(2, commitState.LastApplied);
    } 

    [Fact]
    public async Task Scheduler_WithRaftEnabledOnFollower_DoesNotAssign()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var experimentId =
            Guid.NewGuid();

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Follower scheduler test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        var workerRegistry =
            factory.Services
                .GetRequiredService<
                    WorkerRegistry>();

        workerRegistry.Register("worker-1");

        var scheduler =
            factory.Services
                .GetRequiredService<
                    ExperimentSchedulerService>();

        await scheduler.RunSchedulingCycleAsync(
            CancellationToken.None);

        var experimentRegistry =
            factory.Services
                .GetRequiredService<
                    ExperimentRegistry>();

        var experiment =
            experimentRegistry.GetById(
                experimentId);

        Assert.NotNull(experiment);

        Assert.Equal(
            ExperimentStatus.Pending,
            experiment.Status);

        Assert.Null(
            experiment.AssignedWorkerId);

        var logStore =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Null(
            logStore.GetLast());
    }

    [Fact]
    public async Task Recovery_WithRaftEnabledOnLeader_RequeuesThroughRaft()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var nodeState =
            factory.Services
                .GetRequiredService<
                    RaftNodeState>();

        var election =
            nodeState.BeginElection();

        Assert.True(
            nodeState.TryBecomeLeader(
                election.CurrentTerm));

        var experimentId =
            Guid.NewGuid();

        var workerId =
            "offline-worker-1";

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Raft recovery test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        commandProcessor.Apply(
            new AssignExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: workerId,
                Attempt: 1));

        var recovery =
            factory.Services
                .GetRequiredService<
                    ExperimentRecoveryService>();

        await recovery.RunRecoveryCycleAsync(
            CancellationToken.None);

        var experimentRegistry =
            factory.Services
                .GetRequiredService<
                    ExperimentRegistry>();

        var recoveredExperiment =
            experimentRegistry.GetById(
                experimentId);

        Assert.NotNull(recoveredExperiment);

        Assert.Equal(
            ExperimentStatus.Pending,
            recoveredExperiment.Status);

        Assert.Null(
            recoveredExperiment.AssignedWorkerId);

        var commitState =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>()
                .GetState();

        Assert.Equal(1, commitState.CommitIndex);
        Assert.Equal(1, commitState.LastApplied);
    }

    [Fact]
    public async Task Recovery_WithRaftEnabledOnFollower_DoesNotRequeue()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var experimentId =
            Guid.NewGuid();

        var workerId =
            "offline-worker-1";

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Follower recovery test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        commandProcessor.Apply(
            new AssignExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: workerId,
                Attempt: 1));

        var recovery =
            factory.Services
                .GetRequiredService<
                    ExperimentRecoveryService>();

        await recovery.RunRecoveryCycleAsync(
            CancellationToken.None);

        var experimentRegistry =
            factory.Services
                .GetRequiredService<
                    ExperimentRegistry>();

        var experiment =
            experimentRegistry.GetById(
                experimentId);

        Assert.NotNull(experiment);

        Assert.Equal(
            ExperimentStatus.Running,
            experiment.Status);

        Assert.Equal(
            workerId,
            experiment.AssignedWorkerId);

        var logStore =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Null(
            logStore.GetLast());
    }

    [Fact]
    public async Task StartupRecovery_WithRaftEnabledOnLeader_RequeuesThroughRaft()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var nodeState =
            factory.Services
                .GetRequiredService<
                    RaftNodeState>();

        var election =
            nodeState.BeginElection();

        Assert.True(
            nodeState.TryBecomeLeader(
                election.CurrentTerm));

        var experimentId =
            Guid.NewGuid();

        var workerId =
            "startup-worker-1";

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Raft startup recovery test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        commandProcessor.Apply(
            new AssignExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: workerId,
                Attempt: 1));

        factory.Services
            .GetRequiredService<
                WorkerRegistry>()
            .Register(workerId);

        var startupRecovery =
            factory.Services
                .GetRequiredService<
                    ExperimentStartupRecoveryService>();

        var recoveredCount =
            await startupRecovery.RunStartupRecoveryCycleAsync(
                CancellationToken.None);

        Assert.Equal(1, recoveredCount);

        var experimentRegistry =
            factory.Services
                .GetRequiredService<
                    ExperimentRegistry>();

        var recoveredExperiment =
            experimentRegistry.GetById(
                experimentId);

        Assert.NotNull(recoveredExperiment);

        Assert.Equal(
            ExperimentStatus.Pending,
            recoveredExperiment.Status);

        Assert.Null(
            recoveredExperiment.AssignedWorkerId);

        var commitState =
            factory.Services
                .GetRequiredService<
                    RaftCommitManager>()
                .GetState();

        Assert.Equal(1, commitState.CommitIndex);
        Assert.Equal(1, commitState.LastApplied);
    }

    [Fact]
    public async Task StartupRecovery_WithRaftEnabledOnFollower_DoesNotRequeue()
    {
        using var factory =
            CreateRaftEnabledFactory();

        using var client =
            factory.CreateClient();

        var experimentId =
            Guid.NewGuid();

        var workerId =
            "startup-worker-1";

        var commandProcessor =
            factory.Services
                .GetRequiredService<
                    CoordinatorCommandProcessor>();

        commandProcessor.Apply(
            new CreateExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                Name: "Follower startup recovery test",
                Algorithm: "PPO",
                Environment: "CartPole-v1",
                Seed: 42,
                MaxSteps: 1000,
                Priority: 1,
                TimeoutSeconds: 300,
                SimulateFailure: false));

        commandProcessor.Apply(
            new AssignExperimentCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc:
                    DateTimeOffset.UtcNow,
                ExperimentId: experimentId,
                EventId: Guid.NewGuid(),
                WorkerId: workerId,
                Attempt: 1));

        var startupRecovery =
            factory.Services
                .GetRequiredService<
                    ExperimentStartupRecoveryService>();

        var recoveredCount =
            await startupRecovery.RunStartupRecoveryCycleAsync(
                CancellationToken.None);

        Assert.Equal(0, recoveredCount);

        var experimentRegistry =
            factory.Services
                .GetRequiredService<
                    ExperimentRegistry>();

        var experiment =
            experimentRegistry.GetById(
                experimentId);

        Assert.NotNull(experiment);

        Assert.Equal(
            ExperimentStatus.Running,
            experiment.Status);

        Assert.Equal(
            workerId,
            experiment.AssignedWorkerId);

        var logStore =
            factory.Services
                .GetRequiredService<
                    IRaftLogStore>();

        Assert.Null(
            logStore.GetLast());
    }

    private static WebApplicationFactory<Program>
        CreateRaftEnabledFactory()
    {
        return new CoordinatorWebApplicationFactory()
            .WithWebHostBuilder(
                builder =>
                {
                    builder.ConfigureAppConfiguration(
                        (_, configuration) =>
                        {
                            configuration.AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    ["Raft:ClientCommandReplicationEnabled"] =
                                        "true",
                                    ["Raft:CommandReplicationTimeoutMilliseconds"] =
                                        "500",
                                    ["Raft:CommandReplicationPollMilliseconds"] =
                                        "10"
                                });
                        });

                    builder.ConfigureServices(
                        services =>
                        {
                            services.RemoveAll<
                                IRaftPeerClient>();

                            services.AddSingleton<
                                IRaftPeerClient,
                                SuccessfulPeerClient>();
                        });
                });
    }

    private sealed class SuccessfulPeerClient
        : IRaftPeerClient
    {
        public Task<RequestVoteResponse>
            RequestVoteAsync(
                RaftPeerSnapshot peer,
                RequestVoteRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new RequestVoteResponse(
                    request.Term,
                    VoteGranted: true));
        }

        public Task<AppendEntriesResponse>
            AppendEntriesAsync(
                RaftPeerSnapshot peer,
                AppendEntriesRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new AppendEntriesResponse(
                    request.Term,
                    Success: true));
        }
    }
}