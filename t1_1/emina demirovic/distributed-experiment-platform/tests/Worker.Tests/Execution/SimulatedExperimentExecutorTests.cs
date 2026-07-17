using System.Text.Json;
using Contracts;
using Microsoft.Extensions.Configuration;
using Worker.Execution;

namespace Worker.Tests.Execution;

public sealed class SimulatedExperimentExecutorTests
{
    [Fact]
    public async Task ExecuteAsync_Success_ReturnsMetrics()
    {
        var executor = CreateExecutor();

        var experiment = CreateExperiment(
            simulateFailure: false);

        var progressUpdates =
            new List<ExperimentProgressUpdate>();
            
        var result = await executor.ExecuteAsync(
            experiment,
            (progress, _) =>
            {
                progressUpdates.Add(progress);
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.True(result.Succeeded);

        Assert.Equal(
            "Simulated execution completed successfully.",
            result.ResultMessage);

        Assert.NotNull(result.MetricsJson);
        Assert.True(result.ExecutionDurationMs > 0);

        using var metricsDocument =
            JsonDocument.Parse(result.MetricsJson);

        var metrics = metricsDocument.RootElement;

        Assert.Equal(5, progressUpdates.Count);

        Assert.Equal(
            experiment.MaxSteps,
            progressUpdates[^1].CurrentStep);

        Assert.NotNull(
            progressUpdates[^1].MetricsJson);

        Assert.True(
            progressUpdates
                .Select(progress => progress.CurrentStep)
                .SequenceEqual(
                    progressUpdates
                        .Select(progress => progress.CurrentStep)
                        .OrderBy(step => step)));

        Assert.Equal(
            125.5,
            metrics.GetProperty("totalReward").GetDouble());

        Assert.Equal(
            25.1,
            metrics.GetProperty("meanReward").GetDouble());

        Assert.Equal(
            5,
            metrics.GetProperty("episodes").GetInt32());

        Assert.Equal(
            experiment.MaxSteps,
            metrics.GetProperty("maxSteps").GetInt32());
    }

    [Fact]
    public async Task ExecuteAsync_SimulatedFailure_ReturnsFailedResult()
    {
        var executor = CreateExecutor();

        var experiment = CreateExperiment(
            simulateFailure: true);

        var result = await executor.ExecuteAsync(
            experiment,
            (_, _) => Task.CompletedTask,
            CancellationToken.None);

        Assert.False(result.Succeeded);

        Assert.Equal(
            "Simulated execution failure.",
            result.ResultMessage);

        Assert.Null(result.MetricsJson);
        Assert.True(result.ExecutionDurationMs > 0);
    }

    [Fact]
    public async Task ExecuteAsync_CancellationRequested_ThrowsCancellation()
    {
        var executor = CreateExecutor();

        var experiment = CreateExperiment(
            simulateFailure: false);

        using var cancellation =
            new CancellationTokenSource(
                TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => executor.ExecuteAsync(
            experiment,
            (_, _) => Task.CompletedTask,
            cancellation.Token));
    }

    private static SimulatedExperimentExecutor CreateExecutor()
    {
        var configuration =
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                    new Dictionary<string, string?>
                    {
                        ["Worker:SimulatedExecutionSeconds"] = "1"
                    })
                .Build();

        return new SimulatedExperimentExecutor(
            configuration);
    }

    private static ExperimentResponse CreateExperiment(
        bool simulateFailure)
    {
        return new ExperimentResponse
        {
            Id = Guid.NewGuid(),
            Name = "Simulated executor test",
            Status = ExperimentStatus.Running,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            AssignedWorkerId = "worker-executor-test",
            SimulateFailure = simulateFailure,
            Attempt = 1,
            Algorithm = "PPO",
            Environment = "CartPole-v1",
            Seed = 42,
            MaxSteps = 10_000,
            Priority = 5,
            TimeoutSeconds = 30
        };
    }
}