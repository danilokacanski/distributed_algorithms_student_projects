using Contracts;
using Microsoft.Extensions.Configuration;
using Worker.Execution;

namespace Worker.Tests.Execution;

public sealed class PythonProcessExperimentExecutorTests
{
    [Fact]
    public async Task ExecuteAsync_MissingScript_ReturnsFailedResult()
    {
        var missingScriptPath = Path.Combine(
            Path.GetTempPath(),
            "distributed-experiment-platform-tests",
            Guid.NewGuid().ToString("N"),
            "missing-script.py");

        var configuration =
            new ConfigurationBuilder()
                .AddInMemoryCollection(
                    new Dictionary<string, string?>
                    {
                        ["Worker:Executor:PythonCommand"] = "py",
                        ["Worker:Executor:ScriptPath"] =
                            missingScriptPath
                    })
                .Build();

        var executor =
            new PythonProcessExperimentExecutor(
                configuration);

        var experiment = new ExperimentResponse
        {
            Id = Guid.NewGuid(),
            Name = "Missing Python script test",
            Status = ExperimentStatus.Running,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            AssignedWorkerId = "worker-python-test",
            SimulateFailure = false,
            Attempt = 1,
            Algorithm = "PPO",
            Environment = "CartPole-v1",
            Seed = 42,
            MaxSteps = 10_000,
            Priority = 5,
            TimeoutSeconds = 30
        };

        var result = await executor.ExecuteAsync(
            experiment,
            (_, _) => Task.CompletedTask,
            CancellationToken.None);
            
        Assert.False(result.Succeeded);

        Assert.Contains(
            "Python script was not found",
            result.ResultMessage);

        Assert.Contains(
            missingScriptPath,
            result.ResultMessage);

        Assert.Null(result.MetricsJson);
        Assert.True(result.ExecutionDurationMs >= 0);
    }
}