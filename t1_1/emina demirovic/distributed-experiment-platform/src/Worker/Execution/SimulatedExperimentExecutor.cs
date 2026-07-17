using Contracts;
using System.Diagnostics;
using System.Text.Json;

namespace Worker.Execution;

public sealed class SimulatedExperimentExecutor(
    IConfiguration configuration) : IExperimentExecutor
{
    private readonly TimeSpan _executionDuration =
        TimeSpan.FromSeconds(
            ReadPositiveInt(
                configuration,
                "Worker:SimulatedExecutionSeconds",
                3));

    public async Task<ExperimentExecutionResult> ExecuteAsync(
        ExperimentResponse experiment,
        Func<
            ExperimentProgressUpdate,
            CancellationToken,
            Task> reportProgressAsync,
        CancellationToken cancellationToken)
    {
        const int progressUpdateCount = 5;

        var stopwatch = Stopwatch.StartNew();

        var progressInterval = TimeSpan.FromTicks(
            Math.Max(
                1,
                _executionDuration.Ticks /
                progressUpdateCount));

        for (var updateIndex = 1;
            updateIndex <= progressUpdateCount;
            updateIndex++)
        {
            await Task.Delay(
                progressInterval,
                cancellationToken);

            var currentStep = (int)Math.Min(
                experiment.MaxSteps,
                (long)experiment.MaxSteps *
                updateIndex /
                progressUpdateCount);

            var progressMetricsJson =
                JsonSerializer.Serialize(new
                {
                    meanReward = 5.0 * updateIndex,
                    episodes = updateIndex
                });

            await reportProgressAsync(
                new ExperimentProgressUpdate(
                    currentStep,
                    progressMetricsJson),
                cancellationToken);
        }

        stopwatch.Stop();

        if (experiment.SimulateFailure)
        {
            return new ExperimentExecutionResult(
                false,
                "Simulated execution failure.",
                null,
                stopwatch.ElapsedMilliseconds);
        }

        var metricsJson = JsonSerializer.Serialize(new
        {
            totalReward = 125.5,
            meanReward = 25.1,
            episodes = 5,
            maxSteps = experiment.MaxSteps
        });

        return new ExperimentExecutionResult(
            true,
            "Simulated execution completed successfully.",
            metricsJson,
            stopwatch.ElapsedMilliseconds);
    }

    private static int ReadPositiveInt(
        IConfiguration configuration,
        string key,
        int defaultValue)
    {
        return int.TryParse(configuration[key], out var value) &&
               value > 0
            ? value
            : defaultValue;
    }
}