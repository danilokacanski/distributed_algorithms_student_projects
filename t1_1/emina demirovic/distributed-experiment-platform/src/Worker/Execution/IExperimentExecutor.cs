using Contracts;

namespace Worker.Execution;

public interface IExperimentExecutor
{
    Task<ExperimentExecutionResult> ExecuteAsync(
        ExperimentResponse experiment,
        Func<
            ExperimentProgressUpdate,
            CancellationToken,
            Task> reportProgressAsync,
        CancellationToken cancellationToken);
}