namespace Worker.Execution;

public sealed record ExperimentExecutionResult(
    bool Succeeded,
    string ResultMessage,
    string? MetricsJson,
    long ExecutionDurationMs);