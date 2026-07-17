namespace Worker.Execution;

public sealed record ExperimentProgressUpdate(
    int CurrentStep,
    string? MetricsJson);
