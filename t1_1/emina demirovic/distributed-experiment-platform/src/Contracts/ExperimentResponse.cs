namespace Contracts;

public sealed class ExperimentResponse
{
    public Guid Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public ExperimentStatus Status { get; set; }

    public DateTimeOffset CreatedAtUtc { get; set; }

    public string? AssignedWorkerId { get; set; }

    public DateTimeOffset? FinishedAtUtc { get; set; }

    public string? ResultMessage { get; set; }

    public bool SimulateFailure { get; set; }

    public int Attempt { get; set; }

    public string Algorithm { get; set; } = string.Empty;

    public string Environment { get; set; } = string.Empty;

    public int Seed { get; set; }

    public int MaxSteps { get; set; }

    public int Priority { get; set; }

    public string? MetricsJson { get; set; }

    public long? ExecutionDurationMs { get; set; }

    public int? CurrentStep { get; set; }

    public string? ProgressMetricsJson { get; set; }

    public DateTimeOffset? LastProgressAtUtc { get; set; }

    public bool CancellationRequested { get; set; }

    public int TimeoutSeconds { get; set; } = 300;
    
}