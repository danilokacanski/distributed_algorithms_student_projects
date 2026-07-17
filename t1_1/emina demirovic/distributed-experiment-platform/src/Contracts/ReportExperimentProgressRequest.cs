namespace Contracts;

public sealed class ReportExperimentProgressRequest
{
    public string WorkerId { get; set; } = string.Empty;

    public int Attempt { get; set; }

    public int CurrentStep { get; set; }

    public string? ProgressMetricsJson { get; set; }
}