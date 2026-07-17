namespace Contracts;

public sealed class CreateExperimentRequest
{
    public string Name { get; set; } = string.Empty;
    public bool SimulateFailure { get; set; }

    public string Algorithm { get; set; } = string.Empty;

    public string Environment { get; set; } = string.Empty;

    public int Seed { get; set; }

    public int MaxSteps { get; set; }

    public int Priority { get; set; }

    public int TimeoutSeconds { get; set; } = 300;
}