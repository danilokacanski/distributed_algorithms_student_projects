namespace Coordinator.Data;

public enum ExperimentEventType
{
    Created,
    Assigned,
    Completed,
    Failed,
    Requeued,
    RecoveredOnStartup,
    CancelRequested,
    Cancelled
}