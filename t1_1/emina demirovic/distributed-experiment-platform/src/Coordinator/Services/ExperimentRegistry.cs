using Contracts;
using Coordinator.Data;
using Microsoft.EntityFrameworkCore;

namespace Coordinator.Services;

public sealed class ExperimentRegistry(
    IDbContextFactory<CoordinatorDbContext> dbContextFactory)
{

    
    public ExperimentResponse Create(
        string name,
        string algorithm,
        string environment,
        int seed,
        int maxSteps,
        int priority,
        int timeoutSeconds,
        bool simulateFailure = false)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        var experiment = new ExperimentEntity
        {
            Id = Guid.NewGuid(),
            Name = name,
            Algorithm = algorithm,
            Environment = environment,
            Seed = seed,
            MaxSteps = maxSteps,
            Priority = priority,
            SimulateFailure = simulateFailure,
            TimeoutSeconds = timeoutSeconds,
            Status = ExperimentStatus.Pending,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            AssignedWorkerId = null,
            FinishedAtUtc = null,
            ResultMessage = null,
            MetricsJson = null,
            ExecutionDurationMs = null,
            CurrentStep = null,
            ProgressMetricsJson = null,
            LastProgressAtUtc = null,
            CancellationRequested = false,
            Attempt = 0
        };

        dbContext.Experiments.Add(experiment);

        var createdEvent = new ExperimentEventEntity
        {
            Id = Guid.NewGuid(),
            ExperimentId = experiment.Id,
            Type = ExperimentEventType.Created,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            WorkerId = null,
            Attempt = 0,
            Details =
                $"Experiment '{experiment.Name}' was created " +
                $"for algorithm '{experiment.Algorithm}' and " +
                $"environment '{experiment.Environment}'."
        };

        dbContext.ExperimentEvents.Add(createdEvent);

        dbContext.SaveChanges();

        return ToResponse(experiment);
    }

    public IReadOnlyCollection<ExperimentResponse> GetAll()
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        return dbContext.Experiments
            .AsNoTracking()
            .AsEnumerable()
            .OrderBy(experiment => experiment.CreatedAtUtc)
            .Select(ToResponse)
            .ToArray();
    }

    public ExperimentResponse? GetById(Guid id)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(experiment =>
                experiment.Id == id);

        return experiment is null
            ? null
            : ToResponse(experiment);
    }

    public bool TryAssign(
        Guid id,
        string workerId,
        out ExperimentResponse? assignedExperiment)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var updatedRows = dbContext.Experiments
            .Where(experiment =>
                experiment.Id == id &&
                experiment.Status == ExperimentStatus.Pending)
            .ExecuteUpdate(setters => setters
                .SetProperty(
                    experiment => experiment.Status,
                    ExperimentStatus.Running)
                .SetProperty(
                    experiment => experiment.AssignedWorkerId,
                    workerId)
                .SetProperty(
                    experiment => experiment.FinishedAtUtc,
                    (DateTimeOffset?)null)
                .SetProperty(
                    experiment => experiment.ResultMessage,
                    (string?)null)
                .SetProperty(
                    experiment => experiment.Attempt,
                    experiment => experiment.Attempt + 1)
                .SetProperty(
                    experiment => experiment.MetricsJson,
                    (string?)null)
                .SetProperty(
                    experiment => experiment.ExecutionDurationMs,
                    (long?)null)
                .SetProperty(
                    experiment => experiment.CurrentStep,
                    (int?)null)
                .SetProperty(
                    experiment => experiment.ProgressMetricsJson,
                    (string?)null)
                .SetProperty(
                    experiment => experiment.LastProgressAtUtc,
                    (DateTimeOffset?)null)
                .SetProperty(
                    experiment => experiment.CancellationRequested,
                    false)
                );

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(experiment =>
                experiment.Id == id);

        assignedExperiment = experiment is null
            ? null
            : ToResponse(experiment);

        if (updatedRows != 1 || experiment is null)
        {
            return false;
        }

        dbContext.ExperimentEvents.Add(
            CreateEvent(
                experiment.Id,
                ExperimentEventType.Assigned,
                workerId,
                experiment.Attempt,
                $"Experiment was assigned to worker '{workerId}'."));

        dbContext.SaveChanges();
        transaction.Commit();

        return true;

    }

    public ExperimentResponse? GetNextAssignedToWorker(
        string workerId)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .Where(experiment =>
                experiment.AssignedWorkerId == workerId &&
                experiment.Status == ExperimentStatus.Running &&
                !experiment.CancellationRequested)
            .AsEnumerable()
            .OrderBy(experiment => experiment.CreatedAtUtc)
            .FirstOrDefault();

        return experiment is null
            ? null
            : ToResponse(experiment);
    }

    public bool TryReportProgress(
        Guid id,
        string workerId,
        int attempt,
        int currentStep,
        string? progressMetricsJson,
        out ExperimentResponse? updatedExperiment)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        var lastProgressAtUtc =
            DateTimeOffset.UtcNow;

        var updatedRows = dbContext.Experiments
            .Where(experiment =>
                experiment.Id == id &&
                experiment.Status == ExperimentStatus.Running &&
                experiment.AssignedWorkerId == workerId &&
                experiment.Attempt == attempt &&
                !experiment.CancellationRequested &&
                (experiment.CurrentStep == null ||
                experiment.CurrentStep <= currentStep))
            .ExecuteUpdate(setters => setters
                .SetProperty(
                    experiment => experiment.CurrentStep,
                    (int?)currentStep)
                .SetProperty(
                    experiment => experiment.ProgressMetricsJson,
                    progressMetricsJson)
                .SetProperty(
                    experiment => experiment.LastProgressAtUtc,
                    lastProgressAtUtc));

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(experiment =>
                experiment.Id == id);

        updatedExperiment =
            experiment is null
                ? null
                : ToResponse(experiment);

        return updatedRows == 1 &&
            experiment is not null;
    }
    
    public bool TryComplete(
        Guid id,
        string workerId,
        int attempt,
        bool succeeded,
        bool wasCancelled,
        string? resultMessage,
        out ExperimentResponse? finishedExperiment,
        string? metricsJson,
        long? executionDurationMs)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var finalStatus = wasCancelled
            ? ExperimentStatus.Cancelled
            : succeeded
                ? ExperimentStatus.Completed
                : ExperimentStatus.Failed;

        var finishedAtUtc = DateTimeOffset.UtcNow;

        var updatedRows = dbContext.Experiments
            .Where(experiment =>
                experiment.Id == id &&
                experiment.Status == ExperimentStatus.Running &&
                experiment.AssignedWorkerId == workerId &&
                experiment.Attempt == attempt &&
                experiment.CancellationRequested == wasCancelled)
            .ExecuteUpdate(setters => setters
                .SetProperty(
                    experiment => experiment.Status,
                    finalStatus)
                .SetProperty(
                    experiment => experiment.FinishedAtUtc,
                    finishedAtUtc)
                .SetProperty(
                    experiment => experiment.ResultMessage,
                    resultMessage)
                .SetProperty(
                    experiment => experiment.MetricsJson,
                    metricsJson)
                .SetProperty(
                    experiment => experiment.ExecutionDurationMs,
                    executionDurationMs)
                .SetProperty(
                    experiment => experiment.CancellationRequested,
                    wasCancelled)
                );

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(experiment =>
                experiment.Id == id);

        finishedExperiment = experiment is null
            ? null
            : ToResponse(experiment);

        if (updatedRows != 1 || experiment is null)
        {
            return false;
        }

        var eventType = wasCancelled
            ? ExperimentEventType.Cancelled
            : succeeded
                ? ExperimentEventType.Completed
                : ExperimentEventType.Failed;

        var details = wasCancelled
            ? "Experiment execution was cancelled."
            : succeeded
                ? "Experiment completed successfully."
                : $"Experiment failed. {resultMessage}";

        dbContext.ExperimentEvents.Add(
            CreateEvent(
                experiment.Id,
                eventType,
                workerId,
                attempt,
                details));

        dbContext.SaveChanges();
        transaction.Commit();

        return true;
    } 

    public ExperimentResponse? GetNextPending()
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .Where(experiment =>
                experiment.Status == ExperimentStatus.Pending)
            .AsEnumerable()
            .OrderByDescending(experiment =>
                experiment.Priority)
            .ThenBy(experiment =>
                experiment.CreatedAtUtc)
            .FirstOrDefault();

        return experiment is null
            ? null
            : ToResponse(experiment);
    }

    public bool HasRunningExperiment(string workerId)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        return dbContext.Experiments.Any(experiment =>
            experiment.Status == ExperimentStatus.Running &&
            experiment.AssignedWorkerId == workerId);
    }

    public IReadOnlyCollection<ExperimentResponse> GetRunning()
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        return dbContext.Experiments
            .AsNoTracking()
            .Where(experiment =>
                experiment.Status == ExperimentStatus.Running)
            .AsEnumerable()
            .OrderBy(experiment => experiment.CreatedAtUtc)
            .Select(ToResponse)
            .ToArray();
    }

    public bool TryRequeue(
        Guid id,
        string workerId,
        out ExperimentResponse? requeuedExperiment)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var existingExperiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(experiment =>
                experiment.Id == id);

        if (existingExperiment is null ||
            existingExperiment.Status != ExperimentStatus.Running ||
            existingExperiment.AssignedWorkerId != workerId)
        {
            requeuedExperiment = existingExperiment is null
                ? null
                : ToResponse(existingExperiment);

            return false;
        }

        int updatedRows;
        ExperimentEventType eventType;
        string eventDetails;

        if (existingExperiment.CancellationRequested)
        {
            var finishedAtUtc = DateTimeOffset.UtcNow;

            updatedRows = dbContext.Experiments
                .Where(experiment =>
                    experiment.Id == id &&
                    experiment.Status == ExperimentStatus.Running &&
                    experiment.AssignedWorkerId == workerId &&
                    experiment.CancellationRequested)
                .ExecuteUpdate(setters => setters
                    .SetProperty(
                        experiment => experiment.Status,
                        ExperimentStatus.Cancelled)
                    .SetProperty(
                        experiment => experiment.AssignedWorkerId,
                        (string?)null)
                    .SetProperty(
                        experiment => experiment.FinishedAtUtc,
                        finishedAtUtc)
                    .SetProperty(
                        experiment => experiment.ResultMessage,
                        "Experiment was cancelled after the worker became unavailable.")
                    .SetProperty(
                        experiment => experiment.MetricsJson,
                        (string?)null)
                    .SetProperty(
                        experiment => experiment.ExecutionDurationMs,
                        (long?)null)
                    .SetProperty(
                        experiment => experiment.CurrentStep,
                        (int?)null)
                    .SetProperty(
                        experiment => experiment.ProgressMetricsJson,
                        (string?)null)
                    .SetProperty(
                        experiment => experiment.LastProgressAtUtc,
                        (DateTimeOffset?)null));

            eventType = ExperimentEventType.Cancelled;
            eventDetails =
                $"Experiment was cancelled after worker '{workerId}' became unavailable.";
        }
        else
        {
            updatedRows = dbContext.Experiments
                .Where(experiment =>
                    experiment.Id == id &&
                    experiment.Status == ExperimentStatus.Running &&
                    experiment.AssignedWorkerId == workerId &&
                    !experiment.CancellationRequested)
                .ExecuteUpdate(setters => setters
                    .SetProperty(
                        experiment => experiment.Status,
                        ExperimentStatus.Pending)
                    .SetProperty(
                        experiment => experiment.AssignedWorkerId,
                        (string?)null)
                    .SetProperty(
                        experiment => experiment.FinishedAtUtc,
                        (DateTimeOffset?)null)
                    .SetProperty(
                        experiment => experiment.ResultMessage,
                        (string?)null)
                    .SetProperty(
                        experiment => experiment.MetricsJson,
                        (string?)null)
                    .SetProperty(
                        experiment => experiment.ExecutionDurationMs,
                        (long?)null)
                    .SetProperty(
                        experiment => experiment.CancellationRequested,
                        false));

            eventType = ExperimentEventType.Requeued;
            eventDetails =
                $"Experiment was returned to Pending because worker '{workerId}' became unavailable.";
        }

        if (updatedRows != 1)
        {
            requeuedExperiment = GetById(id);
            return false;
        }

        var updatedExperiment = dbContext.Experiments
            .AsNoTracking()
            .Single(experiment =>
                experiment.Id == id);

        dbContext.ExperimentEvents.Add(
            CreateEvent(
                updatedExperiment.Id,
                eventType,
                workerId,
                updatedExperiment.Attempt,
                eventDetails));

        dbContext.SaveChanges();
        transaction.Commit();

        requeuedExperiment = ToResponse(updatedExperiment);

        return true;
    }

     public bool TryRequestCancellation(
        Guid id,
        out ExperimentResponse? updatedExperiment)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var existingExperiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(experiment =>
                experiment.Id == id);

        if (existingExperiment is null)
        {
            updatedExperiment = null;
            return false;
        }

        if (existingExperiment.Status == ExperimentStatus.Cancelled)
        {
            updatedExperiment = ToResponse(existingExperiment);
            return true;
        }

        int updatedRows;
        ExperimentEventType eventType;
        string details;

        if (existingExperiment.Status == ExperimentStatus.Pending)
        {
            var finishedAtUtc = DateTimeOffset.UtcNow;

            updatedRows = dbContext.Experiments
                .Where(experiment =>
                    experiment.Id == id &&
                    experiment.Status == ExperimentStatus.Pending)
                .ExecuteUpdate(setters => setters
                    .SetProperty(
                        experiment => experiment.Status,
                        ExperimentStatus.Cancelled)
                    .SetProperty(
                        experiment => experiment.CancellationRequested,
                        true)
                    .SetProperty(
                        experiment => experiment.FinishedAtUtc,
                        finishedAtUtc)
                    .SetProperty(
                        experiment => experiment.ResultMessage,
                        "Experiment was cancelled before execution."));

            eventType = ExperimentEventType.Cancelled;
            details =
                "Experiment was cancelled before execution.";
        }
        else if (existingExperiment.Status == ExperimentStatus.Running)
        {
            if (existingExperiment.CancellationRequested)
            {
                updatedExperiment =
                    ToResponse(existingExperiment);

                return true;
            }

            updatedRows = dbContext.Experiments
                .Where(experiment =>
                    experiment.Id == id &&
                    experiment.Status == ExperimentStatus.Running &&
                    !experiment.CancellationRequested)
                .ExecuteUpdate(setters => setters
                    .SetProperty(
                        experiment => experiment.CancellationRequested,
                        true));

            eventType = ExperimentEventType.CancelRequested;
            details =
                "Cancellation was requested for the running experiment.";
        }
        else
        {
            updatedExperiment =
                ToResponse(existingExperiment);

            return false;
        }

        if (updatedRows != 1)
        {
            updatedExperiment = GetById(id);
            return false;
        }

        var updatedEntity = dbContext.Experiments
            .AsNoTracking()
            .Single(experiment =>
                experiment.Id == id);

        dbContext.ExperimentEvents.Add(
            CreateEvent(
                updatedEntity.Id,
                eventType,
                updatedEntity.AssignedWorkerId,
                updatedEntity.Attempt,
                details));

        dbContext.SaveChanges();
        transaction.Commit();

        updatedExperiment = ToResponse(updatedEntity);

        return true;
    } 

    private static ExperimentEventEntity CreateEvent(
        Guid experimentId,
        ExperimentEventType type,
        string? workerId,
        int attempt,
        string details)
    {
        return new ExperimentEventEntity
        {
            Id = Guid.NewGuid(),
            ExperimentId = experimentId,
            Type = type,
            OccurredAtUtc = DateTimeOffset.UtcNow,
            WorkerId = workerId,
            Attempt = attempt,
            Details = details
        };
    } 

    internal static ExperimentResponse ToResponse(
        ExperimentEntity experiment)
    {
        return new ExperimentResponse
        {
            Id = experiment.Id,
            Name = experiment.Name,
            Status = experiment.Status,
            Algorithm = experiment.Algorithm,
            Environment = experiment.Environment,
            Seed = experiment.Seed,
            MaxSteps = experiment.MaxSteps,
            Priority = experiment.Priority,
            CreatedAtUtc = experiment.CreatedAtUtc,
            AssignedWorkerId = experiment.AssignedWorkerId,
            FinishedAtUtc = experiment.FinishedAtUtc,
            ResultMessage = experiment.ResultMessage,
            SimulateFailure = experiment.SimulateFailure,
            Attempt = experiment.Attempt,
            MetricsJson = experiment.MetricsJson,
            CancellationRequested = experiment.CancellationRequested,
            ExecutionDurationMs = experiment.ExecutionDurationMs,
            TimeoutSeconds = experiment.TimeoutSeconds,
            CurrentStep = experiment.CurrentStep,
            ProgressMetricsJson = experiment.ProgressMetricsJson,
            LastProgressAtUtc = experiment.LastProgressAtUtc,
        };
    }

    /*
    public int RequeueInterruptedExperiments()
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var interruptedExperiments = dbContext.Experiments
            .Where(experiment =>
                experiment.Status == ExperimentStatus.Running)
            .ToArray();

        foreach (var experiment in interruptedExperiments)
        {
            var previousWorkerId = experiment.AssignedWorkerId;

            experiment.Status = ExperimentStatus.Pending;
            experiment.AssignedWorkerId = null;
            experiment.FinishedAtUtc = null;
            experiment.ResultMessage = null;
            experiment.CurrentStep = null;
            experiment.ProgressMetricsJson = null;
            experiment.LastProgressAtUtc = null;

            dbContext.ExperimentEvents.Add(
                CreateEvent(
                    experiment.Id,
                    ExperimentEventType.RecoveredOnStartup,
                    previousWorkerId,
                    experiment.Attempt,
                    "Experiment was returned to Pending after Coordinator restart."));
        }

        dbContext.SaveChanges();
        transaction.Commit();

        return interruptedExperiments.Length;
    } 
    */

    public IReadOnlyCollection<ExperimentEventResponse> GetEvents(
    Guid experimentId)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        return dbContext.ExperimentEvents
            .AsNoTracking()
            .Where(experimentEvent =>
                experimentEvent.ExperimentId == experimentId)
            .AsEnumerable()
            .OrderBy(experimentEvent =>
                experimentEvent.OccurredAtUtc)
            .Select(experimentEvent =>
                new ExperimentEventResponse
                {
                    Id = experimentEvent.Id,
                    Type = experimentEvent.Type.ToString(),
                    OccurredAtUtc = experimentEvent.OccurredAtUtc,
                    WorkerId = experimentEvent.WorkerId,
                    Attempt = experimentEvent.Attempt,
                    Details = experimentEvent.Details
                })
            .ToArray();
    }

}