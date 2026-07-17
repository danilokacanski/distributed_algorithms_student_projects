using Contracts;
using Coordinator.Data;
using Coordinator.Services;
using Microsoft.EntityFrameworkCore;

namespace Coordinator.Commands;

public sealed class CoordinatorCommandProcessor(
    IDbContextFactory<CoordinatorDbContext> dbContextFactory)
{
    public void Apply(CoordinatorCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);

        switch (command)
        {
            case CreateExperimentCommand createCommand:
                Apply(createCommand);
                break;

            case AssignExperimentCommand assignCommand:
                Apply(assignCommand);
                break;

            case CompleteExperimentCommand completeCommand:
                Apply(completeCommand);
                break;

            case RequestExperimentCancellationCommand
                cancellationCommand:
                Apply(cancellationCommand);
                break;

            case RequeueExperimentCommand requeueCommand:
                Apply(requeueCommand);
                break;

            case RecoverExperimentOnStartupCommand
                recoveryCommand:
                Apply(recoveryCommand);
                break;

            default:
                throw new InvalidOperationException(
                    $"Unsupported coordinator command " +
                    $"type '{command.GetType().FullName}'.");
        }
    }

    public CommandApplicationResult<ExperimentResponse> Apply(
        CreateExperimentCommand command)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var appliedCommand = dbContext.AppliedCommands
            .AsNoTracking()
            .SingleOrDefault(existingCommand =>
                existingCommand.CommandId ==
                command.CommandId);

        if (appliedCommand is not null)
        {
            var existingExperiment =
                dbContext.Experiments
                    .AsNoTracking()
                    .SingleOrDefault(experiment =>
                        experiment.Id ==
                        appliedCommand.ExperimentId)
                ?? throw new InvalidOperationException(
                    $"Applied command '{command.CommandId}' " +
                    "references an experiment that does not exist.");

            return new CommandApplicationResult<
                ExperimentResponse>(
                ExperimentRegistry.ToResponse(
                    existingExperiment),
                WasAlreadyApplied: true);
        }

        if (dbContext.Experiments.Any(experiment =>
            experiment.Id == command.ExperimentId))
        {
            throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' " +
                "already exists.");
        }

        var experiment = new ExperimentEntity
        {
            Id = command.ExperimentId,
            Name = command.Name,
            Algorithm = command.Algorithm,
            Environment = command.Environment,
            Seed = command.Seed,
            MaxSteps = command.MaxSteps,
            Priority = command.Priority,
            TimeoutSeconds = command.TimeoutSeconds,
            SimulateFailure = command.SimulateFailure,

            Status = ExperimentStatus.Pending,
            CreatedAtUtc = command.OccurredAtUtc,
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

        var createdEvent = new ExperimentEventEntity
        {
            Id = command.EventId,
            ExperimentId = experiment.Id,
            Type = ExperimentEventType.Created,
            OccurredAtUtc = command.OccurredAtUtc,
            WorkerId = null,
            Attempt = 0,
            Details =
                $"Experiment '{experiment.Name}' was created " +
                $"for algorithm '{experiment.Algorithm}' and " +
                $"environment '{experiment.Environment}'."
        };

        var appliedCommandEntity =
            new AppliedCommandEntity
            {
                CommandId = command.CommandId,
                CommandType =
                    nameof(CreateExperimentCommand),
                ExperimentId = experiment.Id,
                OccurredAtUtc = command.OccurredAtUtc
            };

        dbContext.Experiments.Add(experiment);
        dbContext.ExperimentEvents.Add(createdEvent);
        dbContext.AppliedCommands.Add(
            appliedCommandEntity);

        dbContext.SaveChanges();
        transaction.Commit();

        return new CommandApplicationResult<
            ExperimentResponse>(
                ExperimentRegistry.ToResponse(experiment),
                WasAlreadyApplied: false);
    }

    public CommandApplicationResult<ExperimentResponse> Apply(
        AssignExperimentCommand command)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var previousResult = GetPreviouslyAppliedResult(
            dbContext,
            command,
            command.ExperimentId);

        if (previousResult is not null)
        {
            return new CommandApplicationResult<ExperimentResponse>(
                previousResult,
                WasAlreadyApplied: true);
        }

        var updatedRows = dbContext.Experiments
            .Where(experiment =>
                experiment.Id == command.ExperimentId &&
                experiment.Status == ExperimentStatus.Pending &&
                experiment.Attempt == command.Attempt - 1)
            .ExecuteUpdate(setters => setters
                .SetProperty(
                    experiment => experiment.Status,
                    ExperimentStatus.Running)
                .SetProperty(
                    experiment => experiment.AssignedWorkerId,
                    command.WorkerId)
                .SetProperty(
                    experiment => experiment.Attempt,
                    command.Attempt)
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
                    false));

        if (updatedRows != 1)
        {
            throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' " +
                "could not be assigned.");
        }

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .Single(experiment =>
                experiment.Id == command.ExperimentId);

        dbContext.ExperimentEvents.Add(
            CreateEvent(
                command.EventId,
                command.ExperimentId,
                ExperimentEventType.Assigned,
                command.OccurredAtUtc,
                command.WorkerId,
                command.Attempt,
                $"Experiment was assigned to worker " +
                $"'{command.WorkerId}'."));

        AddAppliedCommand(
            dbContext,
            command,
            command.ExperimentId);

        dbContext.SaveChanges();
        transaction.Commit();

        return new CommandApplicationResult<ExperimentResponse>(
            ExperimentRegistry.ToResponse(experiment),
            WasAlreadyApplied: false);
    }

    public CommandApplicationResult<ExperimentResponse> Apply(
        CompleteExperimentCommand command)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var previousResult = GetPreviouslyAppliedResult(
            dbContext,
            command,
            command.ExperimentId);

        if (previousResult is not null)
        {
            return new CommandApplicationResult<ExperimentResponse>(
                previousResult,
                WasAlreadyApplied: true);
        }

        var finalStatus = command.WasCancelled
            ? ExperimentStatus.Cancelled
            : command.Succeeded
                ? ExperimentStatus.Completed
                : ExperimentStatus.Failed;

        var eventType = command.WasCancelled
            ? ExperimentEventType.Cancelled
            : command.Succeeded
                ? ExperimentEventType.Completed
                : ExperimentEventType.Failed;

        var eventDetails = command.WasCancelled
            ? "Experiment execution was cancelled."
            : command.Succeeded
                ? "Experiment completed successfully."
                : $"Experiment failed. {command.ResultMessage}";

        var updatedRows = dbContext.Experiments
            .Where(experiment =>
                experiment.Id == command.ExperimentId &&
                experiment.Status == ExperimentStatus.Running &&
                experiment.AssignedWorkerId == command.WorkerId &&
                experiment.Attempt == command.Attempt &&
                experiment.CancellationRequested ==
                    command.WasCancelled)
            .ExecuteUpdate(setters => setters
                .SetProperty(
                    experiment => experiment.Status,
                    finalStatus)
                .SetProperty(
                    experiment => experiment.FinishedAtUtc,
                    command.OccurredAtUtc)
                .SetProperty(
                    experiment => experiment.ResultMessage,
                    command.ResultMessage)
                .SetProperty(
                    experiment => experiment.MetricsJson,
                    command.MetricsJson)
                .SetProperty(
                    experiment => experiment.ExecutionDurationMs,
                    command.ExecutionDurationMs)
                .SetProperty(
                    experiment => experiment.CancellationRequested,
                    command.WasCancelled));

        if (updatedRows != 1)
        {
            throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' " +
                "could not be completed.");
        }

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .Single(experiment =>
                experiment.Id == command.ExperimentId);

        dbContext.ExperimentEvents.Add(
            CreateEvent(
                command.EventId,
                command.ExperimentId,
                eventType,
                command.OccurredAtUtc,
                command.WorkerId,
                command.Attempt,
                eventDetails));

        AddAppliedCommand(
            dbContext,
            command,
            command.ExperimentId);

        dbContext.SaveChanges();
        transaction.Commit();

        return new CommandApplicationResult<ExperimentResponse>(
            ExperimentRegistry.ToResponse(experiment),
            WasAlreadyApplied: false);
    }

    public CommandApplicationResult<ExperimentResponse> Apply(
        RequestExperimentCancellationCommand command)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var previousResult = GetPreviouslyAppliedResult(
            dbContext,
            command,
            command.ExperimentId);

        if (previousResult is not null)
        {
            return new CommandApplicationResult<ExperimentResponse>(
                previousResult,
                WasAlreadyApplied: true);
        }

        var existingExperiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(experiment =>
                experiment.Id == command.ExperimentId)
            ?? throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' " +
                "does not exist.");

        ExperimentEventEntity? cancellationEvent = null;

        if (existingExperiment.Status == ExperimentStatus.Pending)
        {
            var updatedRows = dbContext.Experiments
                .Where(experiment =>
                    experiment.Id == command.ExperimentId &&
                    experiment.Status == ExperimentStatus.Pending)
                .ExecuteUpdate(setters => setters
                    .SetProperty(
                        experiment => experiment.Status,
                        ExperimentStatus.Cancelled)
                    .SetProperty(
                        experiment =>
                            experiment.CancellationRequested,
                        true)
                    .SetProperty(
                        experiment => experiment.FinishedAtUtc,
                        command.OccurredAtUtc)
                    .SetProperty(
                        experiment => experiment.ResultMessage,
                        "Experiment was cancelled before execution."));

            if (updatedRows != 1)
            {
                throw new InvalidOperationException(
                    $"Experiment '{command.ExperimentId}' " +
                    "could not be cancelled.");
            }

            cancellationEvent = CreateEvent(
                command.EventId,
                command.ExperimentId,
                ExperimentEventType.Cancelled,
                command.OccurredAtUtc,
                workerId: null,
                existingExperiment.Attempt,
                "Experiment was cancelled before execution.");
        }
        else if (existingExperiment.Status ==
                ExperimentStatus.Running)
        {
            if (!existingExperiment.CancellationRequested)
            {
                var updatedRows = dbContext.Experiments
                    .Where(experiment =>
                        experiment.Id == command.ExperimentId &&
                        experiment.Status ==
                            ExperimentStatus.Running &&
                        !experiment.CancellationRequested)
                    .ExecuteUpdate(setters => setters
                        .SetProperty(
                            experiment =>
                                experiment.CancellationRequested,
                            true));

                if (updatedRows != 1)
                {
                    throw new InvalidOperationException(
                        $"Cancellation could not be requested for " +
                        $"experiment '{command.ExperimentId}'.");
                }

                cancellationEvent = CreateEvent(
                    command.EventId,
                    command.ExperimentId,
                    ExperimentEventType.CancelRequested,
                    command.OccurredAtUtc,
                    existingExperiment.AssignedWorkerId,
                    existingExperiment.Attempt,
                    "Cancellation was requested for the " +
                    "running experiment.");
            }
        }
        else
        {
            throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' cannot be " +
                $"cancelled because its status is " +
                $"'{existingExperiment.Status}'.");
        }

        if (cancellationEvent is not null)
        {
            dbContext.ExperimentEvents.Add(cancellationEvent);
        }

        AddAppliedCommand(
            dbContext,
            command,
            command.ExperimentId);

        dbContext.SaveChanges();
        transaction.Commit();

        var updatedExperiment = dbContext.Experiments
            .AsNoTracking()
            .Single(experiment =>
                experiment.Id == command.ExperimentId);

        return new CommandApplicationResult<ExperimentResponse>(
            ExperimentRegistry.ToResponse(updatedExperiment),
            WasAlreadyApplied: false);
    }

    public CommandApplicationResult<ExperimentResponse> Apply(
        RequeueExperimentCommand command)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var previousResult = GetPreviouslyAppliedResult(
            dbContext,
            command,
            command.ExperimentId);

        if (previousResult is not null)
        {
            return new CommandApplicationResult<ExperimentResponse>(
                previousResult,
                WasAlreadyApplied: true);
        }

        var existingExperiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(experiment =>
                experiment.Id == command.ExperimentId)
            ?? throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' " +
                "does not exist.");

        if (existingExperiment.Status !=
                ExperimentStatus.Running ||
            existingExperiment.AssignedWorkerId !=
                command.WorkerId ||
            existingExperiment.Attempt != command.Attempt)
        {
            throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' " +
                "cannot be recovered from worker failure.");
        }

        if (existingExperiment.CancellationRequested !=
            command.CancelInsteadOfRequeue)
        {
            throw new InvalidOperationException(
                "The recovery command does not match the " +
                "current cancellation state.");
        }

        int updatedRows;
        ExperimentEventType eventType;
        string eventDetails;

        if (command.CancelInsteadOfRequeue)
        {
            updatedRows = dbContext.Experiments
                .Where(experiment =>
                    experiment.Id == command.ExperimentId &&
                    experiment.Status ==
                        ExperimentStatus.Running &&
                    experiment.AssignedWorkerId ==
                        command.WorkerId &&
                    experiment.Attempt == command.Attempt &&
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
                        command.OccurredAtUtc)
                    .SetProperty(
                        experiment => experiment.ResultMessage,
                        "Experiment was cancelled after the " +
                        "worker became unavailable.")
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
                        experiment =>
                            experiment.CancellationRequested,
                        true));

            eventType = ExperimentEventType.Cancelled;

            eventDetails =
                $"Experiment was cancelled after worker " +
                $"'{command.WorkerId}' became unavailable.";
        }
        else
        {
            updatedRows = dbContext.Experiments
                .Where(experiment =>
                    experiment.Id == command.ExperimentId &&
                    experiment.Status ==
                        ExperimentStatus.Running &&
                    experiment.AssignedWorkerId ==
                        command.WorkerId &&
                    experiment.Attempt == command.Attempt &&
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
                        experiment => experiment.CurrentStep,
                        (int?)null)
                    .SetProperty(
                        experiment => experiment.ProgressMetricsJson,
                        (string?)null)
                    .SetProperty(
                        experiment => experiment.LastProgressAtUtc,
                        (DateTimeOffset?)null)
                    .SetProperty(
                        experiment =>
                            experiment.CancellationRequested,
                        false));

            eventType = ExperimentEventType.Requeued;

            eventDetails =
                $"Experiment was returned to Pending because worker " +
                $"'{command.WorkerId}' became unavailable.";
        }

        if (updatedRows != 1)
        {
            throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' " +
                "could not be recovered.");
        }

        dbContext.ExperimentEvents.Add(
            CreateEvent(
                command.EventId,
                command.ExperimentId,
                eventType,
                command.OccurredAtUtc,
                command.WorkerId,
                command.Attempt,
                eventDetails));

        AddAppliedCommand(
            dbContext,
            command,
            command.ExperimentId);

        dbContext.SaveChanges();
        transaction.Commit();

        var updatedExperiment = dbContext.Experiments
            .AsNoTracking()
            .Single(experiment =>
                experiment.Id == command.ExperimentId);

        return new CommandApplicationResult<ExperimentResponse>(
            ExperimentRegistry.ToResponse(updatedExperiment),
            WasAlreadyApplied: false);
    }

    public CommandApplicationResult<ExperimentResponse> Apply(
        RecoverExperimentOnStartupCommand command)
    {
        using var dbContext =
            dbContextFactory.CreateDbContext();

        using var transaction =
            dbContext.Database.BeginTransaction();

        var previousResult = GetPreviouslyAppliedResult(
            dbContext,
            command,
            command.ExperimentId);

        if (previousResult is not null)
        {
            return new CommandApplicationResult<ExperimentResponse>(
                previousResult,
                WasAlreadyApplied: true);
        }

        var updatedRows = dbContext.Experiments
            .Where(experiment =>
                experiment.Id == command.ExperimentId &&
                experiment.Status == ExperimentStatus.Running &&
                experiment.AssignedWorkerId ==
                    command.PreviousWorkerId &&
                experiment.Attempt == command.Attempt)
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
                    experiment => experiment.CurrentStep,
                    (int?)null)
                .SetProperty(
                    experiment => experiment.ProgressMetricsJson,
                    (string?)null)
                .SetProperty(
                    experiment => experiment.LastProgressAtUtc,
                    (DateTimeOffset?)null));

        if (updatedRows != 1)
        {
            throw new InvalidOperationException(
                $"Experiment '{command.ExperimentId}' " +
                "could not be recovered during startup.");
        }

        dbContext.ExperimentEvents.Add(
            CreateEvent(
                command.EventId,
                command.ExperimentId,
                ExperimentEventType.RecoveredOnStartup,
                command.OccurredAtUtc,
                command.PreviousWorkerId,
                command.Attempt,
                "Experiment was returned to Pending after " +
                "Coordinator restart."));

        AddAppliedCommand(
            dbContext,
            command,
            command.ExperimentId);

        dbContext.SaveChanges();
        transaction.Commit();

        var updatedExperiment = dbContext.Experiments
            .AsNoTracking()
            .Single(experiment =>
                experiment.Id == command.ExperimentId);

        return new CommandApplicationResult<ExperimentResponse>(
            ExperimentRegistry.ToResponse(updatedExperiment),
            WasAlreadyApplied: false);
    }

    private static ExperimentResponse?
    GetPreviouslyAppliedResult(
        CoordinatorDbContext dbContext,
        CoordinatorCommand command,
        Guid experimentId)
    {
        var appliedCommand = dbContext.AppliedCommands
            .AsNoTracking()
            .SingleOrDefault(existingCommand =>
                existingCommand.CommandId ==
                command.CommandId);

        if (appliedCommand is null)
        {
            return null;
        }

        var expectedCommandType =
            command.GetType().Name;

        if (appliedCommand.CommandType !=
                expectedCommandType ||
            appliedCommand.ExperimentId != experimentId)
        {
            throw new InvalidOperationException(
                $"Command id '{command.CommandId}' has already " +
                "been used for a different command.");
        }

        var experiment = dbContext.Experiments
            .AsNoTracking()
            .SingleOrDefault(existingExperiment =>
                existingExperiment.Id ==
                appliedCommand.ExperimentId)
            ?? throw new InvalidOperationException(
                $"Applied command '{command.CommandId}' " +
                "references an experiment that does not exist.");

        return ExperimentRegistry.ToResponse(experiment);
    }

    private static void AddAppliedCommand(
            CoordinatorDbContext dbContext,
        CoordinatorCommand command,
        Guid experimentId)
    {
        dbContext.AppliedCommands.Add(
            new AppliedCommandEntity
            {
                CommandId = command.CommandId,
                CommandType = command.GetType().Name,
                ExperimentId = experimentId,
                OccurredAtUtc = command.OccurredAtUtc
            });
    }

    private static ExperimentEventEntity CreateEvent(
        Guid eventId,
        Guid experimentId,
        ExperimentEventType type,
        DateTimeOffset occurredAtUtc,
        string? workerId,
        int attempt,
        string details)
    {
        return new ExperimentEventEntity
        {
            Id = eventId,
            ExperimentId = experimentId,
            Type = type,
            OccurredAtUtc = occurredAtUtc,
            WorkerId = workerId,
            Attempt = attempt,
            Details = details
        };
    }
}