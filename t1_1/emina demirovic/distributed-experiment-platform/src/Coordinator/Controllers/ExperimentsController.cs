using Contracts;
using Coordinator.Raft;
using Coordinator.Commands;
using Coordinator.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
namespace Coordinator.Controllers;

[ApiController]
[Route("api/experiments")]
public sealed class ExperimentsController : ControllerBase
{
    private readonly ExperimentRegistry _experimentRegistry;

    private readonly WorkerRegistry _workerRegistry;

    private readonly CoordinatorCommandProcessor _commandProcessor;

    private readonly RaftCommandSubmitter _raftCommandSubmitter;

    private readonly IOptions<RaftOptions> _raftOptions;

    public ExperimentsController(
        ExperimentRegistry experimentRegistry,
        WorkerRegistry workerRegistry,
        CoordinatorCommandProcessor commandProcessor,
        RaftCommandSubmitter raftCommandSubmitter,
        IOptions<RaftOptions> raftOptions)
    {
        _experimentRegistry = experimentRegistry;
        _workerRegistry = workerRegistry;
        _commandProcessor = commandProcessor;
        _raftCommandSubmitter = raftCommandSubmitter;
        _raftOptions = raftOptions;
    }

    [HttpPost]
    public async Task<ActionResult<ExperimentResponse>> Create(
        CreateExperimentRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
        {
            return BadRequest("Experiment name is required.");
        }

        if (string.IsNullOrWhiteSpace(request.Algorithm))
        {
            return BadRequest("Algorithm is required.");
        }

        if (string.IsNullOrWhiteSpace(request.Environment))
        {
            return BadRequest("Environment is required.");
        }

        if (request.MaxSteps <= 0)
        {
            return BadRequest("MaxSteps must be greater than zero.");
        }

        if (request.Priority is < 0 or > 10)
        {
            return BadRequest("Priority must be between 0 and 10.");
        }

        if (request.TimeoutSeconds is < 1 or > 86400)
        {
            return BadRequest(
                "TimeoutSeconds must be between 1 and 86400.");
        }

        var occurredAtUtc =
            DateTimeOffset.UtcNow;

        var command = new CreateExperimentCommand(
            CommandId: Guid.NewGuid(),
            OccurredAtUtc: occurredAtUtc,
            ExperimentId: Guid.NewGuid(),
            EventId: Guid.NewGuid(),
            Name: request.Name,
            Algorithm: request.Algorithm,
            Environment: request.Environment,
            Seed: request.Seed,
            MaxSteps: request.MaxSteps,
            Priority: request.Priority,
            TimeoutSeconds: request.TimeoutSeconds,
            SimulateFailure: request.SimulateFailure);

        if (!_raftOptions.Value.ClientCommandReplicationEnabled)
        {
            var response =
                _commandProcessor.Apply(command);

            return CreatedAtAction(
                nameof(GetById),
                new { id = response.Value.Id },
                response.Value);
        }

        var submission =
            await _raftCommandSubmitter.SubmitAsync(
                command,
                cancellationToken);

        if (submission.Status ==
            RaftCommandSubmissionStatus.NotLeader)
        {
            return StatusCode(
                StatusCodes.Status409Conflict,
                new
                {
                    message =
                        "This Coordinator is not the Raft leader.",
                    leaderId =
                        submission.LeaderId
                });
        }

        if (submission.Status ==
            RaftCommandSubmissionStatus.TimedOut)
        {
            return StatusCode(
                StatusCodes.Status504GatewayTimeout,
                new
                {
                    message =
                        "The command was not committed before the timeout.",
                    logIndex =
                        submission.LogIndex
                });
        }

        var replicatedResponse =
            _experimentRegistry.GetById(
                command.ExperimentId);

        if (replicatedResponse is null)
        {
            return StatusCode(
                StatusCodes.Status500InternalServerError,
                "The command was committed but the experiment was not found.");
        }

        return CreatedAtAction(
            nameof(GetById),
            new { id = replicatedResponse.Id },
            replicatedResponse);
    }

    [HttpGet]
    public ActionResult<IReadOnlyCollection<ExperimentResponse>> GetAll()
    {
        return Ok(_experimentRegistry.GetAll());
    }

    [HttpGet("{id:guid}")]
    public ActionResult<ExperimentResponse> GetById(Guid id)
    {
        var experiment = _experimentRegistry.GetById(id);

        if (experiment is null)
        {
            return NotFound($"Experiment '{id}' was not found.");
            
        }

        return Ok(experiment);
    }

    [HttpPost("{id:guid}/assign")]
    public async Task<ActionResult<ExperimentResponse>> Assign(
        Guid id,
        CancellationToken cancellationToken)
    {
        var existingExperiment =
            _experimentRegistry.GetById(id);

        if (existingExperiment is null)
        {
            return NotFound(
                $"Experiment '{id}' was not found.");
        }

        if (existingExperiment.Status !=
            ExperimentStatus.Pending)
        {
            return Conflict(
                $"Experiment '{id}' cannot be assigned because " +
                $"its status is '{existingExperiment.Status}'.");
        }

        var worker = _workerRegistry.GetFirstOnline();

        if (worker is null)
        {
            return Conflict(
                "No online worker is currently available.");
        }

        var command = new AssignExperimentCommand(
            CommandId: Guid.NewGuid(),
            OccurredAtUtc: DateTimeOffset.UtcNow,
            ExperimentId: id,
            EventId: Guid.NewGuid(),
            WorkerId: worker.WorkerId,
            Attempt: existingExperiment.Attempt + 1);

        if (!_raftOptions.Value.ClientCommandReplicationEnabled)
        {
            try
            {
                var result =
                    _commandProcessor.Apply(command);

                return Ok(result.Value);
            }
            catch (InvalidOperationException exception)
            {
                return Conflict(exception.Message);
            }
        }

        var submission =
            await _raftCommandSubmitter.SubmitAsync(
                command,
                cancellationToken);

        if (submission.Status ==
            RaftCommandSubmissionStatus.NotLeader)
        {
            return StatusCode(
                StatusCodes.Status409Conflict,
                new
                {
                    message =
                        "This Coordinator is not the Raft leader.",
                    leaderId =
                        submission.LeaderId
                });
        }

        if (submission.Status ==
            RaftCommandSubmissionStatus.TimedOut)
        {
            return StatusCode(
                StatusCodes.Status504GatewayTimeout,
                new
                {
                    message =
                        "The command was not committed before the timeout.",
                    logIndex =
                        submission.LogIndex
                });
        }

        var replicatedResponse =
            _experimentRegistry.GetById(id);

        if (replicatedResponse is null)
        {
            return StatusCode(
                StatusCodes.Status500InternalServerError,
                "The command was committed but the experiment was not found.");
        }

        return Ok(replicatedResponse);
    }

    [HttpGet("worker/{workerId}/next")]
    public ActionResult<ExperimentResponse> GetNextForWorker(string workerId)
    {
        var experiment =
            _experimentRegistry.GetNextAssignedToWorker(workerId);

        if (experiment is null)
        {
            return NoContent();
        }

        return Ok(experiment);
    }

    [HttpPost("{id:guid}/progress")]
    public ActionResult<ExperimentResponse> ReportProgress(
        Guid id,
        ReportExperimentProgressRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.WorkerId))
        {
            return BadRequest("WorkerId is required.");
        }

        if (request.Attempt <= 0)
        {
            return BadRequest(
                "A valid execution attempt is required.");
        }

        if (request.CurrentStep < 0)
        {
            return BadRequest(
                "CurrentStep cannot be negative.");
        }

        var existingExperiment =
            _experimentRegistry.GetById(id);

        if (existingExperiment is null)
        {
            return NotFound(
                $"Experiment '{id}' was not found.");
        }

        if (existingExperiment.Status !=
            ExperimentStatus.Running)
        {
            return Conflict(
                $"Experiment '{id}' is not currently running.");
        }

        if (existingExperiment.AssignedWorkerId !=
            request.WorkerId)
        {
            return Conflict(
                $"Experiment '{id}' is not assigned to worker " +
                $"'{request.WorkerId}'.");
        }

        if (existingExperiment.Attempt != request.Attempt)
        {
            return Conflict(
                $"Experiment '{id}' is currently on attempt " +
                $"{existingExperiment.Attempt}, but progress for " +
                $"attempt {request.Attempt} was received.");
        }

        if (existingExperiment.CancellationRequested)
        {
            return Conflict(
                "Progress cannot be reported after cancellation " +
                "has been requested.");
        }

        if (request.CurrentStep > existingExperiment.MaxSteps)
        {
            return BadRequest(
                $"CurrentStep cannot be greater than MaxSteps " +
                $"({existingExperiment.MaxSteps}).");
        }

        if (existingExperiment.CurrentStep.HasValue &&
            request.CurrentStep <
            existingExperiment.CurrentStep.Value)
        {
            return Conflict(
                "CurrentStep cannot move backwards.");
        }

        var updated =
            _experimentRegistry.TryReportProgress(
                id,
                request.WorkerId,
                request.Attempt,
                request.CurrentStep,
                request.ProgressMetricsJson,
                out var updatedExperiment);

        if (!updated || updatedExperiment is null)
        {
            return Conflict(
                $"Progress for experiment '{id}' " +
                "could not be recorded.");
        }

        return Ok(updatedExperiment);
    }

    [HttpPost("{id:guid}/complete")]
    public async Task<ActionResult<ExperimentResponse>> Complete(
        Guid id,
        CompleteExperimentRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.WorkerId))
        {
            return BadRequest("WorkerId is required.");
        }

        if (request.Attempt <= 0)
        {
            return BadRequest(
                "A valid execution attempt is required.");
        }

        var existingExperiment =
            _experimentRegistry.GetById(id);

        if (existingExperiment is null)
        {
            return NotFound(
                $"Experiment '{id}' was not found.");
        }

        if (existingExperiment.Status !=
            ExperimentStatus.Running)
        {
            return Conflict(
                $"Experiment '{id}' is not currently running.");
        }

        if (existingExperiment.AssignedWorkerId !=
            request.WorkerId)
        {
            return Conflict(
                $"Experiment '{id}' is not assigned to worker " +
                $"'{request.WorkerId}'.");
        }

        if (existingExperiment.Attempt != request.Attempt)
        {
            return Conflict(
                $"Experiment '{id}' is currently on attempt " +
                $"{existingExperiment.Attempt}, but result for " +
                $"attempt {request.Attempt} was received.");
        }

        if (existingExperiment.CancellationRequested !=
            request.WasCancelled)
        {
            return Conflict(
                "The completion result does not match the current " +
                "cancellation state.");
        }

        var command = new CompleteExperimentCommand(
            CommandId: Guid.NewGuid(),
            OccurredAtUtc: DateTimeOffset.UtcNow,
            ExperimentId: id,
            EventId: Guid.NewGuid(),
            WorkerId: request.WorkerId,
            Attempt: request.Attempt,
            Succeeded: request.Succeeded,
            WasCancelled: request.WasCancelled,
            ResultMessage: request.ResultMessage,
            MetricsJson: request.MetricsJson,
            ExecutionDurationMs:
                request.ExecutionDurationMs);

        if (!_raftOptions.Value.ClientCommandReplicationEnabled)
        {
            try
            {
                var result =
                    _commandProcessor.Apply(command);

                return Ok(result.Value);
            }
            catch (InvalidOperationException exception)
            {
                return Conflict(exception.Message);
            }
        }

        var submission =
            await _raftCommandSubmitter.SubmitAsync(
                command,
                cancellationToken);

        if (submission.Status ==
            RaftCommandSubmissionStatus.NotLeader)
        {
            return StatusCode(
                StatusCodes.Status409Conflict,
                new
                {
                    message =
                        "This Coordinator is not the Raft leader.",
                    leaderId =
                        submission.LeaderId
                });
        }

        if (submission.Status ==
            RaftCommandSubmissionStatus.TimedOut)
        {
            return StatusCode(
                StatusCodes.Status504GatewayTimeout,
                new
                {
                    message =
                        "The command was not committed before the timeout.",
                    logIndex =
                        submission.LogIndex
                });
        }

        var replicatedResponse =
            _experimentRegistry.GetById(id);

        if (replicatedResponse is null)
        {
            return StatusCode(
                StatusCodes.Status500InternalServerError,
                "The command was committed but the experiment was not found.");
        }

        return Ok(replicatedResponse);
    }

    [HttpPost("{id:guid}/cancel")]
    public async Task<ActionResult<ExperimentResponse>> Cancel(
        Guid id,
        CancellationToken cancellationToken)
    {
        var existingExperiment =
            _experimentRegistry.GetById(id);

        if (existingExperiment is null)
        {
            return NotFound(
                $"Experiment '{id}' was not found.");
        }

        if (existingExperiment.Status ==
            ExperimentStatus.Cancelled)
        {
            return Ok(existingExperiment);
        }

        if (existingExperiment.Status is
            ExperimentStatus.Completed or
            ExperimentStatus.Failed)
        {
            return Conflict(
                $"Experiment '{id}' cannot be cancelled because " +
                $"its status is '{existingExperiment.Status}'.");
        }

        var command =
            new RequestExperimentCancellationCommand(
                CommandId: Guid.NewGuid(),
                OccurredAtUtc: DateTimeOffset.UtcNow,
                ExperimentId: id,
                EventId: Guid.NewGuid());

        if (!_raftOptions.Value.ClientCommandReplicationEnabled)
        {
            try
            {
                var result =
                    _commandProcessor.Apply(command);

                return Ok(result.Value);
            }
            catch (InvalidOperationException exception)
            {
                return Conflict(exception.Message);
            }
        }

        var submission =
            await _raftCommandSubmitter.SubmitAsync(
                command,
                cancellationToken);

        if (submission.Status ==
            RaftCommandSubmissionStatus.NotLeader)
        {
            return StatusCode(
                StatusCodes.Status409Conflict,
                new
                {
                    message =
                        "This Coordinator is not the Raft leader.",
                    leaderId =
                        submission.LeaderId
                });
        }

        if (submission.Status ==
            RaftCommandSubmissionStatus.TimedOut)
        {
            return StatusCode(
                StatusCodes.Status504GatewayTimeout,
                new
                {
                    message =
                        "The command was not committed before the timeout.",
                    logIndex =
                        submission.LogIndex
                });
        }

        var replicatedResponse =
            _experimentRegistry.GetById(id);

        if (replicatedResponse is null)
        {
            return StatusCode(
                StatusCodes.Status500InternalServerError,
                "The command was committed but the experiment was not found.");
        }

        return Ok(replicatedResponse);
    }
    
    [HttpGet("{id:guid}/events")]
    public ActionResult<IReadOnlyCollection<ExperimentEventResponse>>
        GetEvents(Guid id)
    {
        var experiment =
            _experimentRegistry.GetById(id);

        if (experiment is null)
        {
            return NotFound(
                $"Experiment '{id}' was not found.");
        }

        return Ok(
            _experimentRegistry.GetEvents(id));
    }
}