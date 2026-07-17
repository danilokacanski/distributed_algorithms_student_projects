using Coordinator.Raft;
using Microsoft.AspNetCore.Mvc;

namespace Coordinator.Controllers;

[ApiController]
[Route("api/raft")]
public sealed class RaftController(
    RaftNodeState nodeState,
    RaftElectionService electionService)
    : ControllerBase
{
    [HttpGet("status")]
    public ActionResult<RaftNodeSnapshot> GetStatus()
    {
        return Ok(nodeState.GetSnapshot());
    }

    [HttpPost("request-vote")]
    public ActionResult<RequestVoteResponse>
        RequestVote(
            RequestVoteRequest request)
    {
        if (request.Term < 0)
        {
            return BadRequest(
                "Term cannot be negative.");
        }

        if (string.IsNullOrWhiteSpace(
            request.CandidateId))
        {
            return BadRequest(
                "CandidateId is required.");
        }

        if (request.LastLogIndex < 0 ||
            request.LastLogTerm < 0)
        {
            return BadRequest(
                "Log index and term cannot be negative.");
        }

        return Ok(
            nodeState.HandleRequestVote(request));
    }

    [HttpPost("append-entries")]
    public ActionResult<AppendEntriesResponse>
        AppendEntries(
            AppendEntriesRequest request)
    {
        if (request.Term < 0)
        {
            return BadRequest(
                "Term cannot be negative.");
        }

        if (string.IsNullOrWhiteSpace(
            request.LeaderId))
        {
            return BadRequest(
                "LeaderId is required.");
        }

        if (request.PrevLogIndex < 0 ||
            request.PrevLogTerm < 0 ||
            request.LeaderCommit < 0)
        {
            return BadRequest(
                "Log values cannot be negative.");
        }

        if (request.Entries is null)
        {
            return BadRequest(
                "Entries are required.");
        }
        
        return Ok(
            nodeState.HandleAppendEntries(
                request));
    }

    [HttpPost("start-election")]
    public async Task<ActionResult<RaftElectionResult>>
        StartElection(
            CancellationToken cancellationToken)
    {
        var result =
            await electionService.StartElectionAsync(
                cancellationToken);

        return Ok(result);
    }
}