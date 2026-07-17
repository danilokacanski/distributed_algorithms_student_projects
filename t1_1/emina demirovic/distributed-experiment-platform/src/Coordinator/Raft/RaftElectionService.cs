namespace Coordinator.Raft;

public sealed class RaftElectionService(
    RaftNodeState nodeState,
    IRaftPeerClient peerClient,
    RaftHeartbeatSender heartbeatSender)
{
    private readonly SemaphoreSlim _electionLock =
        new(1, 1);

    public async Task<RaftElectionResult>
        StartElectionAsync(
            CancellationToken cancellationToken = default)
    {
        await _electionLock.WaitAsync(
            cancellationToken);

        try
        {
            var electionSnapshot =
                nodeState.BeginElection();

            var lastLogPosition =
                nodeState.GetLastLogPosition();

            var request =
                new RequestVoteRequest(
                    Term:
                        electionSnapshot.CurrentTerm,
                    CandidateId:
                        electionSnapshot.NodeId,
                    LastLogIndex: lastLogPosition.LogIndex,
                    LastLogTerm: lastLogPosition.Term);

            var responseTasks =
                electionSnapshot.Peers
                    .Select(peer =>
                        RequestVoteSafelyAsync(
                            peer,
                            request,
                            cancellationToken))
                    .ToArray();

            var responses =
                await Task.WhenAll(responseTasks);

            var highestObservedTerm =
                responses
                    .Where(response =>
                        response is not null)
                    .Select(response =>
                        response!.Term)
                    .DefaultIfEmpty(
                        electionSnapshot.CurrentTerm)
                    .Max();

            var votesGranted =
                1 + responses.Count(response =>
                    response is
                    {
                        VoteGranted: true
                    } &&
                    response.Term ==
                        electionSnapshot.CurrentTerm);

            var clusterSize =
                electionSnapshot.Peers.Count + 1;

            var quorumSize =
                clusterSize / 2 + 1;

            var won = false;

            if (highestObservedTerm >
                electionSnapshot.CurrentTerm)
            {
                nodeState.ObserveHigherTerm(
                    highestObservedTerm);
            }
            else if (votesGranted >= quorumSize)
            {
                won = nodeState.TryBecomeLeader(
                    electionSnapshot.CurrentTerm);

                if (won)
                {
                    await heartbeatSender.SendHeartbeatAsync(
                        cancellationToken);
                }
            }

            var finalSnapshot =
                nodeState.GetSnapshot();

            won =
                won &&
                finalSnapshot.Role ==
                    RaftNodeRole.Leader &&
                finalSnapshot.CurrentTerm ==
                    electionSnapshot.CurrentTerm;

            return new RaftElectionResult(
                CurrentTerm:
                    finalSnapshot.CurrentTerm,
                Role:
                    finalSnapshot.Role,
                VotesGranted:
                    votesGranted,
                QuorumSize:
                    quorumSize,
                Won:
                    won);
        }
        finally
        {
            _electionLock.Release();
        }
    }


    private async Task<RequestVoteResponse?>
        RequestVoteSafelyAsync(
            RaftPeerSnapshot peer,
            RequestVoteRequest request,
            CancellationToken cancellationToken)
    {
        try
        {
            return await peerClient.RequestVoteAsync(
                peer,
                request,
                cancellationToken);
        }
        catch (HttpRequestException)
        {
            return null;
        }
        catch (OperationCanceledException)
            when (!cancellationToken
                .IsCancellationRequested)
        {
            return null;
        }
    }
}