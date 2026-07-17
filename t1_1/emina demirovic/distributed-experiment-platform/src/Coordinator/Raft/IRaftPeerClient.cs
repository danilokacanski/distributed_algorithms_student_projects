namespace Coordinator.Raft;

public interface IRaftPeerClient
{
    Task<RequestVoteResponse> RequestVoteAsync(
        RaftPeerSnapshot peer,
        RequestVoteRequest request,
        CancellationToken cancellationToken);

    Task<AppendEntriesResponse> AppendEntriesAsync(
    RaftPeerSnapshot peer,
    AppendEntriesRequest request,
    CancellationToken cancellationToken);
}