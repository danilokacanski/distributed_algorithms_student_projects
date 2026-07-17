using System.Net.Http.Json;

namespace Coordinator.Raft;

public sealed class HttpRaftPeerClient(
    IHttpClientFactory httpClientFactory)
    : IRaftPeerClient
{
    public async Task<RequestVoteResponse> RequestVoteAsync(
        RaftPeerSnapshot peer,
        RequestVoteRequest request,
        CancellationToken cancellationToken)
    {
        var client =
            httpClientFactory.CreateClient("RaftPeer");

        var baseUrl =
            peer.BaseUrl.EndsWith('/')
                ? peer.BaseUrl
                : peer.BaseUrl + "/";

        var endpoint = new Uri(
            new Uri(baseUrl),
            "api/raft/request-vote");

        using var response =
            await client.PostAsJsonAsync(
                endpoint,
                request,
                cancellationToken);

        response.EnsureSuccessStatusCode();

        return await response.Content
            .ReadFromJsonAsync<RequestVoteResponse>(
                cancellationToken: cancellationToken)
            ?? throw new InvalidOperationException(
                $"Peer '{peer.NodeId}' returned an empty response.");
    }

    public async Task<AppendEntriesResponse>
    AppendEntriesAsync(
        RaftPeerSnapshot peer,
        AppendEntriesRequest request,
        CancellationToken cancellationToken)
    {
        var client =
            httpClientFactory.CreateClient(
                "RaftPeer");

        var baseUrl =
            peer.BaseUrl.EndsWith('/')
                ? peer.BaseUrl
                : peer.BaseUrl + "/";

        var endpoint = new Uri(
            new Uri(baseUrl),
            "api/raft/append-entries");

        using var response =
            await client.PostAsJsonAsync(
                endpoint,
                request,
                cancellationToken);

        response.EnsureSuccessStatusCode();

        return await response.Content
            .ReadFromJsonAsync<AppendEntriesResponse>(
                cancellationToken:
                    cancellationToken)
            ?? throw new InvalidOperationException(
                $"Peer '{peer.NodeId}' returned " +
                "an empty AppendEntries response.");
    }
}