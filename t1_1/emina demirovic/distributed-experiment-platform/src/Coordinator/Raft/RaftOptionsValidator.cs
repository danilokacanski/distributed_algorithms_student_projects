using Microsoft.Extensions.Options;

namespace Coordinator.Raft;

public sealed class RaftOptionsValidator
    : IValidateOptions<RaftOptions>
{
    public ValidateOptionsResult Validate(
        string? name,
        RaftOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.NodeId))
        {
            return ValidateOptionsResult.Fail(
                "Raft:NodeId is required.");
        }

        if (options.Peers.Count < 2)
        {
            return ValidateOptionsResult.Fail(
                "A three-node Raft cluster requires at least " +
                "two configured peers.");
        }

        if (options.HeartbeatIntervalMilliseconds < 50 ||
            options.HeartbeatIntervalMilliseconds > 10_000)
        {
            return ValidateOptionsResult.Fail(
                "Raft:HeartbeatIntervalMilliseconds must be " +
                "between 50 and 10000.");
        }

        if (options.ElectionTimeoutMinMilliseconds <=
            options.HeartbeatIntervalMilliseconds)
        {
            return ValidateOptionsResult.Fail(
                "Raft:ElectionTimeoutMinMilliseconds must be " +
                "greater than HeartbeatIntervalMilliseconds.");
        }

        if (options.ElectionTimeoutMaxMilliseconds <
            options.ElectionTimeoutMinMilliseconds)
        {
            return ValidateOptionsResult.Fail(
                "Raft:ElectionTimeoutMaxMilliseconds must be " +
                "greater than or equal to ElectionTimeoutMinMilliseconds.");
        }

        if (options.ElectionTimeoutMaxMilliseconds > 60_000)
        {
            return ValidateOptionsResult.Fail(
                "Raft election timeout cannot exceed 60000 milliseconds.");
        }

        if (options.CommandReplicationPollMilliseconds < 10 ||
            options.CommandReplicationPollMilliseconds > 1000)
        {
            return ValidateOptionsResult.Fail(
                "Raft:CommandReplicationPollMilliseconds must be " +
                "between 10 and 1000.");
        }

        if (options.CommandReplicationTimeoutMilliseconds <
            options.CommandReplicationPollMilliseconds)
        {
            return ValidateOptionsResult.Fail(
                "Raft:CommandReplicationTimeoutMilliseconds must be " +
                "greater than or equal to CommandReplicationPollMilliseconds.");
        }

        var peerIds =
            new HashSet<string>(StringComparer.Ordinal);

        foreach (var peer in options.Peers)
        {
            if (string.IsNullOrWhiteSpace(peer.NodeId))
            {
                return ValidateOptionsResult.Fail(
                    "Every Raft peer must have a NodeId.");
            }

            if (string.Equals(
                peer.NodeId,
                options.NodeId,
                StringComparison.Ordinal))
            {
                return ValidateOptionsResult.Fail(
                    "A Raft node cannot include itself " +
                    "in the peer list.");
            }

            if (!peerIds.Add(peer.NodeId))
            {
                return ValidateOptionsResult.Fail(
                    $"Raft peer '{peer.NodeId}' is duplicated.");
            }

            if (!Uri.TryCreate(
                    peer.BaseUrl,
                    UriKind.Absolute,
                    out var peerUri) ||
                (peerUri.Scheme != Uri.UriSchemeHttp &&
                 peerUri.Scheme != Uri.UriSchemeHttps))
            {
                return ValidateOptionsResult.Fail(
                    $"Raft peer '{peer.NodeId}' has an " +
                    "invalid BaseUrl.");
            }
        }

        return ValidateOptionsResult.Success;
    }
}