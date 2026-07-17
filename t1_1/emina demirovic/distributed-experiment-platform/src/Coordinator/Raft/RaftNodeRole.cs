using System.Text.Json.Serialization;

namespace Coordinator.Raft;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum RaftNodeRole
{
    Follower,
    Candidate,
    Leader
}