using Coordinator.Raft;

namespace Coordinator.Tests.Raft;

public sealed class RaftReplicationTrackerTests
{
    [Fact]
    public void Initialize_StartsAfterLeaderLastLogEntry()
    {
        var tracker =
            new RaftReplicationTracker();

        tracker.EnsureInitialized(
            term: 3,
            peers:
            [
                new RaftPeerSnapshot(
                    "peer-1",
                    "http://localhost:6002")
            ],
            leaderLastLogIndex: 5);

        var state =
            tracker.Get("peer-1");

        Assert.Equal(6, state.NextIndex);
        Assert.Equal(0, state.MatchIndex);
    }

    [Fact]
    public void FailedReplication_DecrementsNextIndex()
    {
        var tracker =
            CreateTracker(lastLogIndex: 5);

        tracker.RecordFailure(
            "peer-1",
            term: 3);

        Assert.Equal(
            5,
            tracker.Get("peer-1").NextIndex);
    }

    [Fact]
    public void SuccessfulReplication_UpdatesBothIndexes()
    {
        var tracker =
            CreateTracker(lastLogIndex: 5);

        tracker.RecordSuccess(
            "peer-1",
            replicatedThroughIndex: 4,
            term: 3);

        var state =
            tracker.Get("peer-1");

        Assert.Equal(4, state.MatchIndex);
        Assert.Equal(5, state.NextIndex);
    }

    private static RaftReplicationTracker
        CreateTracker(long lastLogIndex)
    {
        var tracker =
            new RaftReplicationTracker();

        tracker.EnsureInitialized(
            term: 3,
            peers:
            [
                new RaftPeerSnapshot(
                    "peer-1",
                    "http://localhost:6002")
            ],
            leaderLastLogIndex:
                lastLogIndex);

        return tracker;
    }
}