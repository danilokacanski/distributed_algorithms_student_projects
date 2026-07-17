using Coordinator.Commands;
using Coordinator.Raft;
using Microsoft.Extensions.Options;

namespace Coordinator.Tests.Raft;

public sealed class RaftCommandSubmissionTests
{
    [Fact]
    public async Task Follower_RejectsSubmittedCommand()
    {
        var setup =
            CreateSetup();

        var result =
            await setup.Submitter.SubmitAsync(
                CreateCommand());

        Assert.Equal(
            RaftCommandSubmissionStatus.NotLeader,
            result.Status);

        Assert.Null(result.LogIndex);
        Assert.Null(setup.LogStore.GetLast());
    }

    [Fact]
    public async Task Leader_CommitsAndAppliesCommandWithQuorum()
    {
        var setup =
            CreateSetup(
                peerClient:
                    new SuccessfulPeerClient());

        var election =
            setup.State.BeginElection();

        Assert.True(
            setup.State.TryBecomeLeader(
                election.CurrentTerm));

        var result =
            await setup.Submitter.SubmitAsync(
                CreateCommand());

        Assert.Equal(
            RaftCommandSubmissionStatus.Committed,
            result.Status);

        Assert.Equal(1, result.LogIndex);
        Assert.Equal(1, result.AppliedCount);

        Assert.Equal(
            new RaftCommitState(
                CommitIndex: 1,
                LastApplied: 1),
            setup.CommitManager.GetState());
    }

    [Fact]
    public async Task Leader_TimesOutWithoutQuorum()
    {
        var setup =
            CreateSetup(
                peerClient:
                    new RejectingPeerClient(),
                timeoutMilliseconds: 80,
                pollMilliseconds: 10);

        var election =
            setup.State.BeginElection();

        Assert.True(
            setup.State.TryBecomeLeader(
                election.CurrentTerm));

        var result =
            await setup.Submitter.SubmitAsync(
                CreateCommand());

        Assert.Equal(
            RaftCommandSubmissionStatus.TimedOut,
            result.Status);

        Assert.Equal(
            0,
            setup.CommitManager
                .GetState()
                .CommitIndex);
    }

    [Fact]
    public async Task HigherTermDuringReplication_DemotesLeader()
    {
        var setup =
            CreateSetup(
                peerClient:
                    new HigherTermPeerClient());

        var election =
            setup.State.BeginElection();

        Assert.True(
            setup.State.TryBecomeLeader(
                election.CurrentTerm));

        var result =
            await setup.Submitter.SubmitAsync(
                CreateCommand());

        Assert.Equal(
            RaftCommandSubmissionStatus.NotLeader,
            result.Status);

        var snapshot =
            setup.State.GetSnapshot();

        Assert.Equal(
            RaftNodeRole.Follower,
            snapshot.Role);

        Assert.Equal(2, snapshot.CurrentTerm);
    }

    private static TestSetup CreateSetup(
        IRaftPeerClient? peerClient = null,
        int timeoutMilliseconds = 500,
        int pollMilliseconds = 10)
    {
        var options =
            Options.Create(
                new RaftOptions
                {
                    NodeId = "leader",
                    CommandReplicationTimeoutMilliseconds =
                        timeoutMilliseconds,
                    CommandReplicationPollMilliseconds =
                        pollMilliseconds,
                    Peers =
                    [
                        new RaftPeerOptions
                        {
                            NodeId = "peer-1",
                            BaseUrl = "http://localhost:6002"
                        },
                        new RaftPeerOptions
                        {
                            NodeId = "peer-2",
                            BaseUrl = "http://localhost:6003"
                        }
                    ]
                });

        var logStore =
            new InMemoryLogStore();

        var logManager =
            new RaftLogManager(
                logStore,
                new CoordinatorCommandSerializer());

        var commitManager =
            new RaftCommitManager(
                logManager,
                new InMemoryCommitStateStore());

        var state =
            new RaftNodeState(
                options,
                new InMemoryPersistentStateStore(),
                timeProvider: null,
                logManager: logManager,
                commitManager: commitManager);

        var tracker =
            new RaftReplicationTracker();

        var actualPeerClient =
            peerClient ??
            new SuccessfulPeerClient();

        var heartbeatSender =
            new RaftHeartbeatSender(
                state,
                actualPeerClient,
                logManager,
                tracker,
                commitManager);

        var applier =
            new FakeStateMachineApplier(
                commitManager);

        var submitter =
            new RaftCommandSubmitter(
                state,
                logManager,
                heartbeatSender,
                commitManager,
                applier,
                options);

        return new TestSetup(
            state,
            logStore,
            commitManager,
            submitter);
    }

    private static CreateExperimentCommand
        CreateCommand()
    {
        return new CreateExperimentCommand(
            CommandId: Guid.NewGuid(),
            OccurredAtUtc:
                DateTimeOffset.UtcNow,
            ExperimentId: Guid.NewGuid(),
            EventId: Guid.NewGuid(),
            Name: "Raft submit test",
            Algorithm: "PPO",
            Environment: "CartPole-v1",
            Seed: 42,
            MaxSteps: 1000,
            Priority: 1,
            TimeoutSeconds: 300,
            SimulateFailure: false);
    }

    private sealed record TestSetup(
        RaftNodeState State,
        InMemoryLogStore LogStore,
        RaftCommitManager CommitManager,
        RaftCommandSubmitter Submitter);

    private sealed class FakeStateMachineApplier(
        RaftCommitManager commitManager)
        : IRaftStateMachineApplier
    {
        public Task<RaftApplyResult>
            ApplyCommittedEntriesAsync(
                CancellationToken cancellationToken = default)
        {
            var state =
                commitManager.GetState();

            var appliedCount = 0;

            for (var index =
                    state.LastApplied + 1;
                 index <= state.CommitIndex;
                 index++)
            {
                commitManager.MarkApplied(index);
                appliedCount++;
            }

            return Task.FromResult(
                new RaftApplyResult(
                    appliedCount,
                    commitManager
                        .GetState()
                        .LastApplied));
        }
    }

    private sealed class SuccessfulPeerClient
        : IRaftPeerClient
    {
        public Task<RequestVoteResponse>
            RequestVoteAsync(
                RaftPeerSnapshot peer,
                RequestVoteRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new RequestVoteResponse(
                    request.Term,
                    VoteGranted: true));
        }

        public Task<AppendEntriesResponse>
            AppendEntriesAsync(
                RaftPeerSnapshot peer,
                AppendEntriesRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new AppendEntriesResponse(
                    request.Term,
                    Success: true));
        }
    }

    private sealed class RejectingPeerClient
        : IRaftPeerClient
    {
        public Task<RequestVoteResponse>
            RequestVoteAsync(
                RaftPeerSnapshot peer,
                RequestVoteRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new RequestVoteResponse(
                    request.Term,
                    VoteGranted: false));
        }

        public Task<AppendEntriesResponse>
            AppendEntriesAsync(
                RaftPeerSnapshot peer,
                AppendEntriesRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new AppendEntriesResponse(
                    request.Term,
                    Success: false));
        }
    }

    private sealed class HigherTermPeerClient
        : IRaftPeerClient
    {
        public Task<RequestVoteResponse>
            RequestVoteAsync(
                RaftPeerSnapshot peer,
                RequestVoteRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new RequestVoteResponse(
                    request.Term + 1,
                    VoteGranted: false));
        }

        public Task<AppendEntriesResponse>
            AppendEntriesAsync(
                RaftPeerSnapshot peer,
                AppendEntriesRequest request,
                CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new AppendEntriesResponse(
                    request.Term + 1,
                    Success: false));
        }
    }

    private sealed class InMemoryLogStore
        : IRaftLogStore
    {
        private readonly List<RaftLogEntry>
            _entries = [];

        public RaftLogEntry? Get(long logIndex) =>
            _entries.SingleOrDefault(
                entry =>
                    entry.LogIndex == logIndex);

        public RaftLogEntry? GetLast() =>
            _entries.LastOrDefault();

        public IReadOnlyList<RaftLogEntry> GetFrom(
            long startIndex) =>
            _entries
                .Where(entry =>
                    entry.LogIndex >= startIndex)
                .ToArray();

        public void Append(RaftLogEntry entry)
        {
            _entries.Add(entry);
        }

        public int DeleteFrom(long startIndex)
        {
            return _entries.RemoveAll(
                entry =>
                    entry.LogIndex >= startIndex);
        }
    }

    private sealed class InMemoryCommitStateStore
        : IRaftCommitStateStore
    {
        private RaftCommitState _state =
            new(0, 0);

        public RaftCommitState LoadOrCreate() =>
            _state;

        public void Save(
            long commitIndex,
            long lastApplied)
        {
            _state =
                new RaftCommitState(
                    commitIndex,
                    lastApplied);
        }
    }

    private sealed class
        InMemoryPersistentStateStore
        : IRaftPersistentStateStore
    {
        private RaftPersistentState _state =
            new(0, null);

        public RaftPersistentState LoadOrCreate(
            string nodeId) =>
            _state;

        public void Save(
            string nodeId,
            long currentTerm,
            string? votedFor)
        {
            _state =
                new RaftPersistentState(
                    currentTerm,
                    votedFor);
        }
    }
}