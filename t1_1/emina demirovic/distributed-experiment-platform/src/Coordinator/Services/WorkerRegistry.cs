using System.Collections.Concurrent;
using Contracts;

namespace Coordinator.Services;

public sealed class WorkerRegistry
{
    private static readonly TimeSpan OnlineTimeout =
        TimeSpan.FromSeconds(15);

    private readonly ConcurrentDictionary<string, WorkerStatusResponse> _workers =
        new();

    public WorkerStatusResponse Register(string workerId)
    {
        var now = DateTimeOffset.UtcNow;

        return _workers.AddOrUpdate(
            workerId,
            _ => new WorkerStatusResponse
            {
                WorkerId = workerId,
                RegisteredAtUtc = now,
                LastHeartbeatAtUtc = now,
                IsOnline = true
            },
            (_, existingWorker) => new WorkerStatusResponse
            {
                WorkerId = existingWorker.WorkerId,
                RegisteredAtUtc = existingWorker.RegisteredAtUtc,
                LastHeartbeatAtUtc = now,
                IsOnline = true
            });
    }

    public WorkerStatusResponse? Heartbeat(string workerId)
    {
        if (!_workers.TryGetValue(workerId, out var existingWorker))
        {
            return null;
        }

        var updatedWorker = new WorkerStatusResponse
        {
            WorkerId = existingWorker.WorkerId,
            RegisteredAtUtc = existingWorker.RegisteredAtUtc,
            LastHeartbeatAtUtc = DateTimeOffset.UtcNow,
            IsOnline = true
        };

        _workers[workerId] = updatedWorker;

        return updatedWorker;
    }

    public WorkerStatusResponse? GetFirstOnline()
    {
        var now = DateTimeOffset.UtcNow;

        return _workers.Values
            .Where(worker =>
                now - worker.LastHeartbeatAtUtc <= OnlineTimeout)
            .OrderBy(worker => worker.WorkerId)
            .Select(worker => new WorkerStatusResponse
            {
                WorkerId = worker.WorkerId,
                RegisteredAtUtc = worker.RegisteredAtUtc,
                LastHeartbeatAtUtc = worker.LastHeartbeatAtUtc,
                IsOnline = true
            })
            .FirstOrDefault();
    }

    public IReadOnlyCollection<WorkerStatusResponse> GetOnlineWorkers()
    {
        var now = DateTimeOffset.UtcNow;

        return _workers.Values
            .Where(worker =>
                now - worker.LastHeartbeatAtUtc <= OnlineTimeout)
            .OrderBy(worker => worker.WorkerId)
            .Select(worker => new WorkerStatusResponse
            {
                WorkerId = worker.WorkerId,
                RegisteredAtUtc = worker.RegisteredAtUtc,
                LastHeartbeatAtUtc = worker.LastHeartbeatAtUtc,
                IsOnline = true
            })
            .ToArray();
    }
    
    public IReadOnlyCollection<WorkerStatusResponse> GetAll()
    {
        var now = DateTimeOffset.UtcNow;

        return _workers.Values
            .Select(worker => new WorkerStatusResponse
            {
                WorkerId = worker.WorkerId,
                RegisteredAtUtc = worker.RegisteredAtUtc,
                LastHeartbeatAtUtc = worker.LastHeartbeatAtUtc,
                IsOnline = now - worker.LastHeartbeatAtUtc <= OnlineTimeout
            })
            .OrderBy(worker => worker.WorkerId)
            .ToArray();
    }

    public bool IsOnline(string workerId)
    {
        if (!_workers.TryGetValue(workerId, out var worker))
        {
            return false;
        }

        return DateTimeOffset.UtcNow - worker.LastHeartbeatAtUtc
            <= OnlineTimeout;
    }
    
}