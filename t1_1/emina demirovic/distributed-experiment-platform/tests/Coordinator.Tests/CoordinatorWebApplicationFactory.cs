using Coordinator.Data;
using Coordinator.Services;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Coordinator.Tests;

public sealed class CoordinatorWebApplicationFactory
    : WebApplicationFactory<Program>
{
    private readonly string _databaseDirectory;
    private readonly string _databasePath;
    private readonly bool _disableScheduler;
    private readonly bool _disableRecovery;
    private readonly bool _deleteDatabaseOnDispose;

    public CoordinatorWebApplicationFactory(
        bool disableScheduler = true,
        bool disableRecovery = true,
        string? databasePath = null,
        bool deleteDatabaseOnDispose = true)
    {
        _disableScheduler = disableScheduler;
        _disableRecovery = disableRecovery;
        _deleteDatabaseOnDispose = deleteDatabaseOnDispose;

        if (string.IsNullOrWhiteSpace(databasePath))
        {
            _databaseDirectory = Path.Combine(
                Path.GetTempPath(),
                "distributed-experiment-platform-tests",
                Guid.NewGuid().ToString("N"));

            _databasePath = Path.Combine(
                _databaseDirectory,
                "coordinator-tests.db");
        }
        else
        {
            _databasePath = Path.GetFullPath(databasePath);

            _databaseDirectory =
                Path.GetDirectoryName(_databasePath)
                ?? throw new InvalidOperationException(
                    "The test database directory could not be determined.");
        }

        Directory.CreateDirectory(_databaseDirectory);
    }

    protected override void ConfigureWebHost(
        IWebHostBuilder builder)
    {

        builder.UseEnvironment("Testing");

        builder.ConfigureServices(services =>
        {
            if (_disableScheduler)
            {
                var schedulerDescriptor = services.SingleOrDefault(
                    descriptor =>
                        descriptor.ServiceType ==
                            typeof(IHostedService) &&
                        descriptor.ImplementationType ==
                            typeof(ExperimentSchedulerService));

                if (schedulerDescriptor is not null)
                {
                    services.Remove(schedulerDescriptor);
                }
            }

            if (_disableRecovery)
            {
                var recoveryDescriptor = services.SingleOrDefault(
                    descriptor =>
                        descriptor.ServiceType ==
                            typeof(IHostedService) &&
                        descriptor.ImplementationType ==
                            typeof(ExperimentRecoveryService));

                if (recoveryDescriptor is not null)
                {
                    services.Remove(recoveryDescriptor);
                }
            }

            services.RemoveAll<
                IDbContextFactory<CoordinatorDbContext>>();

            services.RemoveAll<
                DbContextOptions<CoordinatorDbContext>>();

            services.AddDbContextFactory<CoordinatorDbContext>(
                options => options.UseSqlite(
                    $"Data Source={_databasePath};Pooling=False",
                    sqliteOptions =>
                        sqliteOptions.MigrationsAssembly(
                            typeof(CoordinatorDbContext)
                                .Assembly.FullName)));
        });
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (disposing &&
            _deleteDatabaseOnDispose &&
            Directory.Exists(_databaseDirectory))
        {
            Directory.Delete(
                _databaseDirectory,
                recursive: true);
        }
    }
}