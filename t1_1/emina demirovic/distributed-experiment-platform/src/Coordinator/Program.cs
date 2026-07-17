using Coordinator.Services;
using Coordinator.Commands;
using Coordinator.Data;
using Coordinator.Raft;
using Microsoft.Extensions.Options;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

var configuredDatabasePath =
    builder.Configuration[
        "Coordinator:DatabasePath"];

var databasePath =
    string.IsNullOrWhiteSpace(
        configuredDatabasePath)
        ? Path.Combine(
            builder.Environment.ContentRootPath,
            "Data",
            "coordinator.db")
        : Path.GetFullPath(
            configuredDatabasePath,
            builder.Environment.ContentRootPath);

builder.Services.AddDbContextFactory<CoordinatorDbContext>(
    options => options.UseSqlite(
        $"Data Source={databasePath}",
        sqliteOptions => sqliteOptions.MigrationsAssembly(
            typeof(CoordinatorDbContext).Assembly.FullName)));

Directory.CreateDirectory(
    Path.GetDirectoryName(databasePath)!);
        
// Add services to the container.

builder.Services.AddControllers();

// RAFT configuration and services
builder.Services.AddSingleton<
    IValidateOptions<RaftOptions>,
    RaftOptionsValidator>();

builder.Services.AddOptions<RaftOptions>()
    .Bind(
        builder.Configuration.GetSection(
            RaftOptions.SectionName))
    .ValidateOnStart();

builder.Services.AddSingleton<
    IRaftPersistentStateStore,
    SqliteRaftPersistentStateStore>();

builder.Services.AddSingleton<
    IRaftLogStore,
    SqliteRaftLogStore>();

builder.Services.AddSingleton<CoordinatorCommandSerializer>();

builder.Services.AddSingleton<RaftLogManager>();

builder.Services.AddSingleton<RaftReplicationTracker>();

builder.Services.AddSingleton<
    IRaftCommitStateStore,
    SqliteRaftCommitStateStore>();

builder.Services.AddSingleton<RaftCommitManager>();

builder.Services.AddSingleton<RaftStateMachineApplier>();

builder.Services.AddSingleton<IRaftStateMachineApplier>(
    provider =>
        provider.GetRequiredService<
            RaftStateMachineApplier>());

builder.Services.AddSingleton<
    RaftCommandSubmitter>();

builder.Services.AddHostedService<RaftStateMachineBackgroundService>();

builder.Services.AddSingleton<RaftNodeState>();

builder.Services.AddHttpClient(
    "RaftPeer",
    client =>
    {
        client.Timeout =
            TimeSpan.FromSeconds(2);
    });

builder.Services.AddSingleton<
    IRaftPeerClient,
    HttpRaftPeerClient>();

builder.Services.AddSingleton<
    RaftHeartbeatSender>();

builder.Services.AddHostedService<
    RaftHeartbeatBackgroundService>();

builder.Services.AddHostedService<
    RaftElectionBackgroundService>();

builder.Services.AddSingleton<
    RaftElectionService>();

builder.Services.AddSingleton<WorkerRegistry>();
builder.Services.AddSingleton<ExperimentRegistry>();
builder.Services.AddSingleton<CoordinatorCommandProcessor>();

builder.Services.AddSingleton<ExperimentSchedulerService>();

builder.Services.AddHostedService(
    provider =>
        provider.GetRequiredService<
            ExperimentSchedulerService>());

builder.Services.AddSingleton<ExperimentStartupRecoveryService>();

builder.Services.AddHostedService(
    provider =>
        provider.GetRequiredService<
            ExperimentStartupRecoveryService>());

builder.Services.AddSingleton<ExperimentRecoveryService>();

builder.Services.AddHostedService(
    provider =>
        provider.GetRequiredService<
            ExperimentRecoveryService>());

builder.Services.AddOpenApi();

var app = builder.Build();
        
using (var scope = app.Services.CreateScope())
{
    var dbContextFactory = scope.ServiceProvider
        .GetRequiredService<
            IDbContextFactory<CoordinatorDbContext>>();

    await using var dbContext =
        await dbContextFactory.CreateDbContextAsync();

    await dbContext.Database.MigrateAsync();
}

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();

public partial class Program
{
    
}