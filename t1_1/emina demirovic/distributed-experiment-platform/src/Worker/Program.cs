using Worker;
using Worker.Execution;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHttpClient();

var executorMode =
    builder.Configuration["Worker:Executor:Mode"]
    ?? "Simulated";

if (executorMode.Equals(
        "Process",
        StringComparison.OrdinalIgnoreCase))
{
    builder.Services.AddSingleton<
        IExperimentExecutor,
        PythonProcessExperimentExecutor>();
}
else
{
    builder.Services.AddSingleton<
        IExperimentExecutor,
        SimulatedExperimentExecutor>();
}

builder.Services.AddHostedService<WorkerService>();

var host = builder.Build();
host.Run();