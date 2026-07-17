using System.Diagnostics;
using System.Text.Json;
using Contracts;

namespace Worker.Execution;

public sealed class PythonProcessExperimentExecutor(
    IConfiguration configuration)
    : IExperimentExecutor
{
    private readonly string _pythonCommand =
        configuration["Worker:Executor:PythonCommand"]
        ?? "py";

    private readonly string _scriptPath =
        ResolveScriptPath(
            configuration["Worker:Executor:ScriptPath"]
            ?? "Scripts/run_experiment.py");

    public async Task<ExperimentExecutionResult> ExecuteAsync(
        ExperimentResponse experiment,
        Func<
            ExperimentProgressUpdate,
            CancellationToken,
            Task> reportProgressAsync,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        if (!File.Exists(_scriptPath))
        {
            stopwatch.Stop();

            return new ExperimentExecutionResult(
                false,
                $"Python script was not found: {_scriptPath}",
                null,
                stopwatch.ElapsedMilliseconds);
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = _pythonCommand,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        startInfo.ArgumentList.Add(_scriptPath);

        startInfo.ArgumentList.Add("--experiment-id");
        startInfo.ArgumentList.Add(experiment.Id.ToString());

        startInfo.ArgumentList.Add("--algorithm");
        startInfo.ArgumentList.Add(experiment.Algorithm);

        startInfo.ArgumentList.Add("--environment");
        startInfo.ArgumentList.Add(experiment.Environment);

        startInfo.ArgumentList.Add("--seed");
        startInfo.ArgumentList.Add(
            experiment.Seed.ToString());

        startInfo.ArgumentList.Add("--max-steps");
        startInfo.ArgumentList.Add(
            experiment.MaxSteps.ToString());

        if (experiment.SimulateFailure)
        {
            startInfo.ArgumentList.Add(
                "--simulate-failure");
        }

        using var process = new Process
        {
            StartInfo = startInfo
        };

        try
        {
            if (!process.Start())
            {
                stopwatch.Stop();

                return new ExperimentExecutionResult(
                    false,
                    "Python process could not be started.",
                    null,
                    stopwatch.ElapsedMilliseconds);
            }

            var standardOutputTask =
                ReadStandardOutputAsync(
                    process.StandardOutput,
                    reportProgressAsync,
                    cancellationToken);

            var standardErrorTask =
                process.StandardError.ReadToEndAsync(
                    cancellationToken);

            try
            {
                await process.WaitForExitAsync(
                    cancellationToken);

                stopwatch.Stop();
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                if (!process.HasExited)
                {
                    process.Kill(
                        entireProcessTree: true);
                }

                throw;
            }

            var output =
                await standardOutputTask;

            var standardError =
                (await standardErrorTask).Trim();

            if (process.ExitCode == 0)
            {
                var message =
                    string.IsNullOrWhiteSpace(
                        output.Message)
                        ? "Python process completed successfully."
                        : output.Message;

                return new ExperimentExecutionResult(
                    true,
                    message,
                    output.MetricsJson,
                    stopwatch.ElapsedMilliseconds);
            }

            var errorMessage =
                string.IsNullOrWhiteSpace(standardError)
                    ? $"Python process exited with code " +
                    $"{process.ExitCode}."
                    : standardError;

            return new ExperimentExecutionResult(
                false,
                errorMessage,
                null,
                stopwatch.ElapsedMilliseconds);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            stopwatch.Stop();

            return new ExperimentExecutionResult(
                false,
                "Python process could not be executed: " +
                exception.Message,
                null,
                stopwatch.ElapsedMilliseconds);
        }
    }

    private static async Task<PythonOutput>
        ReadStandardOutputAsync(
            StreamReader reader,
            Func<
                ExperimentProgressUpdate,
                CancellationToken,
                Task> reportProgressAsync,
            CancellationToken cancellationToken)
    {
        const string resultPrefix = "RESULT_JSON:";
        const string progressPrefix = "PROGRESS_JSON:";

        var messageLines = new List<string>();
        string? metricsJson = null;

        while (true)
        {
            var line = await reader.ReadLineAsync(
                cancellationToken);

            if (line is null)
            {
                break;
            }

            if (line.StartsWith(
                resultPrefix,
                StringComparison.Ordinal))
            {
                metricsJson =
                    line[resultPrefix.Length..];

                continue;
            }

            if (line.StartsWith(
                progressPrefix,
                StringComparison.Ordinal))
            {
                var progressJson =
                    line[progressPrefix.Length..];

                if (TryParseProgress(
                    progressJson,
                    out var progress))
                {
                    await reportProgressAsync(
                        progress,
                        cancellationToken);
                }

                continue;
            }

            messageLines.Add(line);
        }

        return new PythonOutput(
            string.Join(
                Environment.NewLine,
                messageLines),
            metricsJson);
    }

    private static bool TryParseProgress(
        string progressJson,
        out ExperimentProgressUpdate progress)
    {
        try
        {
            using var document =
                JsonDocument.Parse(progressJson);

            var root = document.RootElement;

            if (!root.TryGetProperty(
                    "currentStep",
                    out var currentStepElement) ||
                !currentStepElement.TryGetInt32(
                    out var currentStep))
            {
                progress = default!;
                return false;
            }

            string? metricsJson = null;

            if (root.TryGetProperty(
                "metrics",
                out var metricsElement))
            {
                metricsJson =
                    metricsElement.GetRawText();
            }

            progress = new ExperimentProgressUpdate(
                currentStep,
                metricsJson);

            return true;
        }
        catch (JsonException)
        {
            progress = default!;
            return false;
        }
    }

    private sealed record PythonOutput(
        string Message,
        string? MetricsJson);

    private static string ResolveScriptPath(
        string configuredPath)
    {
        if (Path.IsPathRooted(configuredPath))
        {
            return configuredPath;
        }

        return Path.Combine(
            AppContext.BaseDirectory,
            configuredPath);
    }
}