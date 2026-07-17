using System.Text.Json;
using Coordinator.Commands;

namespace Coordinator.Raft;

public sealed class CoordinatorCommandSerializer
{
    private static readonly JsonSerializerOptions
        SerializerOptions =
            new(JsonSerializerDefaults.Web);

    private static readonly IReadOnlyDictionary<
        string,
        Type> CommandTypes =
            new Dictionary<string, Type>(
                StringComparer.Ordinal)
            {
                [nameof(CreateExperimentCommand)] =
                    typeof(CreateExperimentCommand),

                [nameof(AssignExperimentCommand)] =
                    typeof(AssignExperimentCommand),

                [nameof(CompleteExperimentCommand)] =
                    typeof(CompleteExperimentCommand),

                [nameof(
                    RequestExperimentCancellationCommand)] =
                    typeof(
                        RequestExperimentCancellationCommand),

                [nameof(RequeueExperimentCommand)] =
                    typeof(RequeueExperimentCommand),

                [nameof(
                    RecoverExperimentOnStartupCommand)] =
                    typeof(
                        RecoverExperimentOnStartupCommand)
            };

    public RaftSerializedCommand Serialize(
        CoordinatorCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);

        var commandType =
            command.GetType();

        var commandTypeName =
            commandType.Name;

        if (!CommandTypes.TryGetValue(
                commandTypeName,
                out var supportedType) ||
            supportedType != commandType)
        {
            throw new InvalidOperationException(
                $"Coordinator command type " +
                $"'{commandType.FullName}' is not supported.");
        }

        var payloadJson =
            JsonSerializer.Serialize(
                command,
                commandType,
                SerializerOptions);

        return new RaftSerializedCommand(
            commandTypeName,
            payloadJson);
    }

    public CoordinatorCommand Deserialize(
        string commandType,
        string payloadJson)
    {
        if (string.IsNullOrWhiteSpace(commandType))
        {
            throw new ArgumentException(
                "CommandType is required.",
                nameof(commandType));
        }

        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            throw new ArgumentException(
                "PayloadJson is required.",
                nameof(payloadJson));
        }

        if (!CommandTypes.TryGetValue(
                commandType,
                out var targetType))
        {
            throw new InvalidOperationException(
                $"Coordinator command type " +
                $"'{commandType}' is not supported.");
        }

        try
        {
            var command =
                JsonSerializer.Deserialize(
                    payloadJson,
                    targetType,
                    SerializerOptions);

            return command as CoordinatorCommand
                ?? throw new InvalidOperationException(
                    $"Payload for command type " +
                    $"'{commandType}' could not be deserialized.");
        }
        catch (JsonException exception)
        {
            throw new InvalidOperationException(
                $"Payload for command type " +
                $"'{commandType}' is invalid.",
                exception);
        }
    }
}