using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace MiddlewareMethodExecutor
{
    public static class Executor
    {
        public static async Task<(object? message, object result)> ExecuteAsyncMethod(Type messageType,
            Type messageHandlerType, string methodName,
            PipeReader pipeReader, IServiceProvider serviceProvider, JsonSerializerOptions jsonSerializerOptions,
            CancellationToken cancellationToken, params object[] parameters)
        {
            object messageHandlerInstance = ActivatorUtilities.CreateInstance(serviceProvider, messageHandlerType);
            MethodInfo? handleAsyncMethod = messageHandlerType.GetMethod(methodName);

            if (handleAsyncMethod == null) throw new MissingMethodException(nameof(messageHandlerType), "HandleAsync");

            object? message = await ReadMessageAsync(pipeReader, messageType, jsonSerializerOptions, cancellationToken);

            var invokeParameters = new object[parameters.Length + 2];
            invokeParameters[0] = message;
            invokeParameters[^1] = cancellationToken;
            for (var i = 0; i < parameters.Length; i++) invokeParameters[i + 1] = parameters[i];

            var result = await (Task<object>) handleAsyncMethod.Invoke(messageHandlerInstance, invokeParameters);

            if (result == null) throw new NullReferenceException("Message execution result cannot be null.");

            return (message, result);
        }

        private static async Task<object?> ReadMessageAsync(PipeReader pipeReader, Type messageType,
            JsonSerializerOptions jsonSerializerOptions, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var readResult = await pipeReader.ReadAsync(cancellationToken);
                var buffer = readResult.Buffer;
                pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (readResult.IsCompleted)
                {
                    return buffer.IsEmpty
                        ? null
                        : buffer.IsSingleSegment
                            ? JsonSerializer.Deserialize(buffer.FirstSpan, messageType, jsonSerializerOptions)
                            : DeserializeSequence(buffer, messageType);
                }
            }

            throw new TaskCanceledException();
        }

        private static object DeserializeSequence(ReadOnlySequence<byte> buffer, Type messageType)
        {
            var jsonReader = new Utf8JsonReader(buffer);
            return JsonSerializer.Deserialize(ref jsonReader, messageType);
        }
    }
}