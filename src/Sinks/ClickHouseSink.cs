using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;
using Serilog.Events;
using Serilog.Parsing;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.Configuration;
using Serilog.Sinks.ClickHouse.Schema;
using SerilogClickHouseSink = Serilog.Sinks.ClickHouse.ClickHouseSink;

namespace LogForwarder.Sinks
{
    /// <summary>
    /// Adapts DNS query-log batches to the official Serilog ClickHouse sink.
    /// Batching is owned exclusively by SinkDispatcher; no Serilog logger or
    /// additional batching queue is created.
    /// </summary>
    public sealed class ClickHouseSink : IOutputSink
    {
        private static readonly MessageTemplate Template =
            new MessageTemplateParser().Parse("DNS query {QName}");

        private readonly IBatchedLogEventSink _sink;
        private readonly bool _ownsSink;
        private bool _disposed;

        public ClickHouseSink(string connectionString, string database, string table,
            IBatchedLogEventSink? sink = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
            ArgumentException.ThrowIfNullOrWhiteSpace(database);
            ArgumentException.ThrowIfNullOrWhiteSpace(table);

            _ownsSink = sink is null;
            _sink = sink ?? new SerilogClickHouseSink(new ClickHouseSinkOptions
            {
                ConnectionString = connectionString,
                Schema = DefaultSchema.Create(table, database)
                    .AddPropertyColumn("ClientIp", "String", "client_ip")
                    .AddPropertyColumn("QName", "String", "q_name")
                    .AddPropertyColumn("QType", "LowCardinality(String)", "q_type")
                    .AddPropertyColumn("ResponseCode", "LowCardinality(String)", "response_code")
                    .AddPropertyColumn("ResponseType", "LowCardinality(String)", "response_type")
                    .AddPropertyColumn("PayloadJson", "String", "payload_json")
                    .Build(),
                TableCreation = new TableCreationOptions
                {
                    Mode = TableCreationMode.None,
                    ValidateOnStartup = true
                }
            });
        }

        public async Task ForwardAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ArgumentNullException.ThrowIfNull(logs);
            token.ThrowIfCancellationRequested();
            if (logs.Count == 0)
                return;

            var events = new LogEvent[logs.Count];
            for (var i = 0; i < logs.Count; i++)
                events[i] = Convert(logs[i]);

            // Upstream IBatchedLogEventSink has no CancellationToken parameter.
            // Do not enable whole-batch retries or impose a false timeout guarantee.
            await _sink.EmitBatchAsync(events).ConfigureAwait(false);
        }

        internal static LogEvent Convert(LogEntry entry)
        {
            ArgumentNullException.ThrowIfNull(entry);
            var properties = new[]
            {
                new LogEventProperty("ClientIp", new ScalarValue(entry.ClientIp)),
                new LogEventProperty("QName", new ScalarValue(entry.Question?.QuestionName ?? "")),
                new LogEventProperty("QType", new ScalarValue(entry.Question?.QuestionType.ToString() ?? "")),
                new LogEventProperty("ResponseCode", new ScalarValue(entry.ResponseCode.ToString())),
                new LogEventProperty("ResponseType", new ScalarValue(entry.ResponseType.ToString())),
                new LogEventProperty("PayloadJson", new ScalarValue(
                    JsonSerializer.Serialize(entry, LogEntry.DnsLogSerializerOptions.Default)))
            };
            return new LogEvent(
                new DateTimeOffset(entry.Timestamp.ToUniversalTime()),
                LogEventLevel.Information, null, Template, properties);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            if (_ownsSink && _sink is IDisposable disposable)
                disposable.Dispose();
        }
    }
}
