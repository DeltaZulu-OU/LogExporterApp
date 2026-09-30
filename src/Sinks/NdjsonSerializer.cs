using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace LogExporter.Sinks
{
    /// <summary>
    /// ADR: NDJSON serialization is used by all export strategies and must remain
    /// consistent across sinks. Previously, each strategy copy/pasted its own
    /// serialization loop, creating long-term maintenance and drift risks.
    /// This helper centralizes NDJSON formatting so changes occur in one place,
    /// ensuring consistency, reducing boilerplate, and eliminating subtle bugs.
    /// </summary>
    public static class NdjsonSerializer
    {
        public static void WriteBatch(Stream target, IReadOnlyList<LogEntry> logs) =>
            WriteBatch(target, logs, LogEntry.DnsLogSerializerOptions.Default);

        public static void WriteBatch(
            Stream target,
            IReadOnlyList<LogEntry> logs,
            JsonSerializerOptions options)
        {
            using var writer = CreateWriter(target);

            for (var i = 0; i < logs.Count; i++)
            {
                JsonSerializer.Serialize(writer, logs[i], options);
                CompleteRecord(writer, target);
            }
        }

        private static Utf8JsonWriter CreateWriter(Stream target) =>
            new Utf8JsonWriter(target, new JsonWriterOptions
            {
                Indented = false,
                SkipValidation = false,
                NewLine = "\n"
            });

        private static void CompleteRecord(Utf8JsonWriter writer, Stream target)
        {
            writer.Flush();
            target.WriteByte((byte)'\n');
            writer.Reset();
        }
    }
}
