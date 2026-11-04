/*
Technitium DNS Server
Copyright (C) 2025  Shreyas Zare (shreyas@technitium.com)
Copyright (C) 2025  Zafer Balkan (zafer@zaferbalkan.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

*/

using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Text.Json.Serialization;
using TechnitiumLibrary.Net.Dns;

namespace LogExporter
{
    public class AppConfig
    {
        [Required]
        [JsonPropertyName("sinks")]
        public SinkConfig Sinks { get; set; } = null!;

        [Required]
        [JsonPropertyName("pipeline")]
        public PipelineConfig Pipeline { get; set; } = null!;

        /// <summary>
        /// Loads config and enforces DataAnnotations validation.
        ///<para>
        /// ADR: Validation is intentionally centralized here so that:
        ///   - App receives only a fully valid configuration.
        ///   - Errors surface early with domain-specific messages.
        ///   - No runtime failures occur deep inside the logging pipeline.
        /// This ensures plugin initialization is deterministic and safe.
        /// </para>
        /// </summary>
        public static AppConfig Deserialize(string json)
        {
            var config = JsonSerializer.Deserialize<AppConfig>(json, DnsConfigSerializerOptions.Default)
                         ?? throw new DnsClientException("Configuration could not be deserialized.");

            ValidateObject(config);
            ValidateObject(config.Sinks);
            ValidateObject(config.Pipeline);
            ValidateEnabledFeatures(config);

            return config;
        }

        internal static void ValidateObject(object instance)
        {
            var ctx = new ValidationContext(instance);
            Validator.ValidateObject(instance, ctx, validateAllProperties: true);
        }

        private static void ValidateEnabledSinkBatching(SinkConfig sinks)
        {
            foreach (var feature in new FeatureBase?[]
            {
                sinks.ConsoleSinkConfig, sinks.FileSinkConfig,
                sinks.HttpSinkConfig, sinks.SyslogSinkConfig, sinks.ClickHouseSinkConfig
            })
            {
                if (feature?.Enabled is not true || feature.Batching is null)
                {
                    continue;
                }

                ValidateObject(feature.Batching);

                if (feature.Batching.MaxRetryDelayMs < feature.Batching.InitialRetryDelayMs)
                {
                    throw new ValidationException(
                        "batching.maxRetryDelayMs must be >= initialRetryDelayMs.");
                }
            }
        }

        private static void ValidateEnabledFeatures(AppConfig config)
        {
            ValidateEnabledSinkBatching(config.Sinks);

            if (config.Sinks.FileSinkConfig?.Enabled is true)
            {
                ValidateObject(config.Sinks.FileSinkConfig);
            }

            if (config.Sinks.HttpSinkConfig?.Enabled is true)
            {
                ValidateObject(config.Sinks.HttpSinkConfig);
            }

            if (config.Sinks.ClickHouseSinkConfig?.Enabled is true)
            {
                ValidateObject(config.Sinks.ClickHouseSinkConfig);
                if (config.Sinks.ClickHouseSinkConfig.AllowBatchReplay)
                    throw new ValidationException(
                        "clickhouse.allowBatchReplay is not supported by the Serilog adapter.");
            }

            if (config.Sinks.SyslogSinkConfig?.Enabled is true)
            {
                var syslog = config.Sinks.SyslogSinkConfig;
                ValidateObject(syslog);

                if (string.IsNullOrWhiteSpace(syslog.Protocol))
                {
                    throw new ValidationException(
                        "syslog.protocol is required when syslog logging is enabled.");
                }

                if (!syslog.Protocol.Equals("LOCAL", System.StringComparison.OrdinalIgnoreCase) &&
                    string.IsNullOrWhiteSpace(syslog.Address))
                {
                    throw new ValidationException(
                        "syslog.address is required for UDP, TCP, and TLS syslog logging.");
                }
            }

            if (config.Pipeline.TaggingProcessConfig?.Enabled is true)
            {
                ValidateObject(config.Pipeline.TaggingProcessConfig);
            }
        }
    }
    public class BatchSettings
    {
        [Range(1, int.MaxValue)]
        [JsonPropertyName("batchSize")]
        public int BatchSize { get; set; } = 1000;

        [Range(1, int.MaxValue)]
        [JsonPropertyName("batchDelayMs")]
        public int BatchDelayMs { get; set; } = 25;

        [Range(0, int.MaxValue)]
        [JsonPropertyName("retryTimeLimitMs")]
        public int RetryTimeLimitMs { get; set; } = 2000;

        [Range(1, int.MaxValue)]
        [JsonPropertyName("initialRetryDelayMs")]
        public int InitialRetryDelayMs { get; set; } = 50;

        [Range(1, int.MaxValue)]
        [JsonPropertyName("maxRetryDelayMs")]
        public int MaxRetryDelayMs { get; set; } = 500;
    }

    public class FeatureBase
    {
        [JsonPropertyName("batching")]
        public BatchSettings? Batching { get; set; }
        [JsonPropertyName("enabled")]
        public bool Enabled { get; set; } = true;
    }

    public class SinkConfig
    {
        [Range(1, int.MaxValue, ErrorMessage = "maxQueueSize must be greater than zero.")]
        [JsonPropertyName("maxQueueSize")]
        public int MaxQueueSize { get; set; } = 100000;

        [JsonPropertyName("enableEdnsLogging")]
        public bool EnableEdnsLogging { get; set; } = true;

        [JsonPropertyName("console")]
        public ConsoleSink? ConsoleSinkConfig { get; set; }

        [JsonPropertyName("file")]
        public FileSink? FileSinkConfig { get; set; }

        [JsonPropertyName("http")]
        public HttpSink? HttpSinkConfig { get; set; }

        [JsonPropertyName("syslog")]
        public SyslogSink? SyslogSinkConfig { get; set; }

        [JsonPropertyName("clickhouse")]
        public ClickHouseConfig? ClickHouseSinkConfig { get; set; }

        public class SyslogSink : FeatureBase
        {
            [JsonPropertyName("address")]
            public string? Address { get; set; }

            [Range(1, 65535)]
            [JsonPropertyName("port")]
            public int? Port { get; set; }

            [AllowedValues(["UDP", "TCP", "TLS", "LOCAL"])]
            [JsonPropertyName("protocol")]
            public string? Protocol { get; set; }
        }

        public class ClickHouseConfig : FeatureBase
        {
            [Required]
            [JsonPropertyName("connectionString")]
            public string? ConnectionString { get; set; }

            [Required]
            [RegularExpression("^[A-Za-z_][A-Za-z0-9_]*$")]
            [JsonPropertyName("database")]
            public string? Database { get; set; }

            [Required]
            [RegularExpression("^[A-Za-z_][A-Za-z0-9_]*$")]
            [JsonPropertyName("table")]
            public string? Table { get; set; }

            // Explicitly reject replay requests: the upstream Serilog sink has no
            // cancellation-token API and cannot ensure replay safety after ambiguous writes.
            [JsonPropertyName("allowBatchReplay")]
            public bool AllowBatchReplay { get; set; }
        }

        public class ConsoleSink : FeatureBase;

        public class FileSink : FeatureBase
        {
            [Required(ErrorMessage = "file.path is required when file logging is enabled.")]
            [JsonPropertyName("path")]
            public string? Path { get; set; }
        }

        public class HttpSink : FeatureBase
        {
            [JsonPropertyName("allowBatchReplay")]
            public bool AllowBatchReplay { get; set; }


            [Required(ErrorMessage = "http.endpoint is required when HTTP logging is enabled.")]
            [Url]
            [JsonPropertyName("endpoint")]
            public string? Endpoint { get; set; }

            [JsonPropertyName("headers")]
            public Dictionary<string, string?>? Headers { get; set; }
        }
    }

    public class PipelineConfig
    {
        [JsonPropertyName("normalize")]
        public NormalizeProcess? NormalizeProcessConfig { get; set; }

        [JsonPropertyName("tagging")]
        public TaggingProcess? TaggingProcessConfig {  get; set; }

        public class NormalizeProcess : FeatureBase;

        public class TaggingProcess : FeatureBase
        {
            [Required(ErrorMessage = "tags are required when tagging is enabled.")]
            [MinLength(1, ErrorMessage = "at least one tag is required when tagging is enabled.")]
            [JsonPropertyName("tags")]
            public List<string> Tags { get; set; } = new();
        }

    }
    /// <summary>
    /// Shared serializer configuration for reading dnsApp.config.
    /// ADR: The serializer options are centralized so that parsing behavior
    /// is stable and predictable across the entire plugin lifetime.
    /// </summary>
    public static class DnsConfigSerializerOptions
    {
        public static readonly JsonSerializerOptions Default = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase, // Convert properties to camelCase
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping, // For safe encoding
            NumberHandling = JsonNumberHandling.Strict,
            AllowTrailingCommas = true, // Allow trailing commas in JSON
            DictionaryKeyPolicy = JsonNamingPolicy.CamelCase, // Convert dictionary keys to camelCase
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull // Ignore null values
        };
    }
}
