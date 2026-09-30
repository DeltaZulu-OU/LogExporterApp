/*
Technitium DNS Server
Copyright (C) 2025  Shreyas Zare
Copyright (C) 2025  Zafer Balkan

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

using DnsServerCore.ApplicationCommon;
using LogExporter.Pipeline;
using LogExporter.Sinks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TechnitiumLibrary.Net.Dns;

namespace LogExporter
{
    public sealed class App : IDnsApplication, IDnsQueryLogger
    {
        #region variables

        private const int BULK_INSERT_COUNT = 1000;

        private readonly SinkDispatcher _sinkDispatcher;
        private readonly PipelineDispatcher _enrichmentDispatcher;

        private sealed class IngestionState
        {
            public IngestionState(Channel<LogEntry> channel, bool enableEdnsLogging)
            {
                Channel = channel;
                EnableEdnsLogging = enableEdnsLogging;
            }

            public Channel<LogEntry> Channel { get; }
            public bool EnableEdnsLogging { get; }
        }

        private readonly SemaphoreSlim _lifecycleLock = new SemaphoreSlim(1, 1);
        private IngestionState? _ingestionState;
        private Task? _backgroundTask;
        private CancellationTokenSource? _pipelineCancellation;
        private AppConfig? _config;
        private int _disposed;
        private IDnsServer? _dnsServer;

        private long _droppedCount;
        private static readonly TimeSpan DropLogInterval = TimeSpan.FromSeconds(5);
        private long _lastDropTicks;

        /// <summary>
        /// Completes on the first query log to signal that the DNS server is serving.
        /// </summary>
        /// <remarks>
        /// <c>IDnsServer</c> has no readiness event, and Technitium loads apps before it binds
        /// its listeners. Waiting inside <see cref="InitializeAsync"/> would deadlock startup,
        /// because the server waits for app initialization before it starts listening. The first
        /// logged query is the earliest proof that the server answers queries, so sinks that must
        /// resolve names (see <see cref="SyslogSink"/>) wait on this task in the background.
        /// It outlives config reloads because the server stays up while this instance is
        /// reinitialized. Continuations run asynchronously so they never execute on the query path.
        /// </remarks>
        private readonly TaskCompletionSource _dnsServerReady =
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        #endregion variables

        #region constructor

        public App()
        {
            _sinkDispatcher = new SinkDispatcher();
            _enrichmentDispatcher = new PipelineDispatcher();
            _lastDropTicks = DateTime.UtcNow.Ticks;
        }

        #endregion constructor

        #region IDisposable

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _lifecycleLock.Wait();
            try
            {
                try
                {
                    StopPipelineAsync().GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    _dnsServer?.WriteLog(ex);
                }

                try
                {
                    _sinkDispatcher.Dispose();
                }
                catch (Exception ex)
                {
                    _dnsServer?.WriteLog(ex);
                }

                try
                {
                    _enrichmentDispatcher.Dispose();
                }
                catch (Exception ex)
                {
                    _dnsServer?.WriteLog(ex);
                }

            }
            finally
            {
                _lifecycleLock.Release();
            }
        }

        #endregion IDisposable

        #region public

        public async Task InitializeAsync(IDnsServer dnsServer, string config)
        {
            await _lifecycleLock.WaitAsync().ConfigureAwait(false);
            try
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

                _dnsServer = dnsServer;

                AppConfig nextConfig;
                try
                {
                    nextConfig = AppConfig.Deserialize(config)
                                 ?? throw new DnsClientException("Invalid application configuration.");
                }
                catch (Exception ex)
                {
                    // Reject invalid configuration without stopping the active generation.
                    _dnsServer?.WriteLog(ex);
                    throw;
                }

                // A config update reuses this App instance. Stop the current generation
                // only after the replacement configuration has been validated.
                await StopPipelineAsync().ConfigureAwait(false);

                try
                {
                    _config = nextConfig;
                    ConfigurePipeline();
                    ConfigureSinks();
                }
                catch (Exception ex)
                {
                    _dnsServer?.WriteLog(ex);
                    throw;
                }

                if (!_sinkDispatcher.Any())
                {
                    return;
                }

                var transformChannel = Channel.CreateBounded<LogEntry>(
                    new BoundedChannelOptions(_config!.Sinks.MaxQueueSize)
                    {
                        SingleReader = true,
                        SingleWriter = false,
                        FullMode = BoundedChannelFullMode.DropWrite
                    },
                    _ => IncrementDropAndMaybeLog());

                var enrichedChannel = Channel.CreateBounded<LogEntry>(
                    new BoundedChannelOptions(_config.Sinks.MaxQueueSize)
                    {
                        SingleReader = true,
                        SingleWriter = true,
                        FullMode = BoundedChannelFullMode.DropWrite
                    },
                    _ => IncrementDropAndMaybeLog());

                var pipelineCancellation = new CancellationTokenSource();

                _pipelineCancellation = pipelineCancellation;

                // Workers capture this generation's channels and cancellation token.
                // They never re-read mutable channel fields, so an old worker cannot
                // hop onto replacement channels.
                _backgroundTask = Task.WhenAll(
                    Task.Run(() => EnrichLogsAsync(
                        transformChannel.Reader,
                        enrichedChannel.Writer,
                        pipelineCancellation.Token)),
                    Task.Run(() => ExportLogsAsync(
                        enrichedChannel.Reader,
                        pipelineCancellation.Token)));

                Volatile.Write(
                    ref _ingestionState,
                    new IngestionState(transformChannel, _config.Sinks.EnableEdnsLogging));
            }
            finally
            {
                _lifecycleLock.Release();
            }
        }

        // Step 1: input
        public Task InsertLogAsync(DateTime timestamp, DnsDatagram request,
            IPEndPoint remoteEP, DnsTransportProtocol protocol,
            DnsDatagram response)
        {
            // Signalled before the ingestion-state check: readiness describes the server, not this
            // app's state, and must hold for a later reload that enables a syslog sink. The
            // IsCompleted read keeps the per-query cost to a plain field read once signalled.
            if (!_dnsServerReady.Task.IsCompleted)
            {
                _dnsServerReady.TrySetResult();
            }

            var state = Volatile.Read(ref _ingestionState);
            if (state is null)
            {
                return Task.CompletedTask;
            }

            LogEntry entry;

            try
            {
                // input -> transform: build LogEntry
                entry = new LogEntry(timestamp, remoteEP, protocol, request, response, state.EnableEdnsLogging);
            }
            catch (Exception ex)
            {
                // Malformed packet or unexpected data should not crash the server.
                _dnsServer?.WriteLog(ex);
                return Task.CompletedTask;
            }

            try
            {
                state.Channel.Writer.TryWrite(entry);
            }
            catch (Exception ex)
            {
                _dnsServer?.WriteLog(ex);
            }

            return Task.CompletedTask;
        }

        #endregion public

        #region private

        private async Task StopPipelineAsync()
        {
            var ingestionState = Interlocked.Exchange(ref _ingestionState, null);
            var backgroundTask = Interlocked.Exchange(ref _backgroundTask, null);
            var pipelineCancellation = Interlocked.Exchange(ref _pipelineCancellation, null);

            // Completing the input channel lets both workers drain accepted entries in order.
            // Cancellation is reserved for a future forced-abort path; normal stop is graceful.
            ingestionState?.Channel.Writer.TryComplete();

            try
            {
                if (backgroundTask is not null)
                {
                    await backgroundTask.ConfigureAwait(false);
                }

                await _sinkDispatcher.DrainAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (pipelineCancellation?.IsCancellationRequested is true)
            {
                // Expected when stopping a pipeline generation.
            }
            finally
            {
                pipelineCancellation?.Dispose();
            }
        }

        // Step 2: EnrichLogsAsync – transform -> enrich
        private async Task EnrichLogsAsync(
            ChannelReader<LogEntry> transformReader,
            ChannelWriter<LogEntry> enrichedWriter,
            CancellationToken token)
        {
            try
            {
                while (await transformReader.WaitToReadAsync(token).ConfigureAwait(false))
                {
                    while (transformReader.TryRead(out var entry))
                    {
                        token.ThrowIfCancellationRequested();

                        // If there is no question, most enrichers cannot do anything.
                        if (entry.Question != null && _enrichmentDispatcher.Any())
                        {
                            try
                            {
                                _enrichmentDispatcher.Run(entry, ex => _dnsServer?.WriteLog(ex));
                            }
                            catch (Exception ex)
                            {
                                // Extra guard: dispatcher itself should not tear down the loop.
                                _dnsServer?.WriteLog(ex);
                            }
                        }

                        try
                        {
                            enrichedWriter.TryWrite(entry);
                        }
                        catch (Exception ex)
                        {
                            _dnsServer?.WriteLog(ex);
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Expected when stopping a pipeline generation.
            }
            catch (Exception ex)
            {
                _dnsServer?.WriteLog(ex);
            }
            finally
            {
                // Signal no more enriched entries will be produced.
                try
                {
                    enrichedWriter.TryComplete();
                }
                catch (Exception ex)
                {
                    _dnsServer?.WriteLog(ex);
                }
            }
        }

        // Step 3: ExportLogsAsync – pipeline -> output
        private async Task ExportLogsAsync(ChannelReader<LogEntry> enrichedReader, CancellationToken token)
        {
            // ADR: Reuse this list buffer to avoid GC churn during high-volume logging.
            var batch = new List<LogEntry>(BULK_INSERT_COUNT);

            try
            {
                while (await enrichedReader.WaitToReadAsync(token).ConfigureAwait(false))
                {
                    while (batch.Count < BULK_INSERT_COUNT &&
                           enrichedReader.TryRead(out var entry))
                    {
                        token.ThrowIfCancellationRequested();
                        batch.Add(entry);
                    }

                    if (batch.Count > 0)
                    {
                        try
                        {
                            await _sinkDispatcher
                                .DispatchAsync(batch, token)
                                .ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (token.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (Exception ex)
                        {
                            // Sink failures must be logged but must not crash the server.
                            _dnsServer?.WriteLog(ex);
                        }
                        finally
                        {
                            batch.Clear(); // REUSE — do not reassign
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Expected when stopping a pipeline generation.
            }
            catch (Exception ex)
            {
                _dnsServer?.WriteLog(ex);
            }
        }

        /// <summary>
        /// Replaces every sink according to the current configuration.
        /// </summary>
        /// <remarks>
        /// Each sink is configured in isolation. A sink that cannot be created is reported with a
        /// concise, sink-specific message and skipped, so one broken target (bad path, invalid
        /// endpoint) does not disable every other sink or abort <see cref="InitializeAsync"/>.
        /// </remarks>
        private void ConfigureSinks()
        {
            var sinks = _config!.Sinks;

            ConfigureConsoleSink(sinks.ConsoleSinkConfig, sinks.MaxQueueSize);
            ConfigureFileSink(sinks.FileSinkConfig, sinks.MaxQueueSize);
            ConfigureHttpSink(sinks.HttpSinkConfig, sinks.MaxQueueSize);
            ConfigureSyslogSink(sinks.SyslogSinkConfig, sinks.MaxQueueSize);
        }

        private void ConfigureConsoleSink(SinkConfig.ConsoleSink? config, int queueCapacity)
        {
            _sinkDispatcher.Remove(typeof(ConsoleSink));
            if (config?.Enabled is not true)
            {
                return;
            }

            try
            {
                _sinkDispatcher.Add(
                    new ConsoleSink(),
                    queueCapacity,
                    ex => _dnsServer?.WriteLog(ex),
                    message => _dnsServer?.WriteLog(message));
            }
            catch (Exception ex)
            {
                LogUnexpectedSinkFailure("Console", ex);
            }
        }

        private void ConfigureFileSink(SinkConfig.FileSink? config, int queueCapacity)
        {
            _sinkDispatcher.Remove(typeof(FileSink));
            if (config?.Enabled is not true)
            {
                return;
            }

            try
            {
                AppConfig.ValidateObject(config);
                _sinkDispatcher.Add(
                    new FileSink(config.Path!),
                    queueCapacity,
                    ex => _dnsServer?.WriteLog(ex),
                    message => _dnsServer?.WriteLog(message));
            }
            catch (System.ComponentModel.DataAnnotations.ValidationException ex)
            {
                LogSinkDisabled("File", ex.Message);
            }
            catch (UnauthorizedAccessException)
            {
                LogSinkDisabled("File", $"access to '{config.Path}' is denied. Check the permissions of the DNS server account.");
            }
            catch (DirectoryNotFoundException)
            {
                LogSinkDisabled("File", $"the directory for '{config.Path}' does not exist.");
            }
            catch (IOException ex)
            {
                LogSinkDisabled("File", $"cannot open '{config.Path}': {ex.Message}");
            }
            catch (Exception ex) when (ex is ArgumentException or NotSupportedException)
            {
                LogSinkDisabled("File", $"'{config.Path}' is not a valid file path.");
            }
            catch (Exception ex)
            {
                LogUnexpectedSinkFailure("File", ex);
            }
        }

        private void ConfigureHttpSink(SinkConfig.HttpSink? config, int queueCapacity)
        {
            _sinkDispatcher.Remove(typeof(HttpSink));
            if (config?.Enabled is not true)
            {
                return;
            }

            try
            {
                AppConfig.ValidateObject(config);
                _sinkDispatcher.Add(
                    new HttpSink(config.Endpoint!, config.Headers),
                    queueCapacity,
                    ex => _dnsServer?.WriteLog(ex),
                    message => _dnsServer?.WriteLog(message));
            }
            catch (System.ComponentModel.DataAnnotations.ValidationException ex)
            {
                LogSinkDisabled("HTTP", ex.Message);
            }
            catch (Exception ex) when (ex is ArgumentException or FormatException)
            {
                // HttpSink raises these with messages that already name the endpoint or header.
                LogSinkDisabled("HTTP", ex.Message);
            }
            catch (Exception ex)
            {
                LogUnexpectedSinkFailure("HTTP", ex);
            }
        }

        private void ConfigureSyslogSink(SinkConfig.SyslogSink? config, int queueCapacity)
        {
            _sinkDispatcher.Remove(typeof(SyslogSink));
            if (config?.Enabled is not true)
            {
                return;
            }

            try
            {
                AppConfig.ValidateObject(config);
                // Host name resolution is deferred until the DNS server is serving; see SyslogSink.
                _sinkDispatcher.Add(
                    new SyslogSink(config.Address!,
                                   config.Port,
                                   config.Protocol,
                                   _dnsServerReady.Task,
                                   message => _dnsServer?.WriteLog(message)),
                    queueCapacity,
                    ex => _dnsServer?.WriteLog(ex),
                    message => _dnsServer?.WriteLog(message));
            }
            catch (System.ComponentModel.DataAnnotations.ValidationException ex)
            {
                LogSinkDisabled("Syslog", ex.Message);
            }
            catch (Exception ex) when (ex is ArgumentException or NotSupportedException)
            {
                // SyslogSink raises these with messages that already name the offending value.
                LogSinkDisabled("Syslog", ex.Message);
            }
            catch (Exception ex)
            {
                LogUnexpectedSinkFailure("Syslog", ex);
            }
        }

        /// <summary>
        /// Logs a warning or notification that a specific event log sink has been disabled due to a configuration or environment issue.
        /// </summary>
        /// <param name="sinkName">The name of the sink that is disabled.</param>
        /// <param name="reason">The specific reason explaining why the sink was disabled.</param>
        /// <remarks>
        /// Expected failures are logged without the exception. They describe a configuration or
        /// environment problem the administrator must fix, and a stack trace only hides the reason.
        /// </remarks>
        private void LogSinkDisabled(string sinkName, string reason) => _dnsServer?.WriteLog($"{sinkName} sink is disabled: {reason}");

        /// <summary>
        /// Logs an error message when an unexpected failure occurs in a log sink.
        /// </summary>
        /// <param name="sinkName">The name of the sink that failed.</param>
        /// <param name="ex">The exception that occurred.</param>
        /// <remarks>
        /// Unknown failures keep the stack trace because they point to a bug rather than to a
        /// configuration mistake.
        /// </remarks>
        private void LogUnexpectedSinkFailure(string sinkName, Exception ex) => _dnsServer?.WriteLog($"{sinkName} sink is disabled due to an unexpected error.", ex);

        private void ConfigurePipeline()
        {
            // Remove any existing processor types before applying the new configuration.
            _enrichmentDispatcher.Remove(typeof(Normalize));
            _enrichmentDispatcher.Remove(typeof(Tags));
            if (_config!.Pipeline.NormalizeProcessConfig?.Enabled is true)
            {
                _enrichmentDispatcher.Add(new Normalize());
            }
            if (_config!.Pipeline.TaggingProcessConfig?.Enabled is true)
            {
                _enrichmentDispatcher.Add(new Tags(_config.Pipeline.TaggingProcessConfig.Tags));
            }
        }

        private void IncrementDropAndMaybeLog()
        {
            Interlocked.Increment(ref _droppedCount);

            var nowTicks = DateTime.UtcNow.Ticks;
            var lastTicks = Volatile.Read(ref _lastDropTicks);

            if (new TimeSpan(nowTicks - lastTicks) >= DropLogInterval &&
                Interlocked.CompareExchange(ref _lastDropTicks, nowTicks, lastTicks) == lastTicks)
            {
                var dropped = Interlocked.Exchange(ref _droppedCount, 0);
                _dnsServer?.WriteLog(
                    $"Log export queue full; dropped {dropped} entries over last {DropLogInterval.TotalSeconds:F0}s.");
            }
        }

        #endregion private

        #region properties

        public string Description =>
            "Allows exporting query logs to third party sinks. Supports exporting to FileSink, HTTP endpoint, and SyslogSink.";

        #endregion properties
    }
}
