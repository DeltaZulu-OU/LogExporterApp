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

        // Stage 1 buffer: transformed LogEntry waiting for enrichment
        private Channel<LogEntry>? _transformChannel;

        // Stage 2 buffer: enriched LogEntry waiting for dispatch
        private Channel<LogEntry>? _enrichedChannel;

        private readonly SemaphoreSlim _lifecycleLock = new SemaphoreSlim(1, 1);
        private Task? _backgroundTask;
        private CancellationTokenSource? _pipelineCancellation;
        private AppConfig? _config;
        private bool _disposed;
        private IDnsServer? _dnsServer;
        private volatile bool _enableLogging; // volatile to improve cross-thread visibility

        private long _droppedCount;
        private static readonly TimeSpan DropLogInterval = TimeSpan.FromSeconds(5);
        private long _lastDropTicks;

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

        ~App() => Dispose();

        public void Dispose()
        {
            _lifecycleLock.Wait();
            try
            {
                if (_disposed)
                    return;

                _disposed = true;
                _enableLogging = false;

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

                GC.SuppressFinalize(this);
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
                ObjectDisposedException.ThrowIf(_disposed, this);

                _dnsServer = dnsServer;

                // A config update reuses this App instance. Stop the current generation
                // before replacing channels, enrichers, or sinks so workers cannot cross
                // generations or use resources that are being disposed.
                await StopPipelineAsync().ConfigureAwait(false);

                try
                {
                    _config = AppConfig.Deserialize(config)
                              ?? throw new DnsClientException("Invalid application configuration.");

                    ConfigurePipeline();
                    ConfigureSinks();
                }
                catch (Exception ex)
                {
                    _dnsServer?.WriteLog(ex);
                    _enableLogging = false;
                    throw;
                }

                if (!_sinkDispatcher.Any())
                {
                    _enableLogging = false;
                    return;
                }

                Channel<LogEntry> transformChannel = Channel.CreateBounded<LogEntry>(
                    new BoundedChannelOptions(_config!.Sinks.MaxQueueSize)
                    {
                        SingleReader = true,
                        SingleWriter = false,
                        FullMode = BoundedChannelFullMode.DropWrite
                    });

                Channel<LogEntry> enrichedChannel = Channel.CreateBounded<LogEntry>(
                    new BoundedChannelOptions(_config.Sinks.MaxQueueSize)
                    {
                        SingleReader = true,
                        SingleWriter = true,
                        FullMode = BoundedChannelFullMode.DropWrite
                    });

                CancellationTokenSource pipelineCancellation = new CancellationTokenSource();

                _transformChannel = transformChannel;
                _enrichedChannel = enrichedChannel;
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

                _enableLogging = true;
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
            if (!_enableLogging)
                return Task.CompletedTask;

            Channel<LogEntry>? transformChannel = _transformChannel;
            AppConfig? config = _config;
            if (transformChannel is null || config is null)
                return Task.CompletedTask;

            LogEntry entry;

            try
            {
                // input -> transform: build LogEntry
                entry = new LogEntry(timestamp, remoteEP, protocol, request, response, config.Sinks.EnableEdnsLogging);
            }
            catch (Exception ex)
            {
                // Malformed packet or unexpected data should not crash the server.
                _dnsServer?.WriteLog(ex);
                return Task.CompletedTask;
            }

            try
            {
                if (!transformChannel.Writer.TryWrite(entry))
                    IncrementDropAndMaybeLog();
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
            _enableLogging = false;

            Channel<LogEntry>? transformChannel = _transformChannel;
            Task? backgroundTask = _backgroundTask;
            CancellationTokenSource? pipelineCancellation = _pipelineCancellation;

            _transformChannel = null;
            _enrichedChannel = null;
            _backgroundTask = null;
            _pipelineCancellation = null;

            transformChannel?.Writer.TryComplete();
            pipelineCancellation?.Cancel();

            try
            {
                if (backgroundTask is not null)
                    await backgroundTask.ConfigureAwait(false);
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
                    while (transformReader.TryRead(out LogEntry? entry))
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
                            if (!enrichedWriter.TryWrite(entry))
                            {
                                IncrementDropAndMaybeLog();
                            }
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
            List<LogEntry> batch = new List<LogEntry>(BULK_INSERT_COUNT);

            try
            {
                while (await enrichedReader.WaitToReadAsync(token).ConfigureAwait(false))
                {
                    while (batch.Count < BULK_INSERT_COUNT &&
                           enrichedReader.TryRead(out LogEntry? entry))
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

        private void ConfigureSinks()
        {
            var sinks = _config!.Sinks;
            _sinkDispatcher.Remove(typeof(ConsoleSink));
            if (sinks.ConsoleSinkConfig != null && sinks.ConsoleSinkConfig.Enabled)
                _sinkDispatcher.Add(new ConsoleSink());

            _sinkDispatcher.Remove(typeof(FileSink));
            if (sinks.FileSinkConfig?.Enabled is true)
                _sinkDispatcher.Add(new FileSink(sinks.FileSinkConfig.Path));

            _sinkDispatcher.Remove(typeof(HttpSink));
            if (sinks.HttpSinkConfig?.Enabled is true)
            {
                _sinkDispatcher.Add(
                    new HttpSink(sinks.HttpSinkConfig.Endpoint, sinks.HttpSinkConfig.Headers));
            }

            _sinkDispatcher.Remove(typeof(SyslogSink));
            if (sinks.SyslogSinkConfig?.Enabled is true)
            {
                _sinkDispatcher.Add(
                    new SyslogSink(sinks.SyslogSinkConfig.Address,
                                   sinks.SyslogSinkConfig.Port!.Value,
                                   sinks.SyslogSinkConfig.Protocol));
            }
        }

        private void ConfigurePipeline()
        {
            // Remove any existing enricher types first to avoid duplicate registration.
            _enrichmentDispatcher.Remove(typeof(Normalize));
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

            long nowTicks = DateTime.UtcNow.Ticks;
            long lastTicks = Volatile.Read(ref _lastDropTicks);

            if (new TimeSpan(nowTicks - lastTicks) >= DropLogInterval &&
                Interlocked.CompareExchange(ref _lastDropTicks, nowTicks, lastTicks) == lastTicks)
            {
                long dropped = Interlocked.Exchange(ref _droppedCount, 0);
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
