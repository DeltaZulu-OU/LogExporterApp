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

using Serilog.Events;
using Serilog.Parsing;
using Serilog.Sinks.Syslog;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LogExporter.Sinks
{
    /// <summary>
    /// Exports query logs to a syslog server over UDP, TCP, TLS, or the local syslog daemon.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Serilog logger is not built in the constructor. Technitium loads DNS apps before it
    /// loads zones and binds its listeners, and <c>IDnsServer</c> exposes no readiness event.
    /// Serilog's <c>UdpSyslog</c> resolves the host name synchronously while the logger is built,
    /// so on a host whose resolver points at this DNS server the lookup failed with
    /// <c>EAI_AGAIN</c> ("Resource temporarily unavailable") and aborted app initialization.
    /// </para>
    /// <para>
    /// The logger is therefore built in the background once the <c>serverReady</c> task
    /// completes. <see cref="App"/> completes it on the first query log, which the DNS server
    /// can only deliver once it is serving. Name resolution is then retried in the background
    /// until it succeeds or the sink is disposed, see <see cref="ResolveWithRetryAsync"/>.
    /// </para>
    /// <para>
    /// The address may be an IP address or an FQDN. For UDP, an FQDN is resolved once per
    /// configuration load, matching Serilog's own UDP behaviour; a collector that moves to a new
    /// IP is picked up when the configuration is saved again. TCP and TLS pass the FQDN to Serilog,
    /// which resolves it again on every reconnect and validates the TLS certificate against it.
    /// </para>
    /// </remarks>
    public sealed class SyslogSink : IOutputSink
    {
        #region variables

        private const string _appName = "Technitium DNS Server";
        private const string DEFAULT_PROTOCOL = "udp";
        private const int DEFAULT_PORT = 514;

        private readonly Facility _facility = Facility.Local6;

        /// <summary>
        /// Delay before the first retry. It doubles after each failure and is capped so a
        /// long-lived resolver outage does not create arbitrarily long recovery latency.
        /// </summary>
        private static readonly TimeSpan InitialResolveRetryDelay = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Maximum retry delay before jitter is applied.
        /// </summary>
        private static readonly TimeSpan MaxResolveRetryDelay = TimeSpan.FromSeconds(30);

        private const double RetryJitterMin = 0.8;
        private const double RetryJitterRange = 0.4;

        private readonly string _address;
        private readonly int _port;
        private readonly string _protocol;
        private readonly Action<string>? _log;
        private readonly Func<string, ISyslogTransport> _transportFactory;
        private readonly Func<int, TimeSpan> _retryDelayFactory;

        /// <summary>
        ///     Initializes a new instance of the cancellation token source used to signal disposal and cancel ongoing operations.
        /// </summary>
        /// <remarks>
        /// Deliberately never disposed. A resolution or retry delay may still observe its token after
        /// <see cref="Dispose"/> returns, and a source without a timer holds no unmanaged resources.
        /// </remarks>
        private readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();
        private readonly Task<ISyslogTransport?> _transportTask;

        private bool _disposed;

        // Reuse the message template instead of parsing it per-log
        private const string TemplateText = "{questionsSummary}; RCODE: {rCode}; ANSWER: [{answersSummary}]";
        private static readonly MessageTemplate Template =
            new MessageTemplateParser().Parse(TemplateText);

        #endregion

        #region constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="SyslogSink"/> class.
        /// </summary>
        /// <remarks>
        /// The constructor performs no I/O so that it cannot fail on network conditions during app
        /// load. It still validates its arguments, although <see cref="AppConfig"/> does too, because
        /// Serilog reports the same mistakes later and less clearly: a missing host surfaces as
        /// <c>ArgumentException("host")</c> and LOCAL on Windows as a libc <c>DllNotFoundException</c>.
        /// </remarks>
        /// <param name="address">The network address of the syslog server.</param>
        /// <param name="port">The port number of the syslog server.</param>
        /// <param name="protocol">The transport protocol to use (UDP, TCP, TLS, or LOCAL).</param>
        /// <param name="serverReady">Completes once the DNS server can resolve names.</param>
        /// <param name="log">Receives failures that occur after construction, when no caller is left
        /// to catch an exception.</param>
        public SyslogSink(string address, int? port, string? protocol, Task serverReady, Action<string>? log = null)
            : this(address, port, protocol, serverReady, log, null, null)
        {
        }

        internal SyslogSink(
            string address,
            int? port,
            string? protocol,
            Task serverReady,
            Action<string>? log,
            Func<string, ISyslogTransport>? transportFactory,
            Func<int, TimeSpan>? retryDelayFactory)
        {
            _address = address;
            _port = port ?? DEFAULT_PORT;
            _protocol = (protocol ?? DEFAULT_PROTOCOL).ToLowerInvariant();
            _log = log;

            if (_protocol is not ("tls" or "tcp" or "udp" or "local"))
            {
                throw new NotSupportedException($"protocol '{protocol}' is not supported. Use UDP, TCP, TLS, or LOCAL.");
            }

            if (_protocol == "local" && !(OperatingSystem.IsLinux() || OperatingSystem.IsMacOS()))
            {
                throw new NotSupportedException("protocol LOCAL requires a Unix syslog daemon and is not available on this platform.");
            }

            if (_protocol != "local" && string.IsNullOrWhiteSpace(address))
            {
                throw new ArgumentException($"an address is required for protocol {_protocol.ToUpperInvariant()}.");
            }

            _transportFactory = transportFactory
                ?? (host => SyslogTransport.Create(host, _port, _protocol, _facility, _appName));

            _retryDelayFactory = retryDelayFactory
                ?? (failureCount => GetRetryDelay(failureCount, Random.Shared.NextDouble()));

            _transportTask = CreateTransportWhenReadyAsync(serverReady, _disposeCts.Token);
        }

        #endregion

        #region IDisposable

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <remarks>
        /// The logger may still be under construction, so it is disposed by a continuation rather
        /// than directly. Blocking here until construction finishes could hold up a config reload
        /// for the whole resolution retry window.
        /// </remarks>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _disposeCts.Cancel();

            _transportTask.ContinueWith(
                static t =>
                {
                    if (t.IsCompletedSuccessfully)
                    {
                        t.Result?.Dispose();
                    }
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        #endregion

        #region public

        public async Task ExportAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
        {
            if (_disposed || logs.Count == 0 || token.IsCancellationRequested)
            {
                return;
            }

            var transport = await _transportTask.WaitAsync(token).ConfigureAwait(false);
            if (transport is null)
            {
                return;
            }

            var retryState = new TransportRetryState();

            for (var i = 0; i < logs.Count; i++)
            {
                await SendWithRetryAsync(
                        transport,
                        Convert(logs[i]),
                        retryState,
                        token)
                    .ConfigureAwait(false);
            }
        }

        private async Task SendWithRetryAsync(
            ISyslogTransport transport,
            LogEvent logEvent,
            TransportRetryState retryState,
            CancellationToken token)
        {
            while (true)
            {
                try
                {
                    await transport.SendAsync(logEvent, token).ConfigureAwait(false);
                    ReportTransportRecovery(retryState);
                    return;
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (SocketException ex) when (ex.SocketErrorCode == SocketError.MessageSize)
                {
                    _log?.Invoke(
                        "Syslog dropped one entry because the formatted message exceeds the local transport's maximum message size.");
                    return;
                }
                catch (Exception ex) when (IsRetryableTransportFailure(ex, token))
                {
                    await DelayBeforeTransportRetryAsync(
                            ex,
                            retryState,
                            token)
                        .ConfigureAwait(false);
                }
            }
        }

        private async Task DelayBeforeTransportRetryAsync(
            Exception ex,
            TransportRetryState retryState,
            CancellationToken token)
        {
            retryState.FailureCount++;

            if (!retryState.Degraded)
            {
                retryState.Degraded = true;
                _log?.Invoke(
                    $"Syslog local transport unavailable ({GetTransportErrorName(ex)}); " +
                    "buffering logs and retrying.");
            }

            await Task.Delay(
                    _retryDelayFactory(retryState.FailureCount),
                    token)
                .ConfigureAwait(false);
        }

        private void ReportTransportRecovery(TransportRetryState retryState)
        {
            if (!retryState.Degraded)
            {
                return;
            }

            retryState.Degraded = false;
            retryState.FailureCount = 0;
            _log?.Invoke(
                "Syslog transport recovered; buffered logs are being delivered.");
        }

        private sealed class TransportRetryState
        {
            public bool Degraded { get; set; }
            public int FailureCount { get; set; }
        }

        internal static bool IsRetryableTransportFailure(Exception ex, CancellationToken token) =>
            ex is SocketException
            || ex is IOException
            || ex is AuthenticationException
            || ex is TimeoutException
            || (ex is OperationCanceledException && !token.IsCancellationRequested);

        private static string GetTransportErrorName(Exception ex) =>
            ex is SocketException socketException
                ? socketException.SocketErrorCode.ToString()
                : ex.GetType().Name;

        #endregion

        #region private

        /// <summary>
        /// Waits until the DNS server is serving, then builds the Serilog logger.
        /// </summary>
        /// <remarks>
        /// UDP is the only transport Serilog resolves while building the logger, and it does so
        /// synchronously. The host is resolved asynchronously here and Serilog receives an IP
        /// literal instead. TCP and TLS resolve when they connect, and TLS needs the host name
        /// for certificate validation, so both keep the configured address.
        /// </remarks>
        /// <returns>The transport, or <see langword="null"/> when the sink was disposed or cannot
        /// be created. Failures are reported through the log callback.</returns>
        private async Task<ISyslogTransport?> CreateTransportWhenReadyAsync(Task serverReady, CancellationToken token)
        {
            try
            {
                await serverReady.WaitAsync(token).ConfigureAwait(false);

                var host = _address;
                if (_protocol == "udp" && !IPAddress.TryParse(_address, out _))
                {
                    host = (await ResolveWithRetryAsync(_address, token).ConfigureAwait(false)).ToString();
                }

                return _transportFactory(host);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return null;
            }
            catch (Exception ex)
            {
                _log?.Invoke($"Syslog sink is disabled due to an unexpected error: {ex}");
                return null;
            }
        }

        /// <summary>
        /// Resolves <paramref name="host"/>, retrying with capped exponential backoff and jitter
        /// until resolution succeeds or the sink is disposed.
        /// </summary>
        /// <remarks>
        /// Every <see cref="SocketException"/> is treated as transient. This intentionally avoids
        /// encoding a startup-readiness deadline: the host resolver may itself depend on the DNS
        /// server that is still finishing startup. The degraded state is logged once, and recovery
        /// is logged once when resolution eventually succeeds.
        /// </remarks>
        private async Task<IPAddress> ResolveWithRetryAsync(string host, CancellationToken token)
        {
            var failureCount = 0;
            var degraded = false;

            while (true)
            {
                try
                {
                    var address = await ResolveAsync(host, token).ConfigureAwait(false);

                    if (degraded)
                    {
                        _log?.Invoke($"Syslog sink resolved '{host}'; export resumed.");
                    }

                    return address;
                }
                catch (SocketException ex)
                {
                    failureCount++;

                    if (!degraded)
                    {
                        degraded = true;
                        _log?.Invoke(
                            $"Syslog sink cannot resolve '{host}' yet ({ex.SocketErrorCode}); " +
                            "retrying in the background.");
                    }

                    var delay = GetRetryDelay(
                        failureCount,
                        Random.Shared.NextDouble());

                    await Task.Delay(delay, token).ConfigureAwait(false);
                }
            }
        }

        internal static TimeSpan GetRetryDelay(int failureCount, double jitterSample)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(failureCount, 1);
            ArgumentOutOfRangeException.ThrowIfLessThan(jitterSample, 0);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(jitterSample, 1);

            var baseSeconds = failureCount switch
            {
                1 => InitialResolveRetryDelay.TotalSeconds,
                2 => InitialResolveRetryDelay.TotalSeconds * 2,
                3 => InitialResolveRetryDelay.TotalSeconds * 4,
                4 => InitialResolveRetryDelay.TotalSeconds * 8,
                _ => MaxResolveRetryDelay.TotalSeconds
            };

            baseSeconds = Math.Min(baseSeconds, MaxResolveRetryDelay.TotalSeconds);

            var jitteredSeconds =
                baseSeconds * (RetryJitterMin + RetryJitterRange * jitterSample);

            return TimeSpan.FromSeconds(
                Math.Min(jitteredSeconds, MaxResolveRetryDelay.TotalSeconds));
        }

        /// <summary>
        /// Resolves <paramref name="host"/> once and returns its first IPv4 or IPv6 address.
        /// </summary>
        /// <remarks>
        /// Taking the first usable address mirrors Serilog's own <c>ResolveIP</c>, so switching to
        /// asynchronous resolution does not change which collector receives the logs. An empty
        /// result is raised as a <see cref="SocketException"/> so the retry and the final error
        /// message treat it like any other failed lookup.
        /// </remarks>
        /// <exception cref="SocketException">The lookup failed or returned no usable address.</exception>
        private static async Task<IPAddress> ResolveAsync(string host, CancellationToken token)
        {
            var addresses = await Dns.GetHostAddressesAsync(host, token).ConfigureAwait(false);

            foreach (var address in addresses)
            {
                if (address.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
                {
                    return address;
                }
            }

            throw new SocketException((int)SocketError.NoData);
        }

        private static LogEvent Convert(LogEntry log)
        {
            // Rough capacity: 9 base + 4 question + some answers + edns
            // This avoids repeated List resizes
            var properties = new List<LogEventProperty>(16)
            {
                // Base fields (unchanged semantics)
                new LogEventProperty(
                "timestamp",
                new ScalarValue(log.Timestamp.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"))),
                new LogEventProperty(
                "clientIp",
                new ScalarValue(log.ClientIp)),
                new LogEventProperty(
                "protocol",
                new ScalarValue(log.Protocol.ToString())),
                new LogEventProperty(
                "responseType",
                new ScalarValue(log.ResponseType.ToString())),
                new LogEventProperty(
                "responseRtt",
                new ScalarValue(log.ResponseRtt?.ToString())),
                new LogEventProperty(
                "rCode",
                new ScalarValue(log.ResponseCode.ToString()))
            };

            // Question
            if (log.Question != null)
            {
                var question = log.Question;

                properties.Add(new LogEventProperty(
                    "qName",
                    new ScalarValue(question.QuestionName)));

                properties.Add(new LogEventProperty(
                    "qType",
                    new ScalarValue(question.QuestionType.ToString())));

                properties.Add(new LogEventProperty(
                    "qClass",
                    new ScalarValue(question.QuestionClass.ToString())));

                var questionSummary =
                    $"QNAME: {question.QuestionName}, " +
                    $"QTYPE: {question.QuestionType}, " +
                    $"QCLASS: {question.QuestionClass}";

                properties.Add(new LogEventProperty(
                    "questionsSummary",
                    new ScalarValue(questionSummary)));
            }
            else
            {
                properties.Add(new LogEventProperty(
                    "questionsSummary",
                    new ScalarValue(string.Empty)));
            }

            // Answers
            if (log.Answers.Length > 0)
            {
                // Build answersSummary without LINQ
                var sb = new StringBuilder();
                for (var i = 0; i < log.Answers.Length; i++)
                {
                    var answer = log.Answers[i];

                    properties.Add(new LogEventProperty(
                        $"aName_{i}",
                        new ScalarValue(answer.Name)));

                    properties.Add(new LogEventProperty(
                        $"aType_{i}",
                        new ScalarValue(answer.RecordType.ToString())));

                    properties.Add(new LogEventProperty(
                        $"aClass_{i}",
                        new ScalarValue(answer.RecordClass.ToString())));

                    properties.Add(new LogEventProperty(
                        $"aTtl_{i}",
                        new ScalarValue(answer.RecordTtl.ToString())));

                    properties.Add(new LogEventProperty(
                        $"aRData_{i}",
                        new ScalarValue(answer.RecordData)));

                    properties.Add(new LogEventProperty(
                        $"aDnssecStatus_{i}",
                        new ScalarValue(answer.DnssecStatus.ToString())));

                    if (i > 0)
                    {
                        sb.Append(", ");
                    }

                    sb.Append(answer.RecordData);
                }

                properties.Add(new LogEventProperty(
                    "answersSummary",
                    new ScalarValue(sb.ToString())));
            }
            else
            {
                properties.Add(new LogEventProperty(
                    "answersSummary",
                    new ScalarValue(string.Empty)));
            }

            // EDNS
            if (log.EDNS.Length > 0)
            {
                for (var i = 0; i < log.EDNS.Length; i++)
                {
                    var ednsLog = log.EDNS[i];

                    properties.Add(new LogEventProperty(
                        $"ednsErrType_{i}",
                        new ScalarValue(ednsLog.ErrType)));

                    properties.Add(new LogEventProperty(
                        $"ednsMessage_{i}",
                        new ScalarValue(ednsLog.Message)));
                }
            }

            // Meta
            if (log.Meta.Count > 0)
            {
                foreach (var enrichment in log.Meta)
                {
                    var k = enrichment.Key;
                    var v = enrichment.Value;
                    properties.Add(new LogEventProperty(k, new ScalarValue(v)));
                }
            }

            // Reuse the static MessageTemplate 'Template'
            return new LogEvent(
                timestamp: log.Timestamp,
                level: LogEventLevel.Information,
                exception: null,
                messageTemplate: Template,
                properties: properties);
        }

        #endregion
    }
}
