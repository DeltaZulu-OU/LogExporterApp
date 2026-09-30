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

using Serilog;
using Serilog.Events;
using Serilog.Parsing;
using Serilog.Sinks.Syslog;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
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
    /// can only deliver once it is serving. Name resolution is then retried a few times, see
    /// <see cref="ResolveWithRetryAsync"/>.
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

        const string _appName = "Technitium DNS Server";
        const string DEFAULT_PROTOCOL = "udp";
        const int DEFAULT_PORT = 514;

        readonly Facility _facility = Facility.Local6;

        /// <summary>
        /// Number of resolution attempts before the sink gives up.
        /// </summary>
        /// <remarks>
        /// The first query log proves that the listeners are up, but not that every zone is
        /// loaded or that upstream recursion works yet. Another DNS app may also trigger a query
        /// log while apps are still loading. A short, bounded retry covers these windows without
        /// turning a genuinely wrong address into an endless loop of log messages.
        /// </remarks>
        const int MaxResolveAttempts = 5;

        /// <summary>
        /// Delay before the first retry. It doubles on every further attempt
        /// (2, 4, 8 and 16 seconds), so the sink gives up about 30 seconds after the server is ready.
        /// </summary>
        static readonly TimeSpan InitialResolveRetryDelay = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Longest time a single export waits for the logger to be created.
        /// </summary>
        /// <remarks>
        /// <see cref="SinkDispatcher"/> awaits all sinks together, so waiting for the whole retry
        /// window would stall every other sink for about 30 seconds. One second covers a normal
        /// lookup for the first batch. While retries are still running, later batches are dropped
        /// for syslog only.
        /// </remarks>
        static readonly TimeSpan LoggerWaitTimeout = TimeSpan.FromSeconds(1);

        readonly string _address;
        readonly int _port;
        readonly string _protocol;
        readonly Action<string>? _log;
        /// <remarks>
        /// Deliberately never disposed. A resolution or retry delay may still observe its token after
        /// <see cref="Dispose"/> returns, and a source without a timer holds no unmanaged resources.
        /// </remarks>
        readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();
        readonly Task<Serilog.Core.Logger?> _loggerTask;

        bool _disposed;

        // Reuse the message template instead of parsing it per-log
        const string TemplateText = "{questionsSummary}; RCODE: {rCode}; ANSWER: [{answersSummary}]";
        static readonly MessageTemplate Template =
            new MessageTemplateParser().Parse(TemplateText);

        #endregion

        #region constructor

        /// <remarks>
        /// The constructor performs no I/O so that it cannot fail on network conditions during app
        /// load. It still validates its arguments, although <see cref="AppConfig"/> does too, because
        /// Serilog reports the same mistakes later and less clearly: a missing host surfaces as
        /// <c>ArgumentException("host")</c> and LOCAL on Windows as a libc <c>DllNotFoundException</c>.
        /// </remarks>
        /// <param name="serverReady">Completes once the DNS server can resolve names.</param>
        /// <param name="log">Receives failures that occur after construction, when no caller is left
        /// to catch an exception.</param>
        public SyslogSink(string address, int? port, string? protocol, Task serverReady, Action<string>? log = null)
        {
            _address = address;
            _port = port ?? DEFAULT_PORT;
            _protocol = (protocol ?? DEFAULT_PROTOCOL).ToLowerInvariant();
            _log = log;

            if (_protocol is not ("tls" or "tcp" or "udp" or "local"))
                throw new NotSupportedException($"protocol '{protocol}' is not supported. Use UDP, TCP, TLS, or LOCAL.");

            if (_protocol == "local" && !(OperatingSystem.IsLinux() || OperatingSystem.IsMacOS()))
                throw new NotSupportedException("protocol LOCAL requires a Unix syslog daemon and is not available on this platform.");

            if (_protocol != "local" && string.IsNullOrWhiteSpace(address))
                throw new ArgumentException($"an address is required for protocol {_protocol.ToUpperInvariant()}.");

            _loggerTask = CreateLoggerWhenReadyAsync(serverReady, _disposeCts.Token);
        }

        #endregion

        #region IDisposable

        /// <remarks>
        /// The logger may still be under construction, so it is disposed by a continuation rather
        /// than directly. Blocking here until construction finishes could hold up a config reload
        /// for the whole resolution retry window.
        /// </remarks>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _disposeCts.Cancel();

            _loggerTask.ContinueWith(
                static t =>
                {
                    if (t.IsCompletedSuccessfully)
                        t.Result?.Dispose();
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        #endregion

        #region public

        public async Task ExportAsync(IReadOnlyList<LogEntry> logs, CancellationToken token)
        {
            // ADR: SyslogSink export previously used Task.Run with a synchronous loop,
            // causing threadpool churn and preventing timely shutdown. We now execute
            // sequentially on the caller's async context and check cancellation between
            // log writes. Serilog remains synchronous, but cancellation ensures bounded
            // shutdown latency.

            if (_disposed || logs.Count == 0 || token.IsCancellationRequested)
                return;

            Serilog.Core.Logger? logger;
            try
            {
                logger = await _loggerTask.WaitAsync(LoggerWaitTimeout, token).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                return; // Still resolving or retrying; this batch is dropped for syslog only.
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }

            if (logger is null)
                return; // Creation failed and was reported once; the sink stays inactive.

            foreach (LogEntry log in logs)
            {
                if (token.IsCancellationRequested)
                    break;

                logger.Write(Convert(log));
            }
        }

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
        /// <returns>The logger, or <see langword="null"/> when the sink was disposed or cannot be
        /// created. Failures are reported through the log callback.</returns>
        private async Task<Serilog.Core.Logger?> CreateLoggerWhenReadyAsync(Task serverReady, CancellationToken token)
        {
            try
            {
                await serverReady.WaitAsync(token).ConfigureAwait(false);

                string host = _address;
                if (_protocol == "udp" && !IPAddress.TryParse(_address, out _))
                    host = (await ResolveWithRetryAsync(_address, token).ConfigureAwait(false)).ToString();

                return CreateLogger(host);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return null;
            }
            catch (SocketException ex)
            {
                _log?.Invoke(
                    $"Syslog sink is disabled: cannot resolve '{_address}' after {MaxResolveAttempts} attempts " +
                    $"({ex.SocketErrorCode}). Correct the address or its DNS record and save the configuration to try again.");
                return null;
            }
            catch (Exception ex)
            {
                _log?.Invoke($"Syslog sink is disabled due to an unexpected error: {ex}");
                return null;
            }
        }

        /// <summary>
        /// Resolves <paramref name="host"/>, retrying with exponential backoff on socket errors.
        /// </summary>
        /// <remarks>
        /// Every <see cref="SocketException"/> is retried, including <c>HostNotFound</c>: while
        /// zones are still loading, a locally hosted name can briefly return NXDOMAIN. The first
        /// failure is logged so an administrator can see why syslog output is delayed; the final
        /// failure is rethrown to the caller, which disables the sink.
        /// </remarks>
        /// <exception cref="SocketException">All <see cref="MaxResolveAttempts"/> attempts failed.</exception>
        private async Task<IPAddress> ResolveWithRetryAsync(string host, CancellationToken token)
        {
            TimeSpan delay = InitialResolveRetryDelay;

            for (int attempt = 1; ; attempt++)
            {
                try
                {
                    return await ResolveAsync(host, token).ConfigureAwait(false);
                }
                catch (SocketException ex) when (attempt < MaxResolveAttempts)
                {
                    if (attempt == 1)
                    {
                        _log?.Invoke(
                            $"Syslog sink cannot resolve '{host}' yet ({ex.SocketErrorCode}); " +
                            $"retrying up to {MaxResolveAttempts - 1} more times.");
                    }

                    await Task.Delay(delay, token).ConfigureAwait(false);
                    delay *= 2;
                }
            }
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
            IPAddress[] addresses = await Dns.GetHostAddressesAsync(host, token).ConfigureAwait(false);

            foreach (IPAddress address in addresses)
            {
                if (address.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
                    return address;
            }

            throw new SocketException((int)SocketError.NoData);
        }

        private Serilog.Core.Logger CreateLogger(string host)
        {
            LoggerConfiguration conf = new LoggerConfiguration();

            conf = _protocol switch
            {
                "tls" or "tcp" => conf.WriteTo.TcpSyslog(
                            host,
                            _port,
                            _appName,
                            FramingType.OCTET_COUNTING,
                            SyslogFormat.RFC5424,
                            _facility,
                            useTls: _protocol == "tls"),

                "udp" => conf.WriteTo.UdpSyslog(
                            host,
                            _port,
                            _appName,
                            SyslogFormat.RFC5424,
                            _facility),

                "local" => conf.WriteTo.LocalSyslog(
                            _appName,
                            _facility),

                _ => throw new NotSupportedException("SyslogSink protocol is not supported: " + _protocol),
            };

            return conf.Enrich.FromLogContext().CreateLogger();
        }

        private static LogEvent Convert(LogEntry log)
        {
            // Rough capacity: 9 base + 4 question + some answers + edns
            // This avoids repeated List resizes
            List<LogEventProperty> properties = new List<LogEventProperty>(16)
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
                LogEntry.DnsQuestion question = log.Question;

                properties.Add(new LogEventProperty(
                    "qName",
                    new ScalarValue(question.QuestionName)));

                properties.Add(new LogEventProperty(
                    "qType",
                    new ScalarValue(question.QuestionType.ToString())));

                properties.Add(new LogEventProperty(
                    "qClass",
                    new ScalarValue(question.QuestionClass.ToString())));

                string questionSummary =
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
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < log.Answers.Length; i++)
                {
                    LogEntry.DnsResourceRecord answer = log.Answers[i];

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
                        sb.Append(", ");
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
                for (int i = 0; i < log.EDNS.Length; i++)
                {
                    LogEntry.EDNSLog ednsLog = log.EDNS[i];

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
