using System;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.Syslog;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LogExporter.Sinks
{
    internal interface ISyslogTransport : IDisposable
    {
        Task SendAsync(LogEvent logEvent, CancellationToken token);
    }

    internal static class SyslogTransport
    {
        internal static ISyslogTransport Create(
            string host,
            int port,
            string protocol,
            Facility facility,
            string appName)
        {
            if (protocol == "local")
            {
                return new LocalSyslogTransport(appName, facility);
            }

            var formatter = new Rfc5424Formatter(facility, appName);

            return protocol switch
            {
                "udp" => new UdpSyslogTransport(
                    new IPEndPoint(IPAddress.Parse(host), port),
                    formatter),

                "tcp" or "tls" => new TcpSyslogTransport(
                    new SyslogTcpConfig
                    {
                        Host = host,
                        Port = port,
                        Formatter = formatter,
                        Framer = new MessageFramer(FramingType.OCTET_COUNTING),
                        UseTls = protocol == "tls"
                    }),

                _ => throw new NotSupportedException(
                    "Syslog transport protocol is not supported: " + protocol),
            };
        }
    }

    internal sealed class UdpSyslogTransport : ISyslogTransport
    {
        private readonly IPEndPoint _endpoint;
        private readonly ISyslogFormatter _formatter;
        private readonly UdpClient _client;

        internal UdpSyslogTransport(IPEndPoint endpoint, ISyslogFormatter formatter)
        {
            _endpoint = endpoint;
            _formatter = formatter;
            _client = new UdpClient(endpoint.AddressFamily);
        }

        public async Task SendAsync(LogEvent logEvent, CancellationToken token)
        {
            var data = Encoding.UTF8.GetBytes(_formatter.FormatMessage(logEvent));

            await _client
                .SendAsync(data, data.Length, _endpoint)
                .WaitAsync(token)
                .ConfigureAwait(false);
        }

        public void Dispose() => _client.Dispose();
    }

    internal sealed class TcpSyslogTransport : ISyslogTransport
    {
        private readonly SyslogTcpSink _sink;

        internal TcpSyslogTransport(SyslogTcpConfig config)
        {
            _sink = new SyslogTcpSink(config);
        }

        public Task SendAsync(LogEvent logEvent, CancellationToken token) =>
            _sink.EmitBatchAsync([logEvent]).WaitAsync(token);

        public void Dispose() => _sink.Dispose();
    }

    internal sealed class LocalSyslogTransport : ISyslogTransport
    {
        private readonly Serilog.Core.Logger _logger;

        internal LocalSyslogTransport(string appName, Facility facility)
        {
            _logger = new LoggerConfiguration()
                .WriteTo.LocalSyslog(appName, facility)
                .Enrich.FromLogContext()
                .CreateLogger();
        }

        public Task SendAsync(LogEvent logEvent, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            _logger.Write(logEvent);
            return Task.CompletedTask;
        }

        public void Dispose() => _logger.Dispose();
    }
}
