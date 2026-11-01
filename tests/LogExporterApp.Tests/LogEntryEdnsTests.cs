using LogExporter;
using LogExporter.Sinks;
using Serilog.Events;
using System.Net;
using System.Text.Json;
using TechnitiumLibrary.Net.Dns;
using TechnitiumLibrary.Net.Dns.EDnsOptions;
using TechnitiumLibrary.Net.Dns.ResourceRecords;

namespace LogExporterApp.Tests;

[TestClass]
public sealed class LogEntryEdnsTests
{
    [TestMethod]
    public void ExtraTextWithUrlIsPreserved()
    {
        const string text = "source=block-list-zone; blockListUrl=https://example.org/list.txt; domain=ads.example";
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.Blocked, text).EDNS;

        Assert.HasCount(1, edns);
        Assert.AreEqual("Blocked", edns[0].ErrType);
        Assert.AreEqual(text, edns[0].Message);
    }

    [TestMethod]
    public void ExtraTextWithBracketsIsPreserved()
    {
        const string text = "source=advanced-blocking-app; group=default; regex=^ads[0-9]+\\.";
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.Blocked, text).EDNS;

        Assert.AreEqual(text, edns[0].Message);
    }

    [TestMethod]
    public void ExtraTextWithIpv6IsPreserved()
    {
        const string text = "resolver=[2001:db8::1]:853";
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.Other, text).EDNS;

        Assert.AreEqual("Other", edns[0].ErrType);
        Assert.AreEqual(text, edns[0].Message);
    }

    [TestMethod]
    public void NullExtraTextKeepsInfoCodeInErrType()
    {
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.NotSupported, null).EDNS;

        Assert.HasCount(1, edns);
        Assert.AreEqual("NotSupported", edns[0].ErrType);
        Assert.IsNull(edns[0].Message);
    }

    [TestMethod]
    public void NullExtraTextIsOmittedFromJson()
    {
        using var doc = JsonDocument.Parse(CreateEntry(EDnsExtendedDnsErrorCode.NotSupported, null).ToString());
        var ede = doc.RootElement.GetProperty("edns")[0];

        Assert.AreEqual("NotSupported", ede.GetProperty("errType").GetString());
        Assert.IsFalse(ede.TryGetProperty("message", out _));
    }

    [TestMethod]
    public void EmptyExtraTextIsPreserved()
    {
        using var doc = JsonDocument.Parse(CreateEntry(EDnsExtendedDnsErrorCode.Blocked, "").ToString());
        Assert.AreEqual("", doc.RootElement.GetProperty("edns")[0].GetProperty("message").GetString());
    }

    [TestMethod]
    public void WhitespaceExtraTextIsPreserved()
    {
        const string text = "  [value]:853  ";
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.Other, text).EDNS;

        Assert.AreEqual(text, edns[0].Message);
    }

    [TestMethod]
    public void MultipleEdesKeepOrder()
    {
        var edns = CreateEntry(
            (EDnsExtendedDnsErrorCode.Blocked, "first"),
            (EDnsExtendedDnsErrorCode.NotSupported, (string?)null)).EDNS;

        Assert.HasCount(2, edns);
        Assert.AreEqual("Blocked", edns[0].ErrType);
        Assert.AreEqual("first", edns[0].Message);
        Assert.AreEqual("NotSupported", edns[1].ErrType);
        Assert.IsNull(edns[1].Message);
    }

    [TestMethod]
    public void UnknownInfoCodeSurvivesWireRoundTrip()
    {
        var edns = CreateEntry((EDnsExtendedDnsErrorCode)999, "x", wireRoundTrip: true).EDNS;
        Assert.HasCount(1, edns);
        Assert.AreEqual("999", edns[0].ErrType);
        Assert.AreEqual("x", edns[0].Message);
    }

    [TestMethod]
    public void EmptyExtraTextIsNullAfterWireRoundTrip()
    {
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.NotSupported, "", wireRoundTrip: true).EDNS;
        Assert.HasCount(1, edns);
        Assert.AreEqual("NotSupported", edns[0].ErrType);
        Assert.IsNull(edns[0].Message);
    }

    [TestMethod]
    public void FreeTextSurvivesWireRoundTrip()
    {
        const string text = "source=app; regex=^ads[0-9]+\\\\.; server=[2001:db8::1]:853; url=https://example.org/a  ";
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.Blocked, text, wireRoundTrip: true).EDNS;
        Assert.HasCount(1, edns);
        Assert.AreEqual(text, edns[0].Message);
    }

    [TestMethod]
    public void NonEdeOptionIsIgnoredAfterWireRoundTrip()
    {
        var (request, _) = TestFixtures.CreateDnsExchange("ede.example");
        var options = new EDnsOption[]
        {
            new(EDnsOptionCode.EXTENDED_DNS_ERROR,
                new EDnsExtendedDnsErrorOptionData(EDnsExtendedDnsErrorCode.Blocked, "x")),
            new((EDnsOptionCode)65000, new EDnsUnknownOptionData([1, 2, 3])),
            new(EDnsOptionCode.EXTENDED_DNS_ERROR,
                new EDnsExtendedDnsErrorOptionData(EDnsExtendedDnsErrorCode.NotSupported, null))
        };
        var edns = CreateEntry(request, true, true, options).EDNS;
        Assert.HasCount(2, edns);
        Assert.AreEqual("Blocked", edns[0].ErrType);
        Assert.AreEqual("NotSupported", edns[1].ErrType);
    }

    [TestMethod]
    public async Task SyslogOmitsNullEdeMessageAsync()
    {
        using var transport = new CaptureEdeTransport();
        using var sink = new SyslogSink(
            "127.0.0.1", 514, "udp", Task.CompletedTask,
            _ => { }, _ => transport, _ => TimeSpan.Zero);

        await sink.ExportAsync(
            [
                CreateEntry(EDnsExtendedDnsErrorCode.NotSupported, null, wireRoundTrip: true),
                CreateEntry(EDnsExtendedDnsErrorCode.Blocked, "null", wireRoundTrip: true)
            ],
            CancellationToken.None);

        Assert.HasCount(2, transport.Events);
        Assert.IsTrue(transport.Events[0].Properties.ContainsKey("ednsErrType_0"));
        Assert.IsFalse(transport.Events[0].Properties.ContainsKey("ednsMessage_0"));
        Assert.IsTrue(transport.Events[1].Properties.ContainsKey("ednsMessage_0"));
        Assert.AreEqual("null", ((ScalarValue)transport.Events[1].Properties["ednsMessage_0"]).Value);
    }

    [TestMethod]
    public void EdnsLoggingDisabledYieldsNoRecords()
    {
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.Blocked, "x", ednsLogging: false).EDNS;
        Assert.IsEmpty(edns);
    }

    private static LogEntry CreateEntry(
        EDnsExtendedDnsErrorCode code,
        string? extraText,
        bool ednsLogging = true,
        bool wireRoundTrip = false)
    {
        var (request, _) = TestFixtures.CreateDnsExchange("ede.example");
        return CreateEntry(request, ednsLogging, wireRoundTrip,
            new EDnsOption(EDnsOptionCode.EXTENDED_DNS_ERROR,
                new EDnsExtendedDnsErrorOptionData(code, extraText)));
    }

    private static LogEntry CreateEntry(params (EDnsExtendedDnsErrorCode Code, string? Text)[] errors)
    {
        var (request, _) = TestFixtures.CreateDnsExchange("ede.example");
        return CreateEntry(request, true, false,
            errors.Select(error => new EDnsOption(
                EDnsOptionCode.EXTENDED_DNS_ERROR,
                new EDnsExtendedDnsErrorOptionData(error.Code, error.Text))).ToArray());
    }

    private static LogEntry CreateEntry(
        DnsDatagram request,
        bool ednsLogging,
        bool wireRoundTrip,
        params EDnsOption[] options)
    {
        var response = new DnsDatagram(
            1, true, DnsOpcode.StandardQuery, false, false, true, true, false, false,
            DnsResponseCode.NxDomain,
            request.Question,
            udpPayloadSize: 1232,
            options: options);

        if (wireRoundTrip)
        {
            using var stream = new MemoryStream();
            response.WriteTo(stream);
            stream.Position = 0;
            response = DnsDatagram.ReadFrom(stream);
        }

        return new LogEntry(
            DateTime.UtcNow,
            new IPEndPoint(IPAddress.Loopback, 53000),
            DnsTransportProtocol.Udp,
            request,
            response,
            ednsLogging);
    }

    private sealed class CaptureEdeTransport : ISyslogTransport
    {
        public List<LogEvent> Events { get; } = new();

        public Task SendAsync(LogEvent logEvent, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Events.Add(logEvent);
            return Task.CompletedTask;
        }

        public void Dispose() { }
    }
}
