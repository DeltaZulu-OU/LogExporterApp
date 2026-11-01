using LogExporter;
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
        var edns = CreateEntry((EDnsExtendedDnsErrorCode)15, text).EDNS;

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
    public void EdnsLoggingDisabledYieldsNoRecords()
    {
        var edns = CreateEntry(EDnsExtendedDnsErrorCode.Blocked, "x", ednsLogging: false).EDNS;
        Assert.IsEmpty(edns);
    }

    private static LogEntry CreateEntry(
        EDnsExtendedDnsErrorCode code,
        string? extraText,
        bool ednsLogging = true)
    {
        var (request, _) = TestFixtures.CreateDnsExchange("ede.example");
        return CreateEntry(request, ednsLogging, (code, extraText));
    }

    private static LogEntry CreateEntry(params (EDnsExtendedDnsErrorCode Code, string? Text)[] errors)
    {
        var (request, _) = TestFixtures.CreateDnsExchange("ede.example");
        return CreateEntry(request, true, errors);
    }

    private static LogEntry CreateEntry(
        DnsDatagram request,
        bool ednsLogging,
        params (EDnsExtendedDnsErrorCode Code, string? Text)[] errors)
    {
        var options = errors.Select(error =>
            new EDnsOption(
                EDnsOptionCode.EXTENDED_DNS_ERROR,
                new EDnsExtendedDnsErrorOptionData(error.Code, error.Text))).ToArray();

        var response = new DnsDatagram(
            1, true, DnsOpcode.StandardQuery, false, false, true, true, false, false,
            DnsResponseCode.NxDomain,
            request.Question,
            udpPayloadSize: 1232,
            options: options);

        return new LogEntry(
            DateTime.UtcNow,
            new IPEndPoint(IPAddress.Loopback, 53000),
            DnsTransportProtocol.Udp,
            request,
            response,
            ednsLogging);
    }
}
