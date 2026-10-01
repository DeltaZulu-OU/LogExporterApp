using LogExporter.Sinks;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace LogExporterApp.Tests;

[TestClass]
public sealed class HttpSinkTests
{
    [TestMethod]
    public async Task RecoversAfterNonSuccessResponseAsync()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        var endpoint = (IPEndPoint)listener.LocalEndpoint;
        using var sink = new HttpSink($"http://127.0.0.1:{endpoint.Port}/logs");

        var server = Task.Run(async () =>
        {
            await ServeHttpResponseAsync(listener, "500 Internal Server Error");
            await ServeHttpResponseAsync(listener, "204 No Content");
        }, TestContext.CancellationToken);

        var firstFailed = false;

        try
        {
            await sink.ExportAsync(
                [TestFixtures.CreateLogEntry("http-failure.example")],
                CancellationToken.None);
        }
        catch (HttpRequestException)
        {
            firstFailed = true;
        }

        Assert.IsTrue(firstFailed);

        await sink.ExportAsync(
            [TestFixtures.CreateLogEntry("http-recovery.example")],
            CancellationToken.None);

        await server.WaitAsync(TimeSpan.FromSeconds(2), TestContext.CancellationToken);
    }

    private static async Task ServeHttpResponseAsync(
        TcpListener listener,
        string status)
    {
        using var client = await listener.AcceptTcpClientAsync()
            .WaitAsync(TimeSpan.FromSeconds(2));
        await using var stream = client.GetStream();

        var buffer = new byte[4096];
        var received = 0;

        while (received < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(received));
            if (read == 0)
            {
                break;
            }

            received += read;

            if (Encoding.ASCII.GetString(buffer, 0, received)
                .Contains("\r\n\r\n", StringComparison.Ordinal))
            {
                break;
            }
        }

        var response =
            $"HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        var bytes = Encoding.ASCII.GetBytes(response);
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
    }

    public TestContext TestContext { get; set; }
}
