using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace LogExporter.Tests;

[TestClass]
public sealed class AppLifecycleTests
{
    [TestMethod]
    public async Task ReloadDrainsOldGenerationBeforeSwitchAsync()
    {
        var directory = TestFixtures.CreateTempDirectory();
        var oldPath = Path.Combine(directory, "old.ndjson");
        var newPath = Path.Combine(directory, "new.ndjson");

        try
        {
            using var app = new App();
            var dnsServer = TestFixtures.CreateDnsServer();

            await app.InitializeAsync(dnsServer, TestFixtures.FileConfig(oldPath));

            for (var i = 0; i < 64; i++)
            {
                await TestFixtures.InsertAsync(app, $"old-{i}.example");
            }

            await app.InitializeAsync(dnsServer, TestFixtures.FileConfig(newPath));

            for (var i = 0; i < 64; i++)
            {
                await TestFixtures.InsertAsync(app, $"new-{i}.example");
            }

            app.Dispose();

            var oldNames = TestFixtures.ReadQuestionNames(oldPath);
            var newNames = TestFixtures.ReadQuestionNames(newPath);

            Assert.AreEqual(64, oldNames.Count);
            Assert.AreEqual(64, newNames.Count);
            Assert.IsTrue(oldNames.All(name => name.StartsWith("old-", StringComparison.Ordinal)));
            Assert.IsTrue(newNames.All(name => name.StartsWith("new-", StringComparison.Ordinal)));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [TestMethod]
    public async Task InvalidReloadPreservesActiveGenerationAsync()
    {
        var directory = TestFixtures.CreateTempDirectory();
        var path = Path.Combine(directory, "active.ndjson");

        try
        {
            using var app = new App();
            var dnsServer = TestFixtures.CreateDnsServer();

            await app.InitializeAsync(dnsServer, TestFixtures.FileConfig(path));
            await TestFixtures.InsertAsync(app, "before-invalid.example");

            var rejected = false;

            try
            {
                await app.InitializeAsync(dnsServer, """{"sinks":{} }""");
            }
            catch
            {
                rejected = true;
            }

            Assert.IsTrue(rejected);

            await TestFixtures.InsertAsync(app, "after-invalid.example");
            app.Dispose();

            var names = TestFixtures.ReadQuestionNames(path);

            CollectionAssert.Contains(names, "before-invalid.example");
            CollectionAssert.Contains(names, "after-invalid.example");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [TestMethod]
    public async Task InvalidEnabledSinkReloadPreservesActiveGenerationAsync()
    {
        var directory = TestFixtures.CreateTempDirectory();
        var path = Path.Combine(directory, "active-nested.ndjson");

        try
        {
            using var app = new App();
            var dnsServer = TestFixtures.CreateDnsServer();

            await app.InitializeAsync(dnsServer, TestFixtures.FileConfig(path));
            await TestFixtures.InsertAsync(app, "before-invalid-sink.example");

            var rejected = false;

            try
            {
                await app.InitializeAsync(
                    dnsServer,
                    """{"sinks":{"file":{"enabled":true}},"pipeline":{}}""");
            }
            catch (System.ComponentModel.DataAnnotations.ValidationException)
            {
                rejected = true;
            }

            Assert.IsTrue(rejected);

            await TestFixtures.InsertAsync(app, "after-invalid-sink.example");
            app.Dispose();

            var names = TestFixtures.ReadQuestionNames(path);

            CollectionAssert.Contains(names, "before-invalid-sink.example");
            CollectionAssert.Contains(names, "after-invalid-sink.example");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [TestMethod]
    public async Task DisposeDuringActiveIngestionIsSafeAsync()
    {
        var directory = TestFixtures.CreateTempDirectory();
        var path = Path.Combine(directory, "dispose.ndjson");

        try
        {
            var app = new App();
            var dnsServer = TestFixtures.CreateDnsServer();

            await app.InitializeAsync(
                dnsServer,
                TestFixtures.FileConfig(path, queueSize: 4096));

            var started = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            Exception? ingestionFailure = null;

            var producer = Task.Run(async () =>
            {
                try
                {
                    for (var i = 0; i < 1000; i++)
                    {
                        await TestFixtures.InsertAsync(app, $"dispose-{i}.example");

                        if (i == 10)
                        {
                            started.TrySetResult();
                        }

                        if ((i & 31) == 0)
                        {
                            await Task.Yield();
                        }
                    }
                }
                catch (Exception ex)
                {
                    ingestionFailure = ex;
                }
            });

            await started.Task.WaitAsync(TimeSpan.FromSeconds(2));

            await Task.Run(app.Dispose).WaitAsync(TimeSpan.FromSeconds(5));
            await producer.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.IsNull(ingestionFailure);
            Assert.IsTrue(File.Exists(path));
            Assert.IsTrue(TestFixtures.ReadQuestionNames(path).Count > 0);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [TestMethod]
    public async Task InitializeAfterDisposeIsRejectedAsync()
    {
        var app = new App();
        var dnsServer = TestFixtures.CreateDnsServer();

        app.Dispose();

        var rejected = false;

        try
        {
            await app.InitializeAsync(dnsServer, """{"sinks":{},"pipeline":{}}""");
        }
        catch (ObjectDisposedException)
        {
            rejected = true;
        }

        Assert.IsTrue(rejected);
    }
}
