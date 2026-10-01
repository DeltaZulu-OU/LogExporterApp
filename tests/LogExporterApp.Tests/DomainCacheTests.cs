using LogExporter.Pipeline;
using Nager.PublicSuffix;
using Nager.PublicSuffix.RuleProviders;

namespace LogExporterApp.Tests;

[TestClass]
public sealed class DomainCacheTests
{
    [TestMethod]
    public async Task DoesNotBlockOrCacheBeforeParserIsReadyAsync()
    {
        var parserSource = new TaskCompletionSource<DomainParser?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new Normalize.DomainCache(parserSource.Task);
        const string domainName = "www.example.com";

        var pendingLookup = Task.Run(() => cache.GetOrAdd(domainName));
        var beforeReady = await pendingLookup.WaitAsync(TimeSpan.FromSeconds(1), TestContext.CancellationToken);

        Assert.IsNull(beforeReady.RegistrableDomain);

        var directory = TestFixtures.CreateTempDirectory();
        var rulesPath = Path.Combine(directory, "public_suffix_list.dat");

        try
        {
            await File.WriteAllTextAsync(
                rulesPath,
                """
                // ===BEGIN ICANN DOMAINS===
                com
                org
                co.uk
                // ===END ICANN DOMAINS===
                """, TestContext.CancellationToken);

            var ruleProvider = new LocalFileRuleProvider(rulesPath);
            await ruleProvider.BuildAsync(cancellationToken: TestContext.CancellationToken);

            parserSource.TrySetResult(new DomainParser(ruleProvider));

            var afterReady = cache.GetOrAdd(domainName);

            Assert.AreEqual("example.com", afterReady.RegistrableDomain);
            Assert.AreEqual("example", afterReady.Domain);

            var normalizedLookup = cache.GetOrAdd("WWW.EXAMPLE.COM.");
            Assert.AreEqual("example.com", normalizedLookup.RegistrableDomain);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    public TestContext TestContext { get; set; }
}
