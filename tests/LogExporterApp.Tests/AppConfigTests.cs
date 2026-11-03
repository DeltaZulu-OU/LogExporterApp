using LogExporter;

namespace LogExporterApp.Tests;

[TestClass]
public sealed class AppConfigTests
{
    [TestMethod]
    public void EnabledTaggingRequiresAtLeastOneTag()
    {
        foreach (var config in new[]
        {
            """{"sinks":{},"pipeline":{"tagging":{"enabled":true}}}""",
            """{"sinks":{},"pipeline":{"tagging":{"enabled":true,"tags":[]}}}"""
        })
        {
            var rejected = false;

            try
            {
                AppConfig.Deserialize(config);
            }
            catch (System.ComponentModel.DataAnnotations.ValidationException)
            {
                rejected = true;
            }

            Assert.IsTrue(rejected);
        }
    }
    [TestMethod]
    public void InvalidEnabledSinkBatchingIsRejected()
    {
        var config = """{"sinks":{"http":{"enabled":true,"endpoint":"https://example.com/logs","batching":{"batchSize":0}}},"pipeline":{}}""";
        Assert.ThrowsExactly<System.ComponentModel.DataAnnotations.ValidationException>(
            () => AppConfig.Deserialize(config));
    }

    [TestMethod]
    public void ValidSinkBatchingIsAccepted()
    {
        var config = """{"sinks":{"console":{"enabled":true,"batching":{"batchSize":500,"retryTimeLimitMs":1000}}},"pipeline":{}}""";
        var parsed = AppConfig.Deserialize(config);
        Assert.AreEqual(500, parsed.Sinks.ConsoleSinkConfig!.Batching!.BatchSize);
        Assert.AreEqual(1000, parsed.Sinks.ConsoleSinkConfig.Batching.RetryTimeLimitMs);
    }
}
