using LogForwarder;

namespace LogForwarderApp.Tests;

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
    [TestMethod]
    public void HttpBatchReplayIsDisabledByDefaultAndRequiresOptIn()
    {
        var baseline = AppConfig.Deserialize(
            """{"sinks":{"http":{"enabled":true,"endpoint":"https://example.com/logs"}},"pipeline":{}}""");
        Assert.IsFalse(baseline.Sinks.HttpSinkConfig!.AllowBatchReplay);

        var optedIn = AppConfig.Deserialize(
            """{"sinks":{"http":{"enabled":true,"endpoint":"https://example.com/logs","allowBatchReplay":true}},"pipeline":{}}""");
        Assert.IsTrue(optedIn.Sinks.HttpSinkConfig!.AllowBatchReplay);
    }

    [TestMethod]
    public void ClickHouseConfigurationIsValidatedOnlyWhenEnabled()
    {
        var disabled = AppConfig.Deserialize(
            """{"sinks":{"clickhouse":{"enabled":false}},"pipeline":{}}""");
        Assert.IsFalse(disabled.Sinks.ClickHouseSinkConfig!.Enabled);

        var active = AppConfig.Deserialize(
            """{"sinks":{"clickhouse":{"enabled":true,"connectionString":"Host=clickhouse.example;Port=8443;Database=dns","database":"dns","table":"events","batching":{"batchSize":250}}},"pipeline":{}}""");
        Assert.AreEqual("dns", active.Sinks.ClickHouseSinkConfig!.Database);
        Assert.AreEqual(250, active.Sinks.ClickHouseSinkConfig.Batching!.BatchSize);
        Assert.IsFalse(active.Sinks.ClickHouseSinkConfig.AllowBatchReplay);

        Assert.ThrowsExactly<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            AppConfig.Deserialize(
                """{"sinks":{"clickhouse":{"enabled":true,"connectionString":"Host=localhost;Port=8123","database":"dns","table":"events","allowBatchReplay":true}},"pipeline":{}}"""));

        Assert.ThrowsExactly<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            AppConfig.Deserialize(
                """{"sinks":{"clickhouse":{"enabled":true,"database":"dns","table":"events"}},"pipeline":{}}"""));
        Assert.ThrowsExactly<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            AppConfig.Deserialize(
                """{"sinks":{"clickhouse":{"enabled":true,"connectionString":"Host=clickhouse.example;Port=8443;Database=dns","database":"dns","table":"events;DROP"}},"pipeline":{}}"""));
        Assert.ThrowsExactly<System.ComponentModel.DataAnnotations.ValidationException>(() =>
            AppConfig.Deserialize(
                """{"sinks":{"clickhouse":{"enabled":true,"connectionString":"Host=clickhouse.example;Port=8443;Database=dns","database":"dns","table":"events","batching":{"batchDelayMs":0}}},"pipeline":{}}"""));
    }

    [TestMethod]
    public void ClickHouseConfigurationRejectsMissingOrInvalidIdentifiers()
    {
        const string connection = "Host=localhost;Port=8123;Database=dns";
        foreach (var (database, table) in new[]
        {
            ("", "events"),
            ("dns", ""),
            ("dns analytics", "events"),
            ("dns", "events-name"),
            ("dns", "123events"),
            ("dns;DROP", "events")
        })
        {
            var json = System.Text.Json.JsonSerializer.Serialize(new
            {
                sinks = new
                {
                    clickhouse = new
                    {
                        enabled = true,
                        connectionString = connection,
                        database,
                        table
                    }
                },
                pipeline = new { }
            });
            Assert.ThrowsExactly<System.ComponentModel.DataAnnotations.ValidationException>(
                () => AppConfig.Deserialize(json));
        }
    }

    [TestMethod]
    public void DisabledClickHouseIgnoresInvalidSinkSpecificSettings()
    {
        var parsed = AppConfig.Deserialize(
            """{"sinks":{"clickhouse":{"enabled":false,"allowBatchReplay":true,"database":"invalid-name","batching":{"batchSize":0}}},"pipeline":{}}""");
        Assert.IsFalse(parsed.Sinks.ClickHouseSinkConfig!.Enabled);
    }

    [TestMethod]
    public void ClickHouseReplayOptInIsRejectedRegardlessOfRetryBudget()
    {
        foreach (var retryBudget in new[] { 0, 2000 })
        {
            var json = System.Text.Json.JsonSerializer.Serialize(new
            {
                sinks = new
                {
                    clickhouse = new
                    {
                        enabled = true,
                        connectionString = "Host=localhost;Port=8123",
                        database = "dns",
                        table = "events",
                        allowBatchReplay = true,
                        batching = new { retryTimeLimitMs = retryBudget }
                    }
                },
                pipeline = new { }
            });
            Assert.ThrowsExactly<System.ComponentModel.DataAnnotations.ValidationException>(
                () => AppConfig.Deserialize(json));
        }
    }

}
