using LogExporter.Sinks;

namespace LogExporterApp.Tests;

[TestClass]
public sealed class SinkBatchOptionsTests
{
    [TestMethod]
    public void InvalidPropertiesIdentifyTheirNamesWithoutClaimingMethodParameters()
    {
        AssertInvalid(
            new SinkBatchOptions { QueueCapacity = 0 },
            nameof(SinkBatchOptions.QueueCapacity), 0);
        AssertInvalid(
            new SinkBatchOptions { BatchSize = 0 },
            nameof(SinkBatchOptions.BatchSize), 0);
        AssertInvalid(
            new SinkBatchOptions { BatchDelay = TimeSpan.Zero },
            nameof(SinkBatchOptions.BatchDelay), TimeSpan.Zero);
        AssertInvalid(
            new SinkBatchOptions { RetryTimeLimit = TimeSpan.FromMilliseconds(-1) },
            nameof(SinkBatchOptions.RetryTimeLimit), TimeSpan.FromMilliseconds(-1));
        AssertInvalid(
            new SinkBatchOptions { InitialRetryDelay = TimeSpan.Zero },
            nameof(SinkBatchOptions.InitialRetryDelay), TimeSpan.Zero);
        AssertInvalid(
            new SinkBatchOptions { MaxRetryDelay = TimeSpan.Zero },
            nameof(SinkBatchOptions.MaxRetryDelay), TimeSpan.Zero);
    }

    [TestMethod]
    public void DefaultOptionsAreValid()
    {
        new SinkBatchOptions().Validate();
    }

    private static void AssertInvalid(SinkBatchOptions options, string propertyName, object actualValue)
    {
        var exception = Assert.ThrowsExactly<ArgumentOutOfRangeException>(options.Validate);
        Assert.IsNull(exception.ParamName);
        Assert.AreEqual(actualValue, exception.ActualValue);
        StringAssert.Contains(exception.Message, propertyName);
    }
}
