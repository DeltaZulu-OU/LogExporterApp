using LogForwarder.Sinks;

namespace LogForwarderApp.Tests;

[TestClass]
public sealed class SinkBatchOptionsTests
{
    [TestMethod]
    public void InvalidPropertiesIdentifyTheirNamesWithoutClaimingMethodParameters()
    {
        AssertInvalid(
            new SinkBatchOptions { QueueCapacity = 0 },
            nameof(SinkBatchOptions.QueueCapacity));
        AssertInvalid(
            new SinkBatchOptions { BatchSize = 0 },
            nameof(SinkBatchOptions.BatchSize));
        AssertInvalid(
            new SinkBatchOptions { BatchDelay = TimeSpan.Zero },
            nameof(SinkBatchOptions.BatchDelay));
        AssertInvalid(
            new SinkBatchOptions { RetryTimeLimit = TimeSpan.FromMilliseconds(-1) },
            nameof(SinkBatchOptions.RetryTimeLimit));
        AssertInvalid(
            new SinkBatchOptions { InitialRetryDelay = TimeSpan.Zero },
            nameof(SinkBatchOptions.InitialRetryDelay));
        AssertInvalid(
            new SinkBatchOptions { MaxRetryDelay = TimeSpan.Zero },
            nameof(SinkBatchOptions.MaxRetryDelay));
    }

    [TestMethod]
    public void DefaultOptionsAreValid()
    {
        new SinkBatchOptions().Validate();
    }

    private static void AssertInvalid(SinkBatchOptions options, string propertyName)
    {
        var exception = Assert.ThrowsExactly<InvalidOperationException>(options.Validate);
        StringAssert.Contains(exception.Message, propertyName);
    }
}
