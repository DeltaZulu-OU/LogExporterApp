using LogExporter.Pipeline;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace LogExporter.Tests;

[TestClass]
public sealed class PipelineDispatcherTests
{
    [TestMethod]
    public void ProcessorsRunInRegistrationOrder()
    {
        using var dispatcher = new PipelineDispatcher();
        var order = new List<string>();

        dispatcher.Add(new FirstTestProcessor(order));
        dispatcher.Add(new SecondTestProcessor(order));

        dispatcher.Run(TestFixtures.CreateLogEntry("order.example"));

        CollectionAssert.AreEqual(
            new[] { "first", "second" },
            order);
    }

    [TestMethod]
    public void DuplicateProcessorTypesAreRejected()
    {
        using var dispatcher = new PipelineDispatcher();
        var order = new List<string>();

        dispatcher.Add(new FirstTestProcessor(order));

        var rejected = false;
        var duplicate = new FirstTestProcessor(order);

        try
        {
            dispatcher.Add(duplicate);
        }
        catch (InvalidOperationException)
        {
            rejected = true;
            duplicate.Dispose();
        }

        Assert.IsTrue(rejected);
    }

    [TestMethod]
    public void RemoveAndReAddMovesProcessorToEnd()
    {
        using var dispatcher = new PipelineDispatcher();
        var order = new List<string>();

        dispatcher.Add(new FirstTestProcessor(order));
        dispatcher.Add(new SecondTestProcessor(order));

        dispatcher.Remove(typeof(FirstTestProcessor));
        dispatcher.Add(new FirstTestProcessor(order));

        dispatcher.Run(TestFixtures.CreateLogEntry("reorder.example"));

        CollectionAssert.AreEqual(
            new[] { "second", "first" },
            order);
    }

    [TestMethod]
    public void ErrorCallbackCannotBreakIsolation()
    {
        using var dispatcher = new PipelineDispatcher();
        dispatcher.Add(new ThrowingTestProcessor());

        dispatcher.Run(
            TestFixtures.CreateLogEntry("callback-isolation.example"),
            _ => throw new InvalidOperationException("Synthetic callback failure."));
    }
}
