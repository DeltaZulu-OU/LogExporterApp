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
}
