using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Docker.DotNet;
using Docker.DotNet.Models;
using System.Threading;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    [TestClass]
    public class TestBootstrapper
    {
        [AssemblyInitialize]
        public static void BootSqlServerOnLinuxWithDocker(TestContext tc)
        {
            DockerClient client = new DockerClientConfiguration(new Uri("tcp://localhost:2373")).CreateClient();
            client.Containers.StartContainerAsync("microsoft/mssql-server-linux", new ContainerStartParameters { }).Wait();
        }

        [AssemblyCleanup]
        public static void ShutdownSqlServerDockerImage()
        {
            DockerClient client = new DockerClientConfiguration(new Uri("tcp://localhost:2373")).CreateClient();
            client.Containers.StopContainerAsync("microsoft/mssql-server-linux", new ContainerStopParameters { }, new CancellationToken()).Wait();
        }
    }
}
