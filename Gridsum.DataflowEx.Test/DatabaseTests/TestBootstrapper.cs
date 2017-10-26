using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Docker.DotNet;
using Docker.DotNet.Models;
using System.Threading;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    [TestClass]
    public class TestBootstrapper
    {
        internal static string s_containerName = "mssql-for-dataflowex-test";
        internal static string s_saPassword = "UpLow123-+";
        internal static string s_imageName = "microsoft/mssql-server-linux:2017-GA";
        internal static string s_runningSqlServerContainerID;
        internal static Process s_sqlserverDockerProcess;

        [AssemblyInitialize]
        public static void BootSqlServerWithDockerCli(TestContext tc)
        {
            BootSqlServerWithDockerCli(tc, s_containerName);
        }

        public static void BootSqlServerWithDockerCli(TestContext tc, string containerName)
        {
            //using (var pullProcess = ExecuteCommand("docker", $"pull {s_imageName}", ".", Console.WriteLine, Console.WriteLine))
            //{
            //    pullProcess.WaitForExit();
            //}

            s_sqlserverDockerProcess = ExecuteCommand("docker", $"run --name \"{containerName}\" -e \"ACCEPT_EULA=Y\" -e \"SA_PASSWORD={s_saPassword}\" -a stdin -a stdout -a stderr -p 1433:1433 {s_imageName}", ".", Console.WriteLine, Console.WriteLine);
            Thread.Sleep(20 * 1000); //todo: improve this                      
        }

        [AssemblyCleanup]
        public static void ShutdownSqlServerWithDockerCli()
        {
            ShutdownSqlServerWithDockerCli(true, s_containerName);
        }

        public static void ShutdownSqlServerWithDockerCli(bool removeAfterShutDown, string containerName)
        {
            using (var process = ExecuteCommand("docker", $"stop \"{containerName}\"", ".", Console.WriteLine, Console.WriteLine))
            {
                process.WaitForExit();
            }

            s_sqlserverDockerProcess.Dispose();

            if (removeAfterShutDown)
            {
                using (var process2 = ExecuteCommand("docker", $"rm \"{containerName}\"", ".", Console.WriteLine, Console.WriteLine))
                {
                    process2.WaitForExit();
                }
            }
        }

        #region docker client approach (not working)
        public static void BootSqlServerWithDockerClient(TestContext tc)
        {
            DockerClient client = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")).CreateClient();
            var response = client.Containers.CreateContainerAsync(new CreateContainerParameters {
                Image = "microsoft/mssql-server-linux",
                AttachStderr = true,
                AttachStdin = true,
                AttachStdout = true,
                Env = new[] { "ACCEPT_EULA=Y", $"SA_PASSWORD={s_saPassword}" },
                ExposedPorts = new Dictionary<string, EmptyStruct>() {
                    { "1433/tcp", new EmptyStruct() }
                },
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>> {
                        {
                            "1433/tcp", new List<PortBinding> {
                                new PortBinding { HostPort = 1433.ToString() }
                            }
                        }
                    }
                }

            }).Result;
            s_runningSqlServerContainerID = response.ID;
            client.Containers.StartContainerAsync(s_runningSqlServerContainerID, new ContainerStartParameters { }).Wait();
        }

        public static void ShutdownSqlServerDockerImage()
        {
            DockerClient client = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")).CreateClient();
            client.Containers.StopContainerAsync(s_runningSqlServerContainerID, new ContainerStopParameters {
                WaitBeforeKillSeconds = 60
            }, new CancellationToken()).Wait();
        }
        #endregion

        static Process ExecuteCommand(string executable, string arguments, string workingDirectory, Action<string> output, Action<string> error)
        {           
            var process = new Process();                
            process.StartInfo.FileName = executable;
            process.StartInfo.Arguments = arguments;
            process.StartInfo.WorkingDirectory = workingDirectory;
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.CreateNoWindow = true;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.StandardOutputEncoding = Encoding.UTF8;
            process.StartInfo.StandardErrorEncoding = Encoding.UTF8;
                                
            process.OutputDataReceived += (sender, e) =>
            {
                output(e.Data);                            
            };

            process.ErrorDataReceived += (sender, e) =>
            {
                error(e.Data);
            };

            process.Start();

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            
            return process;             
        }
    }
}
