using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using UChainDB.BingChain.Contracts.Chain;

namespace DbSynchronizer
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables();

            var configuration = builder.Build();

            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole(configuration.GetSection("Logging"));
            loggerFactory.AddFile(configuration.GetSection("Logging"));
            var logger = loggerFactory.CreateLogger("Syncer");

            var appsettings = new AppSettings();
            configuration.GetSection("AppSettings").Bind(appsettings);
            var syncDelay = appsettings.SyncDelayInSeconds;

            var def = appsettings.SyncConfigurationDefault;
            var syncerList = appsettings.SyncConfigurations
                .Select((_, i) => new SyncOption
                {
                    SyncName = $"Sync {i}",
                    ChainAddress = _.Chain.Address ?? def.Chain.Address,
                    ChainTableName = _.Chain.TableName ?? def.Chain.TableName,
                    PrivateKey = new PrivateKey(_.Chain.PrivateKey ?? def.Chain.PrivateKey),
                    DbConnString = _.Database.ConnectionString ?? def.Database.ConnectionString,
                    DbPkName = _.Database.PkName ?? def.Database.PkName,
                    DbSqlSelect = string.Join(" ", _.Database.SqlSelect ?? def.Database.SqlSelect),
                })
                .Select(_ => new Syncer(_, logger))
                .ToArray();

            var stopFlag = false;
            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);
            void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
            {
                e.Cancel = true;
                stopFlag = true;
                Console.WriteLine("Prepared for graceful shutdown...");
            }

            logger.LogInformation($"System started, Ctrl + C to terminate...");
            logger.LogInformation($"Syncing to [{string.Join(",", appsettings.SyncConfigurations.Select(_ => $"{_.Chain.Address}|{_.Chain.TableName}"))}] by [{syncDelay}s] delay");

            while (!stopFlag)
            {
                Task.WaitAll(syncerList.Select(_ => _.Sync()).ToArray());
                logger.LogInformation($"[{DateTime.Now}]Sync finished");
                if (syncDelay == 0) break;

                var dtStart = DateTime.Now;
                while ((DateTime.Now - dtStart).TotalSeconds < syncDelay)
                {
                    Task.Delay(500).Wait();
                    if (stopFlag) break;
                }
            }

            logger.LogInformation("Shutdown");
        }
    }
}