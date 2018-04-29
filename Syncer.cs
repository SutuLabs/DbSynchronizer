using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using UChainDB.BingChain.Contracts.Chain;
using UChainDB.BingChain.Contracts.RpcCommands;
using UChainDB.BingChain.Contracts.SmartContracts;
using UChainDB.BingChain.Contracts.Utility;
using UChainDB.BingChain.Engine.Cryptography;
using UChainDB.BingChain.Engine.Network;
using UChainDB.BingChain.Network.JsonRpc;

namespace DbSynchronizer
{
    internal class Syncer
    {
        private SyncOption option;
        private readonly static ISignAlgorithm signAlgorithm = new ECDsaSignAlgorithm();
        private PrivateKey privateKey;
        private ILogger logger;

        public Syncer(SyncOption option, ILogger logger)
        {
            this.option = option;
            this.privateKey = option.PrivateKey;
            this.logger = logger;
        }

        public async Task Sync()
        {
            try
            {
                var (originDt, targetResp) = await PrepareDataAsync();
                if (originDt == null || targetResp == null) return;

                var ot = ComparableTable.FromDataTable(originDt);
                var tt = ComparableTable.FromChainResponse(targetResp);

                if (!ComparableTable.CompareSchema(ot, tt))
                {
                    logger.LogWarning($"[{this.option.SyncName}]schema is different, unable to sync. Database side: [{string.Join(",", ot.Headers)}], Chain side: [{string.Join(",", tt.Headers)}]");
                    return;
                }

                var actions = new List<DataAction>();
                var removed = ComparableTable.FindRemoved(this.option.ChainTableName, tt, ot).ToArray();
                var added = ComparableTable.FindAdded(this.option.ChainTableName, tt, ot).ToArray();
                var modified = ComparableTable.FindModified(this.option.ChainTableName, tt, ot).ToArray();
                actions.AddRange(removed);
                actions.AddRange(added);
                actions.AddRange(modified);

                var actionBatchSize = 10;
                var actionBatchList = actions
                    .Select((val, idx) => new { val, idx })
                    .GroupBy(_ => _.idx / actionBatchSize)
                    .Select(g => g.Select(_ => _.val).ToArray())
                    .ToArray();

                var witness = (await GetStatusAsync(this.option.ChainAddress)).Tail.Hash;

                foreach (var actionBatch in actionBatchList)
                {
                    await DeployDataAsync(this.option.ChainAddress, this.privateKey, witness, actionBatch);
                    await Task.Delay(100); // wait for chain to finish create transaction
                }

                logger.LogInformation($"[{this.option.SyncName}] sync finished, {added.Length} record(s) added, {removed.Length} record(s) removed, {modified.Length} record(s) modified,");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, $"[{this.option.SyncName}]Exception when doing sync, ignore sync.");
            }
        }

        public async Task<(DataTable, QueryDataRpcResponse)> PrepareDataAsync()
        {
            DataTable originDt = null;
            try
            {
                originDt = await GetOriginalDataAsync(this.option.DbConnString, this.option.DbSqlSelect, this.option.DbPkName);
            }
            catch (SqlException ex)
            {
                logger.LogWarning(ex, $"[{this.option.SyncName}]SQL exception when getting data from origin database, ignore sync.");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, $"[{this.option.SyncName}]Exception when getting data from origin database, ignore sync.");
            }

            if (originDt == null) return (null, null);

            QueryDataRpcResponse targetResp = null;
            try
            {
                targetResp = await GetTargetChainDataAsync(this.option.ChainAddress, this.option.ChainTableName, this.logger);
            }
            catch (ArgumentException ex)
            {
                logger.LogWarning($"[{this.option.SyncName}]: {ex.Message}");
            }
            catch (ApiClientException)
            {
                logger.LogWarning($"[{this.option.SyncName}][{this.option.ChainAddress}]: No Connection, ignore sync.");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, $"[{this.option.SyncName}][{this.option.ChainTableName}]: Exception when getting data, ignore sync.");
            }

            return (originDt, targetResp);
        }

        private static Task<DataTable> GetOriginalDataAsync(string connString, string sql, string pkName)
        {
            using (var connection = new SqlConnection(connString))
            {
                var dt = new DataTable();
                var da = new SqlDataAdapter(sql, connection);
                da.Fill(dt);
                if (dt.PrimaryKey.Length == 0)
                {
                    dt.PrimaryKey = new[] { dt.Columns.OfType<DataColumn>().First(_ => _.ColumnName == pkName) };
                }
                return Task.FromResult(dt);
            }
        }

        private static async Task<QueryDataRpcResponse> GetTargetChainDataAsync(string address, string tableName, ILogger logger)
        {
            var tableresponse = await new JsonRpcRequest
            {
                Method = Commands.ListTables,
            }.ConnectAndRequest<ListTablesRpcResponse>(address);
            var tables = tableresponse.Result.Tables;

            if (tables.All(_ => _.Name != tableName))
            {
                throw new ArgumentException($"Table [{tableName}] not exist in target [{address}].");
            }

            var result = new QueryDataRpcResponse();
            var data = new List<string>();
            var dataHistories = new List<HistoryEntry>();

            using (var client = new WebApiClient())
            {
                try
                {
                    await client.ConnectAsync(address);
                }
                catch (ApiClientException acex)
                {
                    throw new ApiClientException($"Cannot connect to server {address}, due to {acex.Message}", acex);
                }

                if (!client.IsConnected)
                {
                    throw new ApiClientException($"open channel failed");
                }

                var pos = 0;
                while (true)
                {
                    var request = new JsonRpcRequest
                    {
                        Method = Commands.QueryData,
                        Parameters = new QueryDataRpcRequest { TableName = tableName, Start = pos, Count = 100, }
                    };
                    var response = await client.RequestAsync<QueryDataRpcResponse>(request);
                    var query = response.Result;
                    if (result.Headers == null)
                    {
                        result.Headers = query.Headers;
                        result.HeaderHistories = query.HeaderHistories;
                        result.PrimaryKeyName = query.PrimaryKeyName;
                    }
                    if (query.Rows.Length == 0) break;
                    data.AddRange(query.Data);
                    dataHistories.AddRange(query.DataHistories);
                    pos += query.Rows.Length;
                }
            }

            result.Data = data.ToArray();
            result.DataHistories = dataHistories.ToArray();
            return result;
        }

        private static async Task<StatusRpcResponse> GetStatusAsync(string address)
        {
            var response = await new JsonRpcRequest
            {
                Method = Commands.Status,
            }.ConnectAndRequest<StatusRpcResponse>(address);

            return response.Result;
        }

        private static async Task<UInt256> DeployDataAsync(string nodeAddress, PrivateKey initiatorPrivateKey, UInt256 witness, params DataAction[] actions)
        {
            var initiatorAddress = initiatorPrivateKey.GetAddress(signAlgorithm);
            var tran = new DataTransaction(initiatorAddress, null, witness, actions);
            var unlockscript = signAlgorithm.Sign((byte[])tran.GetLockHash(), initiatorPrivateKey);
            tran.UnlockScripts = new UnlockScripts(new[] { new ScriptToken(unlockscript.ToString()) });

            tran.Sign(signAlgorithm, initiatorPrivateKey);
            var signature = tran.Signature.ToString();

            var response = await new JsonRpcRequest
            {
                Method = Commands.CreateDataTransaction,
                Parameters = new CreateDataTransactionRpcRequest
                {
                    Initiator = initiatorAddress.ToString(),
                    Signature = signature.ToString(),
                    WitnessBlock = witness,
                    UnlockScripts = tran.UnlockScripts,
                    Actions = actions,
                }
            }.ConnectAndRequest<CreateTransactionRpcResponse>(nodeAddress);

            if (response.Error != null)
            {
                throw new EvaluateException($"error when deploy transaction: [{response.Error.Code}]{response.Error.Message}");
            }

            return response.Result.TransactionId;
        }
    }
}