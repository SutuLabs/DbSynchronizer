using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using UChainDB.BingChain.Contracts.Chain;
using UChainDB.BingChain.Contracts.RpcCommands;

namespace DbSynchronizer
{
    public class ComparableTable
    {
        public ComparableTable(string[] headers, ComparableRow[] rows, string primaryKeyName)
        {
            this.Headers = headers;
            this.Rows = rows.ToDictionary(_ => _.Key);
            this.PrimaryKeyName = primaryKeyName;
        }

        public string PrimaryKeyName { get; set; }
        public string[] Headers { get; set; }
        public IDictionary<string, ComparableRow> Rows { get; set; }

        public static ComparableTable FromDataTable(DataTable dt)
        {
            var headers = dt.Columns
                .OfType<DataColumn>()
                .Select(_ => _.ColumnName)
                .ToArray();
            var pkcol = dt.PrimaryKey.FirstOrDefault();
            if (pkcol == null)
            {
                throw new ArgumentException("The DataTable lacks of primary key which is mandatory");
            }

            var pkname = pkcol.ColumnName;
            var pkidx = Array.FindIndex(headers, _ => _ == pkname);
            var rows = dt.Rows
                .OfType<DataRow>()
                .Select(r => new ComparableRow(headers, r.ItemArray.Select(_ => _.ToString()).ToArray(), r.ItemArray[pkidx].ToString()))
                .ToArray();
            return new ComparableTable(headers, rows, pkname);
        }

        public static ComparableTable FromChainResponse(QueryDataRpcResponse response)
        {
            var pkname = response.PrimaryKeyName;
            var headers = response.Headers;
            var pkidx = Array.FindIndex(headers, _ => _ == pkname);
            var rows = response.Rows
                .Select(_ => new ComparableRow(headers, _.Cells, _.Cells[pkidx]))
                .ToArray();

            return new ComparableTable(headers, rows, pkname);
        }

        internal static bool CompareSchema(ComparableTable a, ComparableTable b)
        {
            if (a.PrimaryKeyName != b.PrimaryKeyName) return false;
            if (a.Headers.Length != b.Headers.Length) return false;
            for (int i = 0; i < a.Headers.Length; i++)
            {
                if (!b.Headers.Contains(a.Headers[i])) return false;
            }

            return true;
        }

        internal static IEnumerable<DataAction> FindRemoved(string tableName, ComparableTable baseTable, ComparableTable comparativeTable)
        {
            if (!CompareSchema(baseTable, comparativeTable))
            {
                throw new ArgumentException("Cannot to find removed rows when schema is different.");
            }

            foreach (var baseRow in baseTable.Rows)
            {
                if (!comparativeTable.Rows.ContainsKey(baseRow.Key))
                {
                    yield return new DeleteDataAction { SchemaName = tableName, PrimaryKeyValue = baseRow.Key };
                }
            }
        }

        internal static IEnumerable<DataAction> FindAdded(string tableName, ComparableTable baseTable, ComparableTable comparativeTable)
        {
            if (!CompareSchema(baseTable, comparativeTable))
            {
                throw new ArgumentException("Cannot to find added rows when schema is different.");
            }

            foreach (var compRow in comparativeTable.Rows)
            {
                if (!baseTable.Rows.ContainsKey(compRow.Key))
                {
                    var columns = compRow.Value.Cells
                        .Select((_, i) => new ColumnData { Name = comparativeTable.Headers[i], Data = _ })
                        .ToArray();
                    yield return new InsertDataAction { SchemaName = tableName, Columns = columns };
                }
            }
        }

        internal static IEnumerable<DataAction> FindModified(string tableName, ComparableTable baseTable, ComparableTable comparativeTable)
        {
            if (!CompareSchema(baseTable, comparativeTable))
            {
                throw new ArgumentException("Cannot to find modified rows when schema is different.");
            }

            foreach (var baseRow in baseTable.Rows)
            {
                if (comparativeTable.Rows.ContainsKey(baseRow.Key) && baseRow.Value != comparativeTable.Rows[baseRow.Key])
                {
                    var compRow = comparativeTable.Rows[baseRow.Key];
                    int getCellIndex(string header) => Array.FindIndex(comparativeTable.Headers, _ => _ == header);
                    var columns = baseRow.Value.Cells
                        .Select((_, i) => _ == compRow.Cells[getCellIndex(baseTable.Headers[i])] ? null : new ColumnData { Name = baseTable.Headers[i], Data = compRow.Cells[getCellIndex(baseTable.Headers[i])] })
                        .Where(_ => _ != null)
                        .ToArray();
                    yield return new UpdateDataAction { SchemaName = tableName, PrimaryKeyValue = baseRow.Key, Columns = columns };
                }
            }
        }
    }
}