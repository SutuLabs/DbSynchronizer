using System;

namespace DbSynchronizer
{
    public class ComparableRow
    {
        public ComparableRow(string[] headers, string[] cells, string key)
        {
            this.Headers = headers;
            this.Cells = cells;
            this.Key = key;
        }

        public string[] Headers { get; set; }
        public string[] Cells { get; set; }
        public string Key { get; set; }

        public static bool operator ==(ComparableRow a, ComparableRow b)
        {
            if (object.ReferenceEquals(a, null))
            {
                return object.ReferenceEquals(b, null);
            }

            return a.Equals(b);
        }

        public static bool operator !=(ComparableRow a, ComparableRow b)
        {
            return !(a == b);
        }

        public override bool Equals(Object obj)
        {
            // Check for null values and compare run-time types.
            if (obj == null || GetType() != obj.GetType())
                return false;

            var cr = (ComparableRow)obj;
            if (Key != cr.Key) return false;
            if (Cells.Length != cr.Cells.Length) return false;
            int getCellIndex(string[] headers, string header) => Array.FindIndex(headers, _ => _ == header);
            for (int i = 0; i < Cells.Length; i++)
            {
                if (cr.Cells[getCellIndex(cr.Headers, Headers[i])] != Cells[i]) return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            var ret = Key.GetHashCode();
            foreach (var header in Headers)
            {
                ret &= header.GetHashCode();
            }
            foreach (var cell in Cells)
            {
                ret &= cell.GetHashCode();
            }

            return ret;
        }
    }
}