using UChainDB.BingChain.Contracts.Chain;

namespace DbSynchronizer
{
    public class SyncOption
    {
        public string SyncName { get; set; }
        public string DbConnString { get; set; }
        public string DbSqlSelect { get; set; }
        public string DbPkName { get; set; }
        public string ChainAddress { get; set; }
        public string ChainTableName { get; set; }
        public PrivateKey PrivateKey { get; set; }
    }
}