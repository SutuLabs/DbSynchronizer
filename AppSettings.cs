namespace DbSynchronizer
{
    public class AppSettings
    {
        public int SyncDelayInSeconds { get; set; }
        public SyncConfigurationEntity[] SyncConfigurations { get; set; }
        public SyncConfigurationEntity SyncConfigurationDefault { get; set; }

        public class SyncConfigurationEntity
        {
            public ChainEntity Chain { get; set; }
            public DatabaseEntity Database { get; set; }
        }

        public class ChainEntity
        {
            public string TableName { get; set; }
            public string Address { get; set; }
            public string PrivateKey { get; set; }
        }

        public class DatabaseEntity
        {
            public string ConnectionString { get; set; }
            public string PkName { get; set; }
            public string[] SqlSelect { get; set; }
        }
    }

}