using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kiwi
{
    public class KeyValueStore
    {
        private CloudTable table;

        private static string keyPrefix = "KEY-";
        private static string keyPrefixEnd = "KEZ";
        private static string lockPrefix = "LOCK-";
        private static string lockPrefixEnd = "LOCL";
        private static string writeLockKey = lockPrefix + "WRITE";
        private static string siReadLockPrefix = lockPrefix + "SIR-";
        private static string siReadLockPrefixEnd = lockPrefix + "SIS-";

        public class KeyEntity : TableEntity
        {
            private static int maxVersionLength = (ulong.MaxValue + "").Length;
            public KeyEntity() { }
            public KeyEntity(string key, ulong version, string value)
            {
                this.PartitionKey =  key;
                this.RowKey = keyPrefix + FormatVersionToString(version);
                this.Value = value;
            }

            public string CreatorTransaction { get; set; }

            public bool? IsTransactionRunning { get; set; } = null;

            public string Value { get; set; }

            public bool Tombstoned { get; set; } = false;

            public ulong Version => ulong.Parse(this.RowKey.TrimStart(keyPrefix.ToCharArray()));

            public static string FormatVersionToString(ulong version)
            {
                return version.ToString().PadLeft(maxVersionLength, '0');
            }
        }

        public class WriteLockEntity : TableEntity
        {
            public string TransactionId { get; set; }
            public WriteLockEntity() { }
            public WriteLockEntity(string key, string transactionId)
            {
                this.PartitionKey = key;
                this.RowKey = writeLockKey;
                this.TransactionId = transactionId;
            }
        }

        public class SiReadLockEntity : TableEntity
        {
            public SiReadLockEntity() { }
            public SiReadLockEntity(string key, string transactionId)
            {
                this.PartitionKey = key;
                this.RowKey = siReadLockPrefix + transactionId;
            }

            public string TransactionId => this.RowKey.TrimStart(siReadLockPrefix.ToCharArray());
        }

        public KeyValueStore()
        {
            string storageConnectionString = "<storage connection string>"
            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
            var tableClient = account.CreateCloudTableClient();

            table = tableClient.GetTableReference("kiwitest");
        }


        public ulong Create(string key, string value, string transactionId)
        {
            // convert it to azure storage exception.
            var entities = GetKeyVersionsGreaterThanOrEqual(key, 0);
            ulong latestVersionToBeInserted = 0;

            if(entities.Any())
            {
                if(entities.Last().Tombstoned)
                {
                    latestVersionToBeInserted = entities.Last().Version + 1;
                }
                else
                {
                    throw new ArgumentException("Entity already exists");
                }
            }

            return SetKeyWithVersionToValue(key, value, latestVersionToBeInserted, transactionId);
        }

        public ulong Overwrite(string key, string value, string transactionId)
        {
            var entities = GetKeyVersionsGreaterThanOrEqual(key, 0);
            ulong? currentVersion = null;
            if(entities.Any())
            {
                currentVersion = entities.Last().Version;
            }

            return SetKeyWithVersionToValue(key, value, (currentVersion ?? 0) + 1, transactionId);
        }

        public ulong Update(string key, string value, ulong latestCommitedVersionShouldBe, string transactionId)
        {
            var entities = GetKeyVersionsGreaterThanOrEqual(key, latestCommitedVersionShouldBe);
            if(entities.Count() == 1)
            {
                var nextVersion = entities.Last().Version + 1;
                return SetKeyWithVersionToValue(key, value, nextVersion, transactionId);
            }
            else
            {
                // TODO: remove this exception. use azure storage concurrency.
                throw new ArgumentException("newer version available");
            }
        }

        public ulong Delete(string key, ulong latestCommitedVersionShouldBe, string transactionId)
        {
            // TODO: merge this with Set using KeyEntity class
            var entities = GetKeyVersionsGreaterThanOrEqual(key, latestCommitedVersionShouldBe);
            if (entities.Count() == 1)
            {
                if(entities.Last().Tombstoned)
                {
                    throw new ArgumentException("Key does not exist");
                }

                var nextVersion = entities.Last().Version + 1;
                var kv = new KeyEntity(key, nextVersion, null)
                {
                    Tombstoned = true,
                    CreatorTransaction = transactionId
                };

                return SetKeyWithVersionToValue(kv);
            }
            else
            {
                // TODO: remove this exception. use azure storage concurrency.
                throw new ArgumentException("Key does not exist");
            }
        }

        public string Read(string key)
        {
            return Read(key, out ulong version);
        }

        public string Read(string key, out ulong version)
        {
            var entities = GetKeyVersionsGreaterThanOrEqual(key, 0);

            if (entities.Count() == 0 || entities.Last().Tombstoned)
            {
                throw new ArgumentException("key does not exist");
            }

            var latestEntity = entities.Last();
            version = latestEntity.Version;
            return latestEntity.Value;
        }

        public IEnumerable<string> ReadAllTransactionsGreaterThanVersion(string key, ulong version)
        {
            var entities = GetKeyVersionsGreaterThanOrEqual(key, version + 1);
            return entities.Select(e => e.CreatorTransaction);
        }

        public void AcquireWriteLock(string key, string transactionId)
        {
            var writeLockEntity = new WriteLockEntity(key, transactionId);
            TableOperation opr = TableOperation.Insert(writeLockEntity);

            table.Execute(opr);
        }

        public string GetWriteLockOwner(string key)
        {
            var partitionMatchFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, key);
            var rowKeyFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, writeLockKey);
            var filter = TableQuery.CombineFilters(partitionMatchFilter, TableOperators.And, rowKeyFilter);
            var q = new TableQuery<KeyEntity>().Where(filter);

            var entities = table.ExecuteQuery<KeyEntity>(q);
            if(entities.Count() == 0)
            {
                return null;
            }

            return entities.First().CreatorTransaction;
        }
        public void ReleaseWriteLock(string key, string transactionId)
        {
            var writeLockEntity = new WriteLockEntity(key, transactionId);
            writeLockEntity.ETag = "*";
            table.Execute(TableOperation.Delete(writeLockEntity));
        }

        public void AddSiLock(string key, string transactionId)
        {
            // should have si.owner field
            var sireaderLockEntity = new SiReadLockEntity(key, transactionId);
            TableOperation opr = TableOperation.Insert(sireaderLockEntity);

            table.Execute(opr);
        }

        public void ReleaseSiLock(string key, string transactionId)
        {
            var siReadLockEntity = new SiReadLockEntity(key, transactionId);
            siReadLockEntity.ETag = "*";
            table.Execute(TableOperation.Delete(siReadLockEntity));
        }

        public IEnumerable<string> GetTransactionsHoldingSiReadLocks(string key)
        {
            var partitionMatchFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, key);
            var rowKeyFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, siReadLockPrefix);
            var rowKeyFilterEnd = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, siReadLockPrefixEnd);
            var filter = TableQuery.CombineFilters(partitionMatchFilter, TableOperators.And, rowKeyFilter);
            filter = TableQuery.CombineFilters(filter, TableOperators.And, rowKeyFilterEnd);
            var q = new TableQuery<SiReadLockEntity>().Where(filter);

            var entities = table.ExecuteQuery<SiReadLockEntity>(q);
            var x = entities.Count();
            foreach(var entity in entities)
            {
                yield return entity.TransactionId;
            }
        }

        private ulong SetKeyWithVersionToValue(string key, string value, ulong version, string transactionId)
        {
            return SetKeyWithVersionToValue(new KeyEntity(key, version, value) { CreatorTransaction = transactionId });
        }

        private ulong SetKeyWithVersionToValue(KeyEntity keyValue)
        {
            TableOperation opr = TableOperation.Insert(keyValue);

            var tableResult = table.Execute(opr);
            return (tableResult.Result as KeyEntity).Version;
        }

        private IEnumerable<KeyEntity> GetKeyVersionsGreaterThanOrEqual(string key, ulong minVersion)
        {
            var partitionMatchFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, key);
            var rowKeyFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, keyPrefix + KeyEntity.FormatVersionToString(minVersion));
            var rowKeyFilterEnd = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, keyPrefixEnd);
            var filter = TableQuery.CombineFilters(partitionMatchFilter, TableOperators.And, rowKeyFilter);
            filter = TableQuery.CombineFilters(filter, TableOperators.And, rowKeyFilterEnd);
            var q = new TableQuery<KeyEntity>().Where(filter);

            var entities = table.ExecuteQuery<KeyEntity>(q);
            return entities;
        }
    }
}
