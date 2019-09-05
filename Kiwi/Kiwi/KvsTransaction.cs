
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Kiwi
{
    public class KvsTransaction
    {
        private KeyValueStore kvs;
        private Guid transactionId;
        private ConcurrentDictionary<string, ulong> keyVersions;
        private ConcurrentDictionary<string, bool> writtenKeys;
        private CloudBlobContainer container;

        public string TransactionId => transactionId.ToString();
        public enum TransactionStatus
        {
            Running,
            CompletedWithFailure,
            CompletedWithSuccess
        }

        public class TransactionDescriptor
        {
            public TransactionStatus Status { get; set; }
            public bool InConflict { get; set; } = false;
            public bool OutConflict { get; set; } = false;
        }



        public KvsTransaction()
        {
            this.transactionId = Guid.NewGuid();
            string storageConnectionString = "<storage connection string>"
            CloudStorageAccount account = CloudStorageAccount.Parse(storageConnectionString);
            var blobClient = account.CreateCloudBlobClient();

            this.container = blobClient.GetContainerReference("kiwi");
            container.CreateIfNotExists();
            CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(TransactionId);
            cloudBlockBlob.UploadText("marker");
            SetTransactionDescriptor(cloudBlockBlob, new TransactionDescriptor());
            cloudBlockBlob.SetMetadata();

            this.kvs = new KeyValueStore();
            keyVersions = new ConcurrentDictionary<string, ulong>();
            writtenKeys = new ConcurrentDictionary<string, bool>();
        }

        public string Read(string key)
        {
            kvs.AddSiLock(key, transactionId.ToString());
            string writeLockOwner = kvs.GetWriteLockOwner(key);
            if(writeLockOwner != null)
            {
                SetInconflictWith(writeLockOwner);
                SetOutconflictWith(transactionId.ToString());
            }

            var value = kvs.Read(key, out ulong version);
            keyVersions.AddOrUpdate(key, version, (string k, ulong v) => version);

            var newlyCommitedTransactions = kvs.ReadAllTransactionsGreaterThanVersion(key, version);

            foreach(var newlyCommitedTransaction in newlyCommitedTransactions)
            {
                AcquireAndExecuteTransaction(newlyCommitedTransaction, (TransactionDescriptor newerTransactionDescriptor) =>
                {
                    AcquireAndExecuteTransaction(TransactionId, (TransactionDescriptor currentTransactionDescription) =>
                    {
                        if (newerTransactionDescriptor.Status == TransactionStatus.CompletedWithSuccess && newerTransactionDescriptor.OutConflict)
                        {
                            currentTransactionDescription.InConflict = true;
                            currentTransactionDescription.OutConflict = true;
                        }

                        newerTransactionDescriptor.InConflict = true;
                        currentTransactionDescription.OutConflict = true;
                    });
                });
            }

            return value;
        }

        public void Write(string key, string value)
        {
            writtenKeys.AddOrUpdate(key, true, (k, v) => true);
            kvs.AcquireWriteLock(key, TransactionId);
            foreach(var siReadTransaction in kvs.GetTransactionsHoldingSiReadLocks(key))
            {
                if(siReadTransaction == TransactionId)
                {
                    continue;
                }

                AcquireAndExecureTransactionBlob(siReadTransaction, siTransactionBlob =>
                {
                    var siTransactionDescriptor = GetTransactionDescriptor(siTransactionBlob, null);
                    var siTransactionIsRunning = (siTransactionDescriptor.Status == TransactionStatus.Running);
                    TransactionDescriptor currentTransactionDescriptor = null;

                    AcquireAndExecureTransactionBlob(TransactionId, currentTransactionBlob =>
                    {
                        currentTransactionDescriptor = GetTransactionDescriptor(currentTransactionBlob, null);
                        if (siTransactionIsRunning ||
                           siTransactionBlob.Properties.LastModified > currentTransactionBlob.Properties.Created)
                        {
                            if(siTransactionDescriptor.Status == TransactionStatus.CompletedWithSuccess &&
                                siTransactionDescriptor.InConflict)
                            {
                                currentTransactionDescriptor.InConflict = true;
                                currentTransactionDescriptor.OutConflict = true;
                                SetTransactionDescriptor(currentTransactionBlob, currentTransactionDescriptor);
                            }
                            else
                            {
                                siTransactionDescriptor.OutConflict = true;
                                currentTransactionDescriptor.InConflict = true;
                                SetTransactionDescriptor(currentTransactionBlob, currentTransactionDescriptor);
                                SetTransactionDescriptor(siTransactionBlob, siTransactionDescriptor);
                            }
                        }
                    });
                });
            }

            ulong version = 0;
            ulong newVersion = 0;
            if(keyVersions.TryGetValue(key, out version))
            {
                newVersion = kvs.Update(key, value, version, TransactionId);
            }
            else
            {
                newVersion = kvs.Create(key, value, TransactionId);
            }

            keyVersions.AddOrUpdate(key, newVersion, (k, v) => newVersion);
        }

        public TransactionStatus Commit()
        {
            var status = TransactionStatus.Running;
            AcquireAndExecuteTransaction(TransactionId, td =>
            {
                if(td.InConflict && td.OutConflict)
                {
                    td.Status = TransactionStatus.CompletedWithFailure;
                }
                else
                {
                    td.Status = TransactionStatus.CompletedWithSuccess;
                }

                status = td.Status;
            });

            foreach(var key in writtenKeys.Keys)
            {
                kvs.ReleaseWriteLock(key, TransactionId);
            }

            return status;
        }

        private void SetInconflictWith(string inconflictTransaction)
        {
            AcquireAndExecuteTransaction(inconflictTransaction, (TransactionDescriptor transactionDescriptor) => transactionDescriptor.InConflict = true);
        }

        private void SetOutconflictWith(string outconflictTransaction)
        {
            AcquireAndExecuteTransaction(outconflictTransaction, (TransactionDescriptor transactionDescriptor) => transactionDescriptor.OutConflict = true);
        }

        private void AcquireAndExecuteTransaction(string transactionId, Action<TransactionDescriptor> action)
        {
            var transactionBlob = container.GetBlockBlobReference(transactionId);
            string lease = transactionBlob.AcquireLease(TimeSpan.FromSeconds(60), Guid.NewGuid().ToString());
            var transactionDescriptor = GetTransactionDescriptor(transactionBlob, new TransactionDescriptor());

            action.Invoke(transactionDescriptor);

            SetTransactionDescriptor(transactionBlob, transactionDescriptor);
            var accessCondition = AccessCondition.GenerateLeaseCondition(lease);
            transactionBlob.SetMetadata(accessCondition);
            transactionBlob.ReleaseLease(accessCondition);
        }

        private void AcquireAndExecureTransactionBlob(string transactionId, Action<CloudBlockBlob> action)
        {
            var transactionBlob = container.GetBlockBlobReference(transactionId);
            string lease = transactionBlob.AcquireLease(TimeSpan.FromSeconds(60), Guid.NewGuid().ToString());

            action.Invoke(transactionBlob);

            var accessCondition = AccessCondition.GenerateLeaseCondition(lease);
            transactionBlob.SetMetadata(accessCondition);
            transactionBlob.ReleaseLease(accessCondition);
        }

        private void Trial()
        {
            
        }

        public string Trial2()
        {
            var transactionBlob = container.GetBlobReference("040362fe-7b2e-4a12-ae39-87ec3abaf205");
            string lease = transactionBlob.AcquireLease(TimeSpan.FromSeconds(60), Guid.NewGuid().ToString());


            var stream = new MemoryStream();
            var cloudBlob = container.GetBlobReference("040362fe-7b2e-4a12-ae39-87ec3abaf205");
            MemoryStream memoryStream = new MemoryStream();
            cloudBlob.DownloadToStream(memoryStream);
            memoryStream.Position = 0;
            StreamReader streamReader = new StreamReader(memoryStream);
            String blobText = streamReader.ReadToEnd();

            return blobText;
        }

        public void SetTransactionDescriptor(CloudBlockBlob transactionBlob, TransactionDescriptor transactionDescriptor)
        {
            //transactionBlob.UploadText(JsonConvert.SerializeObject(transactionDescriptor));
            transactionBlob.Metadata["TransactionDescriptor"] = JsonConvert.SerializeObject(transactionDescriptor);
        }

        public TransactionDescriptor GetTransactionDescriptor(CloudBlockBlob transactionBlob, TransactionDescriptor defaultDescriptor)
        {
            transactionBlob.FetchAttributes();
            if(transactionBlob.Metadata.ContainsKey("TransactionDescriptor"))
            {
                return JsonConvert.DeserializeObject<TransactionDescriptor>(transactionBlob.Metadata["TransactionDescriptor"]);
            }

            return defaultDescriptor;
        }
    }
}
