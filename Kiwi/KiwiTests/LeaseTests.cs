using Kiwi;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KiwiTests
{
    [TestClass]
    public class LeaseTests
    {
        private string Key = "key" + (new Random()).Next();
        private string Key2 = "key" + (new Random()).Next();

        [TestMethod]
        public void UserShouldNotBeAbleToAcquireLeaseWithoutReleasing()
        {
            var taskList = new List<Thread>();
            var timer = new Stopwatch();
            JustDoIt();
            timer.Start();
            for(int i = 0; i < 1; i++)
            {
                var t = new Thread(() => JustDoIt(1));
                t.Start();
                taskList.Add(t);
                //taskList.Add(Task.Factory.StartNew(JustDoIt));
            }

            taskList.ForEach(t => t.Join());
            var x = timer.ElapsedMilliseconds;
            Console.WriteLine(x);
        }

        private void JustDoIt(int x)
        {
            for(int i = 0; i < x; i++)
            {
                JustDoIt();
            }
        }

        private void JustDoIt()
        {
            string Key = "key" + (new Random()).Next();
            string value = Guid.NewGuid().ToString();
            var transaction = new KvsTransaction();
            //var currentValue = transaction.Read(transactionId);
            transaction.Write(Key, value);
            transaction.Commit();

            //transaction = new KvsTransaction();
            //Assert.AreEqual(transaction.Read(Key), value);
        }

        [TestMethod]
        public void NonSerializableTransactionShouldNotProceed()
        {
            string Key = "key" + (new Random()).Next();
            string Key2 = "key" + (new Random()).Next();

            string value = Guid.NewGuid().ToString();
            var transaction = new KvsTransaction();

            var transaction1 = new KvsTransaction();
            string value1 = Guid.NewGuid().ToString();

            var transaction2 = new KvsTransaction();
            string value2 = Guid.NewGuid().ToString();

            transaction.Write(Key, value);
            transaction.Write(Key2, value);
            transaction.Commit();

            transaction1.Read(Key);
            transaction1.Read(Key2);

            transaction2.Read(Key);
            transaction2.Read(Key2);

            transaction1.Write(Key, value1);
            transaction2.Write(Key2, value2);

            Assert.AreEqual(transaction1.Commit(), KvsTransaction.TransactionStatus.CompletedWithFailure);
            Assert.AreEqual(transaction2.Commit(), KvsTransaction.TransactionStatus.CompletedWithFailure);
        }

        [TestMethod]
        public void SerializableTransactionShouldProceed()
        {
            string value = Guid.NewGuid().ToString();
            var transaction = new KvsTransaction();

            var transaction1 = new KvsTransaction();
            string value1 = Guid.NewGuid().ToString();

            var transaction2 = new KvsTransaction();
            string value2 = Guid.NewGuid().ToString();

            transaction.Write(Key, value);
            transaction.Write(Key2, value);
            transaction.Commit();

            transaction1.Read(Key);
            transaction1.Read(Key2);

            transaction2.Read(Key);
            transaction2.Read(Key2);

            transaction1.Write(Key, value1);
            Assert.AreEqual(transaction1.Commit(), KvsTransaction.TransactionStatus.CompletedWithSuccess);

            transaction2.Write(Key2, value2);
            Assert.AreEqual(transaction2.Commit(), KvsTransaction.TransactionStatus.CompletedWithFailure);
        }

        public void ModifyValues(IDictionary<string, string> transactionValues, string newValue)
        {
            foreach(var transaction in transactionValues.Keys)
            {
                transactionValues[transaction] = newValue;
            }
        }
    }
}
