using Kiwi;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace KiwiTests
{
    [TestClass]
    public class KeyValueStoreTests
    {
        private string Key = "key" + (new Random()).Next();
        private string DefaultTransaction = Guid.NewGuid().ToString();

        private string RandomValue
        {
            get
            {
                return (new Random()).Next() + string.Empty;
            }
        }

        private KeyValueStore Kvs = new KeyValueStore();

        [TestMethod]
        public void ShouldBeAbleToSave()
        {
            var originalValue = RandomValue;
            var originalVersion = Kvs.Overwrite(Key, originalValue, DefaultTransaction);
            Assert.AreEqual(Kvs.Read(Key), originalValue);

            var newValue = RandomValue;
            var newVersion = Kvs.Update(Key, newValue, originalVersion, DefaultTransaction);
            Assert.AreEqual(Kvs.Read(Key, out ulong version), newValue);
            Assert.AreEqual(version, newVersion);
        }

        [TestMethod]
        public void ShouldBeAbleReadAllVersionsLaterThanGivenVersion()
        {
            var firstValue = RandomValue;
            var firstVersion = Kvs.Overwrite(Key, firstValue, DefaultTransaction);
            var secondValue = RandomValue;
            var secondVersion = Kvs.Overwrite(Key, secondValue, DefaultTransaction);

            var transactions = Kvs.ReadAllTransactionsGreaterThanVersion(Key, firstVersion);
            Assert.AreEqual(transactions.Count(), 1);
            Assert.AreEqual(transactions.First(), DefaultTransaction);

            transactions = Kvs.ReadAllTransactionsGreaterThanVersion(Key, secondVersion);
            Assert.AreEqual(transactions.Count(), 0);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ShouldNotSetValueIfOlderVersion()
        {
            var OlderVersion = Kvs.Overwrite(Key, RandomValue, DefaultTransaction);

            var latestValue = RandomValue;
            var currentVersion = Kvs.Update(Key, latestValue, OlderVersion, DefaultTransaction);

            Kvs.Update(Key, RandomValue, OlderVersion, DefaultTransaction);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ShouldThrowExceptionIfNotFound()
        {
            Kvs.Read("somestringwhichdoesnotexist");
        }

        [TestMethod]
        public void ShouldBeAbleToDelete()
        {
            var version = Kvs.Overwrite(Key, RandomValue, DefaultTransaction);
            var newDeletedVersion = Kvs.Delete(Key, version, DefaultTransaction);

            var isExceptionRaised = false;
            try
            {
                Kvs.Read(Key);
            }
            catch(ArgumentException)
            {
                isExceptionRaised = true;
            }

            Assert.AreEqual(isExceptionRaised, true);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void DeleteOnNonExistingKeyShouldFail()
        {
            Kvs.Delete("somestringwhichdoesnotexist", 100, DefaultTransaction);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void DeleteOnDeletedKeyShouldFail()
        {
            var version = Kvs.Overwrite(Key, RandomValue, DefaultTransaction);
            var newDeletedVersion = Kvs.Delete(Key, version, DefaultTransaction);

            Kvs.Delete(Key, newDeletedVersion, DefaultTransaction);
        }

        [TestMethod]
        public void SetOnDeletedKeyShouldNotFail()
        {
            var value = RandomValue;
            var version = Kvs.Overwrite(Key, RandomValue, DefaultTransaction);
            var newDeletedVersion = Kvs.Delete(Key, version, DefaultTransaction);

            var newVersion = Kvs.Create(Key, value, DefaultTransaction);

            Assert.AreEqual(Kvs.Read(Key), value);
        }

        [TestMethod]
        public void ShouldBeAbleToAcquireAndReleaseWriteLock()
        {
            string txId = Guid.NewGuid().ToString();
            Kvs.AcquireWriteLock(Key, txId);

            var isExceptionRaised = false;
            try
            {
                Kvs.AcquireWriteLock(Key, txId);
            }
            catch(StorageException)
            {
                isExceptionRaised = true;
            }

            Assert.IsTrue(isExceptionRaised);

            Kvs.ReleaseWriteLock(Key, txId);

            // Assert that WriteLock was released;
            Kvs.AcquireWriteLock(Key, txId);
        }

        [TestMethod]
        public void ShouldBeAbleToAddAndReleaseSireadLock()
        {
            string txId = Guid.NewGuid().ToString();
            Kvs.AddSiLock(Key, txId);

            var isExceptionRaised = false;
            try
            {
                Kvs.AddSiLock(Key, txId);
            }
            catch(StorageException)
            {
                isExceptionRaised = true;
            }

            Assert.IsTrue(isExceptionRaised);

            var siLocks = Kvs.GetTransactionsHoldingSiReadLocks(Key);
            Assert.AreEqual(siLocks.First(), txId);
        }
    }
}
