using Kiwi;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace KiwiCore
{
    class Program
    {
        static void Main(string[] args)
        {
            var taskList = new List<Thread>();
            var timer = new Stopwatch();
            JustDoIt();
            int exceptions = 0;
            timer.Start();
            for (int i = 0; i < 100; i++)
            {
                try
                {
                    var t = new Thread(() => JustDoIt(100));
                    t.Start();
                    taskList.Add(t);
                }
                catch(Exception)
                {
                    exceptions++;
                    Console.WriteLine(exceptions);
                }
                
                //taskList.Add(Task.Factory.StartNew(JustDoIt));
            }
            
            taskList.ForEach(t => {
                try
                {
                    t.Join();
                }
                catch(Exception e)
                {
                    exceptions++;
                    Console.WriteLine(exceptions);
                }
            });
            var x = timer.ElapsedMilliseconds;
            Console.WriteLine("Complete " + x);
            Console.ReadLine();
        }

        private static void JustDoIt(int x)
        {
            for (int i = 0; i < x; i++)
            {
                JustDoIt();
            }
        }

        private static void JustDoIt()
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
    }
}
