using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KiwiConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var taskList = new List<Thread>();
            var timer = new Stopwatch();
            JustDoIt();
            timer.Start();
            for (int i = 0; i < 1000; i++)
            {
                var t = new Thread(() => JustDoIt(5));
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
            for (int i = 0; i < x; i++)
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
    }
    }
}
