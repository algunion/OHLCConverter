using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MinuteUp;

namespace TestProj
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            var trserv = new TradingDateService();


            var sourceDir = @"D:\order_157717.tar\order_157717";
            var targetDir = @"D:\test";



            var dm = new DataManager(30, new TimeSpan(9, 30, 0), new TimeSpan(16, 0, 0), trserv, DateTime.Now.Subtract(TimeSpan.FromDays(1)));
            if (sourceDir is not null && targetDir is not null)
            {
                if (!Directory.Exists(sourceDir))
                {
                    Console.WriteLine($"--source-dir value is not referring to an existing directory: {sourceDir}");
                }
                else
                {
                    var csvFiles = Array.FindAll(Directory.GetFiles(sourceDir), path => Path.GetExtension(path) == ".csv");
                    List<double> elapsed = new List<double>();
                    foreach (var f in csvFiles)
                    {
                        
                        DateTime now = DateTime.Now;
                        Console.WriteLine($"Processing {f}");
                        
                        var bs = new ServerEntityBatchSaver();
                        await dm.CreatePseudoEODDataFromMinuteBarCsvAsync(f, 12, bs);
                        await bs.DropToCsv(Path.Combine(targetDir, Path.GetFileName(f)));
                        var end = DateTime.Now.Subtract(now).TotalSeconds;
                        Console.WriteLine($"Elapsed: {end}");
                        elapsed.Add(end);
                        double avg = Queryable.Average(elapsed.AsQueryable());
                        Console.WriteLine($"mean: {avg}");
                        Console.WriteLine("-----------------");
                    }
                }
            }
            
            return 0;

            


        }
    }
}
