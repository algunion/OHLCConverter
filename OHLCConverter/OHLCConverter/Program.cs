using System;
using System.Threading.Tasks;
using Sylvan.Data;

namespace OHLCConverter
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Start Testing:");

            var p = new Program();

            await p.TestCSV(@"D:\order_157717.tar\order_157717\table_mxim.csv");
            Console.WriteLine("Finished");

            //OHLC r = new() { Open = 10m; High = 10 };
            OHLC r = new()
            {
                Open = 10m,
                Close = 10m
            };

            Console.ReadKey();           
            
        }
        // @"D:\order_157717.tar\order_157717\table_mxim.csv"
        public async Task TestCSV(string path)
        {
            int count = 0;
            using var csv = await Sylvan.Data.Csv.CsvDataReader.CreateAsync(path);

            while (await csv.ReadAsync())
            {
                var id = csv.GetString(0);
                var name = csv.GetString(1);
                var o = csv.GetDecimal(2);
                var h = csv.GetDecimal(3);
                var l = csv.GetDecimal(4);
                var c = csv.GetDecimal(5);
                //Console.WriteLine("Count is {0} and volume is {1}", count, csv.GetString(6));
                var v = Decimal.Parse(csv.GetString(6), System.Globalization.NumberStyles.Float);
                count++;
            }

            Console.WriteLine("Count is: {0}", count);

        }
    }

        
}
