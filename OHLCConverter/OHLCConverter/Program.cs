using System;
using System.Globalization;
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
                var date = dateTimeParser(csv.GetString(0));
                var time = timeParser(csv.GetString(1));
                var o = csv.GetDecimal(2);
                var h = csv.GetDecimal(3);
                var l = csv.GetDecimal(4);
                var c = csv.GetDecimal(5);
                
                // some volume values are represented by scientific notation in the source files
                // using NumberStyles.Float fixes the parsing issue
                // also, there are volume values that are represented as floating points so I decided to go
                // with deciamal (integer is not a valid option anyway) instead of float to avoid precision related issues
                var v = Decimal.Parse(csv.GetString(6), NumberStyles.Float);


                count++;
            }

            Console.WriteLine("Count is: {0}", count);


            // helper local functions for custom dataformat and time parsing
            // just for readability purpose             
            DateTime dateTimeParser(string input)
            {                
                return DateTime.ParseExact(input, "yyyyMMdd", CultureInfo.InvariantCulture);
            }

            TimeSpan timeParser(string input)
            {
                return TimeSpan.ParseExact(input.PadLeft(4, '0'), "hhmm", CultureInfo.InvariantCulture);
            }
        }
    }

        
}
