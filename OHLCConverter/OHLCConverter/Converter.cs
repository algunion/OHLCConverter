using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OHLCConverter
{
    public record OHLC()
    {
        public decimal Open { get; init; }
        public decimal High { get; init; }
        public decimal Low { get; init; }
        public decimal Close { get; init; }
        public decimal Volume { get; init; }
        public DateTime Date { get; init; }
        public TimeSpan OpenTime { get; init; }

        public string CSVRow(string separator = ",")
        {
            return
                String.Join(separator, new string[] {
                    this.Date.ToString("yyyyMMdd"),
                    this.OpenTime.ToString("hmm"),
                    this.Open.ToString(),
                    this.High.ToString(),
                    this.Low.ToString(),
                    this.Close.ToString(),
                    this.Volume.ToString()
                });
        }
    }

    class Converter
    {
        // relative small number of candles/bars with timeframe > 1 minute (the resizing of when append whould not be an issue).
        List<OHLC> chart = new();
        
        public readonly TimeSpan timeframe;
        public readonly TimeSpan sessionStart;
        public readonly TimeSpan sessionEnd;

        DateTime date;
        TimeSpan openTime;
        decimal tOpen;
        decimal tHigh;
        decimal tLow;
        decimal tClose;
        bool startNewBar = true;

        
        
        
        public Converter(TimeSpan timeframe, TimeSpan sessionStart, TimeSpan sessionEnd)
        {
            this.timeframe = timeframe;
            this.sessionStart = sessionStart;
            this.sessionEnd = sessionEnd;
        }

        public void Convert(decimal m1Open, decimal m1High, decimal m1Low, decimal m1Close, DateTime m1Date, TimeSpan m1Time)
        {
            if (startNewBar)
            {
                this.openTime = m1Time;

            }
        }





    }
}
