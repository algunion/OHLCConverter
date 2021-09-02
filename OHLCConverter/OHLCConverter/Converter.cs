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
                    Date.ToString("yyyyMMdd"),
                    OpenTime.ToString("hmm"),
                    $"{Open}",
                    $"{High}",
                    $"{Low}",
                    $"{Close}",
                    $"{Volume}"                    
                });
        }
    }

    class Converter
    {
        // relative small number of candles/bars with timeframe > 1 minute (the resizing of when append whould not be an issue).
        List<OHLC> chart = new();
        
        readonly int _timeFrame;
        readonly TimeSpan _sessionStart;
        readonly TimeSpan _sessionEnd;

        DateTime _openDate;
        TimeSpan _openTime;
        decimal _open;
        decimal _high;
        decimal _low;
        decimal _close;     
        
        
        public Converter(int nnTarget, TimeSpan sessionStart, TimeSpan sessionEnd)
        {
            _timeFrame = nnTarget;
            _sessionStart = sessionStart;
            _sessionEnd = sessionEnd;            
        }

        public void Convert(decimal m1Open, decimal m1High, decimal m1Low, decimal m1Close, DateTime m1Date, TimeSpan m1Time)
        {
            var minutesOffset = m1Time.Minutes;
            
            if (isNewBar())
            {
                _openTime = new(m1Time.Hours, minutesOffset - minutesOffset % _timeFrame, 0);
                

            }

            bool isNewBar()
            {
                return 
                    m1Time.Subtract(_openTime).TotalMinutes > _timeFrame
                    || m1Date > _openDate;   
                
                
            }
        }





    }
}
