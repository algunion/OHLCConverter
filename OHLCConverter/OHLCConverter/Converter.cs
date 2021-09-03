using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Sylvan.Data.Csv;

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

        public decimal Extra1 { get; init; }
        public decimal Extra2 { get; init; }
        public decimal Extra3 { get; init; }

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
                    $"{Volume}",
                    $"{Extra1}",
                    $"{Extra2}",
                    $"{Extra3}"
                });
        }
    }

    /// <summary>
    /// Converter class containing the logic for converting 1-minute charts to NN minutes timeframes.
    /// </summary>
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
        decimal _high = Decimal.MinValue;
        decimal _low = Decimal.MaxValue;
        decimal _close;
        decimal _volume = 0;
        bool init = false;
        decimal _extra1;
        decimal _extra2;
        decimal _extra3;
        
        
        public Converter(int nnTarget, TimeSpan sessionStart, TimeSpan sessionEnd)
        {
            _timeFrame = nnTarget;
            _sessionStart = sessionStart;
            _sessionEnd = sessionEnd;            
        }

        /// <summary>
        /// Receives a stream of 1-minute OHLC bars - to avoid loading the entire datasets in memory.
        /// This method contains the main logic for NN timeframe bars generation from 1-minute bars.
        /// </summary>
        /// <param name="m1Bar"></param>
        public void Convert(OHLC m1Bar)
        {
            if (!inSession(m1Bar))
                return;
            
            var minutesOffset = m1Bar.OpenTime.Minutes;
            
            if (isNewBar(m1Bar))
            {
                if (init)
                {
                    AddCurrentBar();
                }

                _volume = 0;
                _high = Decimal.MinValue;
                _low = Decimal.MaxValue;
                _openTime = new TimeSpan(m1Bar.OpenTime.Hours, minutesOffset - minutesOffset % _timeFrame, 0);
                _openDate = m1Bar.Date;
                _open = m1Bar.Open;

                _extra1 = m1Bar.Extra1;
                _extra2 = m1Bar.Extra2;
                _extra3 = m1Bar.Extra3;

                init = true;
            }

            _high = Math.Max(m1Bar.High, _high);
            _low = Math.Min(m1Bar.Low, _low);
            _close = m1Bar.Close;
            _volume += m1Bar.Volume;
            _extra1 = Math.Max(m1Bar.Extra1, _extra1);
            _extra2 = Math.Max(m1Bar.Extra2, _extra2);
            _extra3 = Math.Max(m1Bar.Extra3, _extra3);


            bool isNewBar(OHLC m1Bar)
            {
                return 
                    m1Bar.OpenTime.Subtract(_openTime).TotalMinutes > _timeFrame
                    || m1Bar.Date > _openDate;                  
                
            }            
               
            // Added support for all possible time intervals.
            // For example: session starting at 10pm and session ending at 6am.
            // This means including everything after 6am and before 10pm.               
            bool inSession(OHLC m1Bar)
            {
                if (_sessionStart < _sessionEnd)
                {
                    return m1Bar.OpenTime >= _sessionStart && m1Bar.OpenTime < _sessionEnd;
                } else if (_sessionStart > _sessionEnd)
                {
                    return m1Bar.OpenTime >= _sessionStart || m1Bar.OpenTime < _sessionEnd;
                }
                
                // _sessionStart == sessionEnd - treated as 24h session
                return true;
            }
        }

        /// <summary>
        /// Add wip (and finalized) bar to chart list.
        /// </summary>
        private void AddCurrentBar()
        {
            OHLC bar = new()
            {
                Date = _openDate,
                OpenTime = _openTime,
                Open = _open,
                High = _high,
                Low = _low,
                Close = _close,
                Volume = _volume,
                Extra1 = _extra1,
                Extra2 = _extra2,
                Extra3 = _extra3
            };

            chart.Add(bar);
        }

        
        /// <summary>
        /// Adding the last potentially unfinished/wip bar to chart.
        /// Called internally before writing/exporting the generated data.
        /// </summary>
        private void Flush()
        {
            if (init)
            {
                AddCurrentBar();
                init = false;
            }           
        }

        /// <summary>
        /// Write the generated NN bar to CSV file.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public async Task WriteCSVAsync(string path)
        {
            Flush();
            await File.WriteAllLinesAsync(path, chart.Select(x => x.CSVRow()));                      
        }
    }
}
