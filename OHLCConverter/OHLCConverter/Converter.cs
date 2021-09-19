using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Sylvan.Data.Csv;

namespace OHLCConverter
{
    public record OHLC()
    {
        public double Open { get; init; }
        public double High { get; init; }
        public double Low { get; init; }
        public double Close { get; init; }
        public decimal Volume { get; init; }
        public DateTime Date { get; init; }
        public TimeSpan OpenTime { get; init; }

        public decimal SplitRatio { get; init; }
        public decimal Extra2 { get; init; }
        public double Dividend { get; init; }

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
                    $"{SplitRatio}",
                    $"{Extra2}",
                    $"{Dividend}"
                });
        }
    }

    /// <summary>
    /// Converter class containing the logic for converting 1-minute charts to NN minutes timeframes.
    /// </summary>
    class Converter
    {
        List<OHLC> _chart;
        
        readonly int _instrumentId;
        readonly int _timeframe;
        readonly string _sourcePath;
        readonly TimeSpan _sessionStart;
        readonly TimeSpan _sessionEnd;

        DateTime _openDate;
        TimeSpan _openTime;
        double _open;
        double _high = Double.MinValue;
        double _low = Double.MaxValue;
        double _close;
        decimal _volume = 0;
        bool init = false;
        decimal _splitRatio;
        decimal _extra2;
        double _dividend;
        bool _split = false;
        decimal _prevSplitRatio;

        readonly bool _saveChart;
        readonly DataManager _dataManager;
        readonly IServerEntityBatchSaver _batchSaver;


        public Converter(
            int instrumentId, 
            int timeFrame, 
            string sourcePath, 
            TimeSpan sessionStart, 
            TimeSpan sessionEnd, 
            DataManager dataManager, 
            IServerEntityBatchSaver batchSaver, 
            bool localChart = false)
        {
            _instrumentId = instrumentId;
            _timeframe = timeFrame;
            _sourcePath = sourcePath;
            _sessionStart = sessionStart;
            _sessionEnd = sessionEnd;
            _dataManager = dataManager;
            _batchSaver = batchSaver;

            if (localChart)
            {
                _chart = new List<OHLC>();
                _saveChart = true;
            }
        }

        public async Task BatchSave()
        {
            // config no headers (this can also be added in the constructor)
            var options = new CsvDataReaderOptions { HasHeaders = false };

            // Sylvan.Data CSV reader is an extremely fast reader using the SIMD approach.
            using var csv = await CsvDataReader.CreateAsync(_sourcePath, options);

            while (await csv.ReadAsync())
            {
                var bar1M = readBar();
                
                // Depending on the timeframe and mapping from
                // bars to eod, we might need to skip some data
                // (see also your description on this).
                if (bar1M.Date >= _dataManager.DateLimit)
                    Convert(bar1M);
            }

            // commit the last in-progress bar
            Flush();

            OHLC readBar()
            {
                var date = dateTimeParser(csv.GetString(0));
                var time = timeParser(csv.GetString(1));
                var o = csv.GetDouble(2);
                var h = csv.GetDouble(3);
                var l = csv.GetDouble(4);
                var c = csv.GetDouble(5);

                // Some volume values are represented by scientific notation in the source files,
                // using NumberStyles.Float fixes the parsing issue.
                // Also, there are volume values that are represented as floating points so I decided to go
                // with deciamal (integer is not a valid option anyway) instead of float to avoid precision related issues.
                var vol = Decimal.Parse(csv.GetString(6), NumberStyles.Float, CultureInfo.InvariantCulture);  

                var splitRatio = csv.GetDecimal(7);
                var extra2 = csv.GetDecimal(8);
                var dividend = csv.GetDouble(9);

                return new()
                {
                    Date = date,
                    OpenTime = time,
                    Open = o,
                    High = h,
                    Low = l,
                    Close = c,
                    Volume = vol,
                    SplitRatio = splitRatio,
                    Extra2 = extra2,
                    Dividend = dividend
                };
            }

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

        /// <summary>
        /// Receives a stream of 1-minute OHLC bars - to avoid loading the entire datasets in memory.
        /// This method contains the main logic for NN timeframe bars generation from 1-minute bars.
        /// </summary>
        /// <param name="m1Bar"></param>
        private void Convert(OHLC m1Bar)
        {
            if (!inSession(m1Bar))
                return;

            var minutesOffset = m1Bar.OpenTime.Minutes;

            if (isNewBar(m1Bar))
            {
                if (init)
                {
                    AddCurrentBar();
                    if (m1Bar.SplitRatio != _splitRatio)
                    {
                        _split = true;
                        _prevSplitRatio = _splitRatio;
                    }                  
                }

                _volume = 0;
                _high = Double.MinValue;
                _low = Double.MaxValue;
                _openTime = new TimeSpan(m1Bar.OpenTime.Hours, minutesOffset - minutesOffset % _timeframe, 0);
                _openDate = m1Bar.Date;
                _open = m1Bar.Open;

                _splitRatio = m1Bar.SplitRatio;
                _extra2 = m1Bar.Extra2;
                _dividend = m1Bar.Dividend;

                init = true;
            }

            _high = Math.Max(m1Bar.High, _high);
            _low = Math.Min(m1Bar.Low, _low);
            _close = m1Bar.Close;
            _volume += m1Bar.Volume;
            _splitRatio = m1Bar.SplitRatio;
            _extra2 = Math.Max(m1Bar.Extra2, _extra2);
            _dividend = Math.Max(m1Bar.Dividend, _dividend);


            bool isNewBar(OHLC m1Bar)
            {
                return
                    m1Bar.OpenTime.Subtract(_openTime).TotalMinutes > _timeframe
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
                }
                else if (_sessionStart > _sessionEnd)
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
                SplitRatio = _splitRatio,
                Extra2 = _extra2,
                Dividend = _dividend
            };

            _batchSaver.BatchSave(_dataManager.HistoricEODFromOHLC(_instrumentId, bar));
            
            if (_split)
            {
                _batchSaver.BatchSave(_dataManager.HistoricSplitFromOHLC(_instrumentId, bar, _prevSplitRatio));
                _split = false;
            }

            if (bar.Dividend != 0)
            {
                _batchSaver.BatchSave(_dataManager.HistoricDividendFromOHLC(_instrumentId, bar));
            }

            if (_saveChart)
                _chart.Add(bar);
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
            if (_saveChart)
            {
                await File.WriteAllLinesAsync(path, _chart.Select(x => x.CSVRow()));
            }
            else
            {
                Console.WriteLine("To save chart as CSV please set _saveChart to true.");
            }
        }

    }
}
