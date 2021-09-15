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
        public decimal Open { get; init; }
        public decimal High { get; init; }
        public decimal Low { get; init; }
        public decimal Close { get; init; }
        public decimal Volume { get; init; }
        public DateTime Date { get; init; }
        public TimeSpan OpenTime { get; init; }

        public decimal Splits { get; init; }
        public decimal Extra2 { get; init; }
        public decimal Dividends { get; init; }

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
                    $"{Splits}",
                    $"{Extra2}",
                    $"{Dividends}"
                });
        }
    }

    /// <summary>
    /// Converter class containing the logic for converting 1-minute charts to NN minutes timeframes.
    /// </summary>
    class Converter
    {

        // relative small number of candles/bars with timeframe > 1 minute (the resizing of when append whould not be an issue).
        readonly List<OHLC> chart = new();

        readonly int _timeframe;
        readonly string _sourcePath;
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


        public Converter(int timeFrame, string sourcePath, TimeSpan sessionStart, TimeSpan sessionEnd)
        {
            _timeframe = timeFrame;
            _sourcePath = sourcePath;
            _sessionStart = sessionStart;
            _sessionEnd = sessionEnd;
        }

        public async Task Start()
        {
            // config no headers (this can also be added in the constructor)
            var options = new CsvDataReaderOptions { HasHeaders = false };

            // Sylvan.Data CSV reader is an extremely fast reader using the SIMD approach.
            using var csv = await CsvDataReader.CreateAsync(_sourcePath, options);

            while (await csv.ReadAsync())
            {
                Convert(readBar());
            }

            // commit the last in-progress bar
            Flush();

            OHLC readBar()
            {
                var date = dateTimeParser(csv.GetString(0));
                var time = timeParser(csv.GetString(1));
                var o = csv.GetDecimal(2);
                var h = csv.GetDecimal(3);
                var l = csv.GetDecimal(4);
                var c = csv.GetDecimal(5);


                // Some volume values are represented by scientific notation in the source files,
                // using NumberStyles.Float fixes the parsing issue.
                // Also, there are volume values that are represented as floating points so I decided to go
                // with deciamal (integer is not a valid option anyway) instead of float to avoid precision related issues.
                var vol = Decimal.Parse(csv.GetString(6), NumberStyles.Float);

                var extra1 = csv.GetDecimal(7);
                var extra2 = csv.GetDecimal(8);
                var extra3 = csv.GetDecimal(9);

                return new()
                {
                    Date = date,
                    OpenTime = time,
                    Open = o,
                    High = h,
                    Low = l,
                    Close = c,
                    Volume = vol,
                    Splits = extra1,
                    Extra2 = extra2,
                    Dividends = extra3
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
                }

                _volume = 0;
                _high = Decimal.MinValue;
                _low = Decimal.MaxValue;
                _openTime = new TimeSpan(m1Bar.OpenTime.Hours, minutesOffset - minutesOffset % _timeframe, 0);
                _openDate = m1Bar.Date;
                _open = m1Bar.Open;

                _extra1 = m1Bar.Splits;
                _extra2 = m1Bar.Extra2;
                _extra3 = m1Bar.Dividends;

                init = true;
            }

            _high = Math.Max(m1Bar.High, _high);
            _low = Math.Min(m1Bar.Low, _low);
            _close = m1Bar.Close;
            _volume += m1Bar.Volume;
            _extra1 = Math.Max(m1Bar.Splits, _extra1);
            _extra2 = Math.Max(m1Bar.Extra2, _extra2);
            _extra3 = Math.Max(m1Bar.Dividends, _extra3);


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
                Splits = _extra1,
                Extra2 = _extra2,
                Dividends = _extra3
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
            await File.WriteAllLinesAsync(path, chart.Select(x => x.CSVRow()));
        }

        public List<OHLC> Chart
        {
            get => chart;
        }

    }
}
