using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OHLCConverter
{
    class DataManager
    {
        readonly int _timeframe;
        readonly TimeSpan _sessionStart;
        readonly TimeSpan _sessionEnd;
        readonly DateTime _anchorDate;
        readonly ITradingDateService _tradingDataService;

        // to not abuse the ITradingDateService for each instrument
        Dictionary<DateTime, DateTime> _pseudoEODMap = new();
        DateTime _dateLimit;

        public DataManager(
            int timeframe, 
            TimeSpan sessionStart, 
            TimeSpan sessionEnd, 
            DateTime anchorDate, 
            ITradingDateService tradingDataService)
        {
            _timeframe = timeframe;
            _sessionStart = sessionStart;
            _sessionEnd = sessionEnd;
            _anchorDate = anchorDate;
            _tradingDataService = tradingDataService;                       
        }

        public async Task CreatePseudoEODDataFromMinuteBarCsvAsync(string csvFile, int instrumentId, IServerEntityBatchSaver batchSaver)
        {
            if (_pseudoEODMap.Count == 0)
            {
                _pseudoEODMap = ComputePseudoEODMap();
                _dateLimit = _pseudoEODMap.Keys.Min();
            }
            
            Converter converter = new(instrumentId, _timeframe, csvFile, _sessionStart, _sessionEnd, this, batchSaver, localChart: false);
            await converter.BatchSave();
            
            // initial functionality preserved (milestone 1)
            // localChart: true
            //await converter.WriteCSVAsync("test.csv");
        }

        public HistoricEodDataServerEntity HistoricEODFromOHLC(int instrumentId, OHLC ohlc)
        {
            return new HistoricEodDataServerEntity()
            {
                InstrumentId = instrumentId,
                TradingDate = _pseudoEODMap[ohlc.Date.Add(ohlc.OpenTime)],
                Open = (double)ohlc.Open,
                High = (double)ohlc.High,
                Low = (double)ohlc.Low,
                Close = (double)ohlc.Close,
                VolumeInHundreds = (int)(ohlc.Volume / 100)
            };
        }

        public HistoricSplitServerEntity HistoricSplitFromOHLC(int instrumentId, OHLC ohlc, decimal prev)
        {
            var gcd = getGcd(prev, ohlc.SplitRatio);

            return new HistoricSplitServerEntity()
            {
                InstrumentId = instrumentId,
                ExDividendDate = _pseudoEODMap[ohlc.Date.Add(ohlc.OpenTime)],
                NewShares = (int)(prev / gcd),
                OldShares = (int)(ohlc.SplitRatio / gcd)
            };

            decimal getGcd(decimal a, decimal b)
            {
                return (b == 0 ? a : getGcd(b, a % b));
            }

        }
        public HistoricDividendServerEntity HistoricDividendFromOHLC(int instrumentId, OHLC ohlc)
        {
            return new HistoricDividendServerEntity()
            {
                InstrumentId = instrumentId,
                ExDividendDate = _pseudoEODMap[ohlc.Date.Add(ohlc.OpenTime)],
                Dividend = ohlc.Dividend
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="noWeekend"></param>
        /// <returns></returns>
        private Dictionary<DateTime, DateTime> ComputePseudoEODMap(bool noWeekend=true)
        {
            // helpers for readability
            var tframe = TimeSpan.FromMinutes(_timeframe);
            var oneDay = TimeSpan.FromDays(1);
            var twoDays = TimeSpan.FromDays(2);
            var lastOpen = _sessionEnd.Subtract(tframe);

            Dictionary<DateTime, DateTime> workingMap = new Dictionary<DateTime, DateTime>();

            var validDates = _tradingDataService.GetTradingDates(new DateTime(100, 1, 1), _anchorDate).Reverse();
            
            
            // start one day before the actual anchor
            var timeCursor = _anchorDate.Date.Subtract(oneDay).Add(lastOpen);


            // descending from most recent date into past
            foreach (var validDate in validDates)
            {
                workingMap.Add(timeCursor, validDate.Add(_sessionEnd));
                timeCursor = MoveTimeCursor();
            }

            // Managing some edge cases.
            // Avoided to collapse the logical scenarios
            // a little bit verbose but it helps thinking 
            // about various scenarios. 
            // 10pm-6am are legitimate intervals even if
            // there is no overlap with normall trading hours.
            DateTime MoveTimeCursor()
            {
                var newCursor = timeCursor.Subtract(tframe);
                
                if (_sessionStart < _sessionEnd)
                {
                    if (newCursor.TimeOfDay < _sessionStart)
                    {
                        newCursor = newCursor.Date.Subtract(oneDay).Add(lastOpen);
                    } 
                    else if (newCursor.TimeOfDay >= _sessionEnd)
                    {
                        newCursor = newCursor.Date.Add(lastOpen);
                    }
                }
                else if (_sessionStart > _sessionEnd)
                {
                    if (newCursor.TimeOfDay < _sessionStart 
                        && newCursor.TimeOfDay > _sessionEnd)
                    {
                        newCursor = newCursor.Date.Add(lastOpen);
                    }
                }

                // skipping the weekend (non)sessions
                if (noWeekend)
                {
                    if (newCursor.DayOfWeek == DayOfWeek.Sunday)
                    {
                        newCursor = newCursor.Date.Subtract(twoDays).Add(lastOpen);
                    }
                    else if (newCursor.DayOfWeek == DayOfWeek.Saturday)
                    {
                        newCursor = newCursor.Date.Subtract(oneDay).Add(lastOpen);
                    }
                }

                return newCursor;
            }

            return workingMap;
        }

        public DateTime DateLimit 
        {
            get { return _dateLimit; }
        }

        }
}
