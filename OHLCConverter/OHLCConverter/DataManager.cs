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
        ITradingDateService _tradingDataService;

        // to not abuse the ITradingDateService for each instrument
        readonly Dictionary<DateTime, DateTime> _pseudoEODMap;

        public DataManager(int timeframe, TimeSpan sessionStart, TimeSpan sessionEnd, DateTime anchorDate, ITradingDateService tradingDataService)
        {
            _timeframe = timeframe;
            _sessionStart = sessionStart;
            _sessionEnd = sessionEnd;
            _anchorDate = anchorDate;
            _tradingDataService = tradingDataService;


            // to not abuse the ITradingDateService for each instrument
            _pseudoEODMap = ComputePseudoEODMap();
            
        }

        public async Task CreatePseudoEODDataFromMinuteBarCsvAsync(string csvFile, int instrumentId, IServerEntityBatchSaver batchSaver)
        {
            Converter converter = new(_timeframe, csvFile, _sessionStart, _sessionEnd);

            // I am sticking with converting all file and mapping the dates afterward.
            // Unless we really have a performance problem, ending up with complicated ways like
            // reading the files backwards in order to extract the latest date from the EOF is not worthy
            await converter.Start();

            foreach (var ohlc in converter.Chart)
            {

                batchSaver.BatchSave(historicEODFromOHLC(ohlc));
                batchSaver.BatchSave(historicSplitFromOHLC(ohlc));
                batchSaver.BatchSave(historicDividendFromOHLC(ohlc));
                
            }            

            HistoricEodDataServerEntity historicEODFromOHLC (OHLC ohlc)
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

            HistoricSplitServerEntity historicSplitFromOHLC (OHLC ohlc)
            {
                return new HistoricSplitServerEntity()
                {
                    InstrumentId = instrumentId,
                    ExDividendDate = _pseudoEODMap[ohlc.Date.Add(ohlc.OpenTime)],
                    NewShares = (int)ohlc.Splits, // ??? only one extra field 1 here
                    OldShares = (int)ohlc.Splits // ???  but we have two entries New/Old
                };
            }
             HistoricDividendServerEntity historicDividendFromOHLC (OHLC ohlc)
            {
                return new HistoricDividendServerEntity()
                {
                    InstrumentId = instrumentId,
                    ExDividendDate = _pseudoEODMap[ohlc.Date.Add(ohlc.OpenTime)],
                    Dividend = (int)ohlc.Dividends
                };
            }
        }

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

                if (noWeekend)
                {
                    if (newCursor.DayOfWeek == DayOfWeek.Sunday)
                    {
                        newCursor = newCursor.Subtract(twoDays).Add(lastOpen);
                    }
                    else if (newCursor.DayOfWeek == DayOfWeek.Saturday)
                    {
                        newCursor = newCursor.Subtract(oneDay).Add(lastOpen);
                    }
                }

                return newCursor;
            }

            return workingMap;
        }
        
    }
}
