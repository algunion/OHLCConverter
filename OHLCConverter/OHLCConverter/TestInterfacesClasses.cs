using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace OHLCConverter
{
    class TradingDateService : ITradingDateService
    {
        public IReadOnlyList<DateTime> GetTradingDates(DateTime startDate, DateTime endDate)
        {
            return Enumerable.Range(0, 1 + endDate.Subtract(startDate).Days)
                .Select(offset => startDate.AddDays(offset))
                .ToArray();
        }
    }

    class ServerEntityBatchSaver : IServerEntityBatchSaver
    {
        readonly List<HistoricEodDataServerEntity> eodDatas = new();
        readonly List<HistoricSplitServerEntity> splitDatas = new();
        readonly List<HistoricDividendServerEntity> dividDatas = new();
        
        public void BatchSave(HistoricEodDataServerEntity entity)
        {
            eodDatas.Add(entity);
        }

        public void BatchSave(HistoricSplitServerEntity entity)
        {
            splitDatas.Add(entity);
        }

        public void BatchSave(HistoricDividendServerEntity entity)
        {
            dividDatas.Add(entity);
        }

        /// <summary>
        /// Testing purposes only.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="sep"></param>
        /// <returns></returns>
        public async Task DropToCsv(string path, string sep = ",")
        {
            var lines = new List<string>();

            var splitDict = splitDatas.ToDictionary(x => x.ExDividendDate, x => x);
            var dividendsDict = dividDatas.ToDictionary(x => x.ExDividendDate, x => x);

            foreach (var ohlc in eodDatas)
            {
                var row = CSVRow(ohlc);
                var splitStr = "NA";
                var divStr = "NA";
                
                if (splitDict.ContainsKey(ohlc.TradingDate))
                {
                    var sp = splitDict[ohlc.TradingDate];
                    splitStr = $"{sp.OldShares}/{sp.NewShares}";
                }

                if (dividendsDict.ContainsKey(ohlc.TradingDate))
                {
                    var d = dividendsDict[ohlc.TradingDate];
                    divStr = $"{d.Dividend}";
                }

                row = $"{row}{sep}{splitStr}{sep}{divStr}";

                lines.Add(row);
            }

            await File.WriteAllLinesAsync(path, lines);
        }

        private string CSVRow(HistoricEodDataServerEntity entity, string sep = ",")
        {
            return
                String.Join(sep, new string[] {
                    entity.TradingDate.ToString("yyyy-MM-dd"),
                    entity.TradingDate.TimeOfDay.ToString("hmm"),
                    $"{entity.Open}",
                    $"{entity.High}",
                    $"{entity.Low}",
                    $"{entity.Close}",
                    $"{entity.VolumeInHundreds}"
                });
        }
    }
}
