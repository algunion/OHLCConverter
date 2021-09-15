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
            // returning descending
            return Enumerable.Range(0, 1 + endDate.Subtract(startDate).Days)
                .Select(offset => startDate.AddDays(offset)).Reverse().ToArray();
        }
    }

    class ServerEntityBatchSaver : IServerEntityBatchSaver
    {
        List<HistoricEodDataServerEntity> eodDatas = new();
        List<HistoricSplitServerEntity> splitDatas = new();
        List<HistoricDividendServerEntity> dividDatas = new();
        
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

        public void DropToCsv(string path)
        {
            var lines = new List<string>();

            foreach (var i in Enumerable.Range(0, eodDatas.Count))
            {
                lines.Add(CSVRow(eodDatas[i], splitDatas[i], dividDatas[i]));
            }

            File.WriteAllLines(path, lines);
        }

        private string CSVRow(HistoricEodDataServerEntity entity, HistoricSplitServerEntity split, HistoricDividendServerEntity div, string separator = ",")
        {
            return
                String.Join(separator, new string[] {
                    entity.TradingDate.ToString("yyyy-MM-dd"),
                    entity.TradingDate.TimeOfDay.ToString("hmm"),
                    $"{entity.Open}",
                    $"{entity.High}",
                    $"{entity.Low}",
                    $"{entity.Close}",
                    $"{entity.VolumeInHundreds}",
                    $"{split.NewShares}",                    
                    $"{div.Dividend}"
                });
        }
    }
}
