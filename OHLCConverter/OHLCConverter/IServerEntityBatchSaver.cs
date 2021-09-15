using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OHLCConverter
{
    interface IServerEntityBatchSaver
    {
        void BatchSave(HistoricEodDataServerEntity entity);
        void BatchSave(HistoricSplitServerEntity entity);
        void BatchSave(HistoricDividendServerEntity entity);
    }

    public class HistoricEodDataServerEntity
    {
        public int InstrumentId { get; set; }
        public DateTime TradingDate { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public long VolumeInHundreds { get; set; } // Note that we currently ignore the last two digits of the volume.
    }

    public class HistoricSplitServerEntity
    {
        public int InstrumentId { get; set; }
        public DateTime ExDividendDate { get; set; }
        public int OldShares { get; set; }
        public int NewShares { get; set; }
    }

    public class HistoricDividendServerEntity
    {
        public int InstrumentId { get; set; }
        public DateTime ExDividendDate { get; set; }
        public double Dividend { get; set; }
    }
}
