using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OHLCConverter
{
    interface ITradingDateService
    {
        IReadOnlyList<DateTime> GetTradingDates(DateTime startDate, DateTime endDate);
    }
}
