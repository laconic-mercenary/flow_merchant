from ledger import Entry
from merchant_order import Order
from utils import null_or_empty

class BasePerformance:
    def __init__(self):
        self.win_pct = 0.0
        self.total_trades = 0.0
        self.winning_trades = 0.0
        self.total_pnl = 0.0

class BaseAnalytics:
    def __init__(self):
        self.data = {}

    def check_new_data(self, order:Order) -> BasePerformance:
        raise NotImplementedError

    def add(self, ledger_entry:Entry) -> None:
        order = Order.from_dict(ledger_entry.data)
        performance = self.check_new_data(order)
        performance.total_trades += 1.0
        performance.total_pnl += ledger_entry.amount
        if ledger_entry.amount > 0.0:
            performance.winning_trades += 1.0
        performance.win_pct = performance.winning_trades / performance.total_trades

    def results(self) -> list:
        results = list(self.data.values())
        results.sort(key=lambda x: x.win_pct, reverse=True)
        return results

class Analytics:

    class OverallPerformance(BasePerformance):
        def __init__(self):
            super().__init__()

    class Overall(BaseAnalytics):
        def check_new_data(self, order:Order) -> BasePerformance:
            key = "*"
            if key not in self.data:
                self.data[key] = Analytics.OverallPerformance()
            return self.data.get(key)

    class TickerPerformance(BasePerformance):
        def __init__(self, ticker:str):
            if null_or_empty(ticker):
                raise ValueError("ticker cannot be empty")
            self.ticker = ticker
            super().__init__()

    class Tickers(BaseAnalytics):
        def check_new_data(self, order:Order) -> BasePerformance:
            if order.ticker not in self.data:
                self.data[order.ticker] = Analytics.TickerPerformance(order.ticker)
            return self.data[order.ticker]

    class SpreadPerformance(BasePerformance):
        def __init__(self, profit:float, stop:float):
            self.take_profit = profit
            self.stop_loss = stop
            super().__init__()

    class Spreads(BaseAnalytics):
        def check_new_data(self, order:Order) -> BasePerformance:
            profit = order.merchant_params.takeprofit_percent
            stop = order.merchant_params.stoploss_percent
            key = f"{profit}-{stop}"
            if key not in self.data:
                self.data[key] = Analytics.SpreadPerformance(profit=profit, stop=stop)
            return self.data[key]

    class IntervalPerformance(BasePerformance):
        def __init__(self, high_interval:str, low_interval:str):
            self.high_interval = high_interval
            self.low_interval = low_interval
            super().__init__()

    class Intervals(BaseAnalytics):
        def check_new_data(self, order:Order) -> BasePerformance:
            hi = order.merchant_params.high_interval
            lo = order.merchant_params.low_interval
            key = f"{hi}-{lo}"
            if key not in self.data:
                self.data[key] = Analytics.IntervalPerformance(high_interval=hi, low_interval=lo)
            return self.data[key]
        
    @staticmethod
    def all_performance_metrics(ledger_entries:list[Entry]) -> dict:
            interval_analytics = Analytics.Intervals()
            spread_analytics = Analytics.Spreads()
            ticker_analytics = Analytics.Tickers()
            overall = Analytics.Overall()

            for ledger_entry in ledger_entries:
                interval_analytics.add(ledger_entry=ledger_entry)
                spread_analytics.add(ledger_entry=ledger_entry)
                ticker_analytics.add(ledger_entry=ledger_entry)
                overall.add(ledger_entry=ledger_entry)
            
            overall_results = [result.__dict__ for result in overall.results()]
            interval_results = [interval.__dict__ for interval in interval_analytics.results()]
            spread_results = [spread.__dict__ for spread in spread_analytics.results()]
            ticker_results = [ticker.__dict__ for ticker in ticker_analytics.results()]
            
            return {
                "overall": overall_results,
                "intervals": interval_results,
                "spreads": spread_results,
                "tickers": ticker_results
            }


if __name__ == "__main__":
    import unittest

    class TestLedgerAnalytics(unittest.TestCase):
        def test_performance_by_interval(self):
            pass

    unittest.main()