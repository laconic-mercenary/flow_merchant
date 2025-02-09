from ledger import Ledger, Entry
from merchant_order import Order, MerchantParams, Metadata, SubOrders, SubOrder, Projections
from utils import unix_timestamp_ms, consts as util_consts

class Performance:
    def __init__(self, total_pnl: float = None, highest_pnl: float = None, lowest_pnl: float = None, win_pct: float = None) -> None:
        self.total_pnl = total_pnl
        self.highest_pnl = highest_pnl
        self.lowest_pnl = lowest_pnl
        self.win_pct = win_pct

class Pnl:
    def __init__(self, timestamp: int = None, pnl: float = None):
        self.timestamp = timestamp
        self.pnl = pnl

class TickerData:
    def __init__(self, starttime: int = None, endtime: int = None, notes: str = None, performance: Performance = None, entries: list[Entry] = None):
        self.starttime = starttime
        self.endtime = endtime
        self.notes = notes
        self.performance = performance
        self.entries = entries
        
    def ticker(self) -> str:
        if self.entries is None or len(self.entries) == 0:
            raise ValueError("entries is required")
        return self.entries[0].name

class SpreadData:
    def __init__(self, take_profit: float = None, stop_loss: float = None, by_ticker: dict[str, TickerData] = None):
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.by_ticker = by_ticker

class IntervalData:
    def __init__(self, high_interval: int = None, low_interval: int = None, by_spread: dict[str, SpreadData] = None):
        self.high_interval = high_interval
        self.low_interval = low_interval
        self.by_spread = by_spread
    
class LedgerAnalysis:
    def __init__(self, by_interval: dict[str, IntervalData] = None):
        self.by_interval = by_interval

class LedgerAnalytics:
    def performance_by_interval(self, entries:list[Entry]) -> LedgerAnalysis:
        if entries is None:
            raise ValueError("entries is required")
        by_interval = {}
        winner_ct = 0
        for entry in entries:
            order = Order.from_dict(entry.data)
            interval_key = f"{order.merchant_params.high_interval}-{order.merchant_params.low_interval}"
            if interval_key not in by_interval:
                by_interval[interval_key] = IntervalData(
                    high_interval=order.merchant_params.high_interval,
                    low_interval=order.merchant_params.low_interval,
                    by_spread={}
                )
            spread_key = f"{order.merchant_params.takeprofit_percent}-{order.merchant_params.stoploss_percent}"
            if spread_key not in by_interval[interval_key].by_spread:
                by_interval[interval_key].by_spread[spread_key] = SpreadData(
                    take_profit=order.merchant_params.takeprofit_percent,
                    stop_loss=order.merchant_params.stoploss_percent,
                    by_ticker={}
                )
            ticker_key = order.ticker
            if ticker_key not in by_interval[interval_key].by_spread[spread_key].by_ticker:
                by_interval[interval_key].by_spread[spread_key].by_ticker[ticker_key] = TickerData(
                    starttime=entry.timestamp,
                    endtime=entry.timestamp,
                    notes=order.merchant_params.notes,
                    performance=Performance(
                        total_pnl=0.0,
                        highest_pnl=entry.amount,
                        lowest_pnl=entry.amount,
                        win_pct=0.0
                    ),
                    entries=[]
                )
            ticker_data = by_interval[interval_key].by_spread[spread_key].by_ticker[ticker_key]
            ticker_data.entries.append(entry)
            ticker_data.endtime = max(ticker_data.endtime, entry.timestamp)
            ticker_data.starttime = min(ticker_data.starttime, entry.timestamp)
            performance = ticker_data.performance
            performance.total_pnl += entry.amount
            performance.highest_pnl = max(performance.highest_pnl, entry.amount)
            performance.lowest_pnl = min(performance.lowest_pnl, entry.amount)
            if entry.amount > 0.0:
                winner_ct += 1.0
            if len(entries) != 0:
                performance.win_pct = winner_ct / len(entries)

        return LedgerAnalysis(by_interval=by_interval)  

class PerformanceView:
    def __init__(self, ledger_analysis: LedgerAnalysis):
        if ledger_analysis is None:
            raise ValueError("ledger_analysis is required")
        self.ledger_analysis = ledger_analysis

    class SpreadPerformance:
        def __init__(self, spread: SpreadData, tickers: list[TickerData]):
            if spread is None:
                raise ValueError("spread is required")
            if tickers is None:
                raise ValueError("tickers is required")
            self.spread = spread
            self.tickers = tickers
            self.total_pnl = sum([ticker.performance.total_pnl for ticker in tickers])

    class IntervalPerformance:
        def __init__(self, interval: IntervalData, spreads: list):
            if interval is None:
                raise ValueError("interval is required")
            if spreads is None:
                raise ValueError("spreads is required")
            if len(spreads) != 0:
                if not isinstance(spreads[0], PerformanceView.SpreadPerformance):
                    raise ValueError("spreads must be a list of SpreadPerformance")
            self.interval = interval
            self.spreads = spreads
            self.total_pnl = sum([spread.total_pnl for spread in spreads])

    def profits_by_category(self) -> list[IntervalPerformance]:
        interval_results = []
        by_interval = self.ledger_analysis.by_interval
        
        for interval_key in by_interval.keys():
            spread_results = []
            intervals = by_interval[interval_key]
            
            for spread_key in intervals.by_spread.keys():
                ticker_results = []
                spreads = intervals.by_spread[spread_key]
                
                for ticker_key in spreads.by_ticker.keys():
                    tickers = spreads.by_ticker[ticker_key]
                    ticker_results.append(tickers)
                
                ticker_results.sort(key=lambda ticker: ticker.performance.total_pnl, reverse=True)
                spread_results.append(self.SpreadPerformance(spreads, ticker_results))

            spread_results.sort(key=lambda spread: spread.total_pnl, reverse=True)
            interval_results.append(self.IntervalPerformance(intervals, spread_results))

        interval_results.sort(key=lambda interval: interval.total_pnl, reverse=True)

        return interval_results

if __name__ == "__main__":
    import unittest

    class TestLedgerAnalytics(unittest.TestCase):
        def test_performance_by_interval(self):
            entries = [
                Entry(
                    test=False, 
                    amount=1.0, 
                    name="test", 
                    timestamp=1, 
                    hash="asdf", 
                    data=Order(
                        ticker="BTCUSDT",
                        merchant_params=MerchantParams(
                            high_interval="60",
                            low_interval="5",
                            takeprofit_percent=1.0,
                            stoploss_percent=1.0,
                            notes="test1",
                            version=2
                        ),
                        sub_orders=SubOrders(
                            main_order=SubOrder(
                                id="test1",
                                api_rx={},
                                time=1692121200000,
                                price=1.0,
                                contracts=1
                            ),
                            stop_loss=SubOrder(
                                id="test2",
                                api_rx={},
                                time=1692121200000,
                                price=0.8,
                                contracts=1
                            ),
                            take_profit=SubOrder(
                                id="test3",
                                api_rx={},
                                time=1692121200000,
                                price=1.2,
                                contracts=1
                            )
                        ),
                        metadata=Metadata(
                            id="test1",
                            time_created=unix_timestamp_ms(),
                            is_dry_run=False
                        ),
                        projections=Projections(
                            profit_without_fees=1.0,
                            loss_without_fees=1.0
                        )
                    ).__dict__
                ),
                Entry(
                    test=False,
                    amount=-0.5,
                    name="test2",
                    timestamp=2,
                    hash="asdf2",
                    data=Order(
                        ticker="BTCUSDT",
                        merchant_params=MerchantParams(
                            high_interval="60",
                            low_interval="5",
                            takeprofit_percent=1.0,
                            stoploss_percent=1.0,
                            notes="test2",
                            version=1
                        ),
                        sub_orders=SubOrders(
                            main_order=SubOrder(
                                id="test2a",
                                api_rx={},
                                time=1692121200000,
                                price=1.0,
                                contracts=1
                            ),
                            stop_loss=SubOrder(
                                id="test2b",
                                api_rx={},
                                time=1692121200000,
                                price=0.8,
                                contracts=1
                            ),
                            take_profit=SubOrder(
                                id="test2c",
                                api_rx={},
                                time=1692121200000,
                                price=1.2,
                                contracts=1
                            ),
                        ),
                        metadata=Metadata(
                            id="test2",
                            time_created=1692121200000,
                            is_dry_run=False
                        ),
                        projections = Projections(
                            profit_without_fees=1.0,
                            loss_without_fees=1.0
                        )
                    ).__dict__
                ),
                Entry(
                    test=False,
                    amount=20.0,
                    name="test3",
                    timestamp=6,
                    hash="asdf3",
                    data=Order(
                        ticker="ETHUSDT",
                        merchant_params=MerchantParams(
                            high_interval="5",
                            low_interval="5",
                            takeprofit_percent=5.0,
                            stoploss_percent=5.0,
                            notes="test3",
                            version=1
                        ),
                        sub_orders=SubOrders(
                            main_order=SubOrder(
                                id="test3a",
                                api_rx={},
                                time=1692121200000,
                                price=1.0,
                                contracts=1
                            ),
                            stop_loss=SubOrder(
                                id="test3b",
                                api_rx={},
                                time=1692121200000,
                                price=0.8,
                                contracts=1
                            ),
                            take_profit=SubOrder(
                                id="test3c",
                                api_rx={},
                                time=1692121200000,
                                price=1.2,
                                contracts=1
                            ),
                        ),
                        metadata=Metadata(
                            id="test3",
                            time_created=1692121200000,
                            is_dry_run=False
                        ),
                        projections = Projections(
                            profit_without_fees=1.0,
                            loss_without_fees=1.0
                        )
                    ).__dict__
                )
            ]

            perf_analysis = LedgerAnalytics().performance_by_interval(entries)
            self.assertEqual(len(perf_analysis.by_interval.keys()), 2)

            self.assertIn(
                "60-5",
                perf_analysis.by_interval.keys()
            )
            self.assertIn(
                "5-5",
                perf_analysis.by_interval.keys()
            )

            ticker_data_5_5 = perf_analysis.by_interval["5-5"].by_spread["5.0-5.0"].by_ticker["ETHUSDT"]
            performance_5_5 = ticker_data_5_5.performance
            self.assertEqual(performance_5_5.total_pnl, 20.0)
            self.assertEqual(performance_5_5.highest_pnl, 20.0)
            self.assertEqual(performance_5_5.lowest_pnl, 20.0)
            self.assertEqual(ticker_data_5_5.endtime, 6)
            self.assertEqual(ticker_data_5_5.starttime, 6)

            ticker_data_5_60 = perf_analysis.by_interval["60-5"].by_spread["1.0-1.0"].by_ticker["BTCUSDT"]
            performance_5_60 = ticker_data_5_60.performance
            self.assertEqual(performance_5_60.total_pnl, 0.5)
            self.assertEqual(performance_5_60.highest_pnl, 1.0)
            self.assertEqual(performance_5_60.lowest_pnl, -0.5)
            self.assertEqual(ticker_data_5_60.endtime, 2)
            self.assertEqual(ticker_data_5_60.starttime, 1)

            ##
            performance_view = PerformanceView(perf_analysis)
            profits_by_category = performance_view.profits_by_category()
            
            self.assertEqual(len(profits_by_category), 2)
            self.assertTrue(profits_by_category[0].total_pnl > profits_by_category[1].total_pnl)
            self.assertEqual(profits_by_category[0].interval.high_interval, "5")
            self.assertEqual(profits_by_category[0].interval.low_interval, "5")
            self.assertEqual(profits_by_category[1].interval.high_interval, "60")
            self.assertEqual(profits_by_category[1].interval.low_interval, "5")
            self.assertEqual(profits_by_category[0].total_pnl, 20.0)
            self.assertEqual(profits_by_category[1].total_pnl, 0.5)
            
            self.assertEqual(len(profits_by_category[0].spreads), 1)
            self.assertEqual(profits_by_category[1].spreads[0].spread.take_profit ,1.0)
            self.assertEqual(profits_by_category[1].spreads[0].spread.stop_loss, 1.0)

    unittest.main()