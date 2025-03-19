from merchant_signal import MerchantSignal
from utils import rand_select

_STANDARD_INTERVALS = {
    "5-minute": "5m",
    "1-hour": "1h"
}

_BRACKETS = {
    "3": [
        (1.0, 1.0),
        (3.0, 2.0),
        (4.0, 1.0),
        (7.0, 1.5),
    ],
    "5": [
        (1.0, 1.0),
        (3.0, 2.0),
        (4.0, 1.0),
        (7.0, 1.5),
    ],
    "15": [
        (1.0, 1.0),
        (3.0, 1.0),
        (5.0, 2.0),
        (10.0, 2.0),
        (15.0, 2.0)
    ],
    "30": [
        (1.0, 1.0),
        (3.0, 1.0),
        (5.0, 2.0),
        (10.0, 2.0),
        (15.0, 2.0)
    ],
    "60": [
        (1.0, 1.0),
        (3.0, 1.0),
        (5.0, 2.0),
        (10.0, 2.0),
        (15.0, 2.0)
    ]
}

class StandardizedIntervals(MerchantSignal):

    def __init__(self, signal:MerchantSignal, database:dict):
        super().__init__(signal.msg)
        if database is None:
            raise ValueError("interval database is required")
        self.interval_db = database

    def low_interval(self):
        low_int = super().low_interval()
        if low_int not in self.interval_db:
            raise ValueError(f"{low_int} was not found in interval database {self.interval_db}")
        return self.interval_db.get(low_int)

    def high_interval(self):
        high_int = super().high_interval()
        if high_int not in self.interval_db:
            raise ValueError(f"{high_int} was not found in interval database {self.interval_db}")
        return self.interval_db.get(high_int)


class TradingViewIntervals(StandardizedIntervals):

    def __init__(self, signal:MerchantSignal):
        super().__init__(signal=signal, database={
            "5": _STANDARD_INTERVALS.get("5-minute"),
            "60": _STANDARD_INTERVALS.get("1-hour")
        })

class RandomBracket(MerchantSignal):

    def __init__(self, signal:MerchantSignal):
        super().__init__(signal.msg)
        low, high = self.random_bracket(_BRACKETS)
        if self.is_set(super().suggested_stoploss()):
            low = super().suggested_stoploss()
        if self.is_set(super().takeprofit_percent()):
            high = super().takeprofit_percent()
        self.flowmerchant.update({
            "random_bracket": (low, high)
        })

    def suggested_stoploss(self) -> float:
        low, high = self.flowmerchant.get("random_bracket")
        stoploss = low
        return stoploss

    def takeprofit_percent(self) -> float:
        low, high = self.flowmerchant.get("random_bracket")
        takeprofit = high
        return takeprofit
    
    def is_set(self, bracket:float) -> bool:
        return bracket != 0.0
    
    def random_bracket(self, brackets:dict) -> tuple[float, float]:
        if self.low_interval() not in brackets:
            raise ValueError(f"low_interval not supported: {self.low_interval()}")
        brackets = brackets[self.low_interval()]
        low, high = rand_select(brackets)
        return (low, high)

def apply_all(signal: MerchantSignal) -> MerchantSignal:
    signal = RandomBracket(signal=signal)
    return signal

if __name__ == "__main__":
    import unittest

    class Test(unittest.TestCase):
 
        def test_e2e(self):
            ### TODO this will not work well with the standardized intervals
            signal = MerchantSignal({
                "flowmerchant": {
                    "low_interval": "5",
                    "high_interval": "60"
                }
            })
            signal = RandomBracket(signal=signal)
            self.assertIn(signal.random_bracket(_BRACKETS), _BRACKETS.get("5"))
                        
    unittest.main()