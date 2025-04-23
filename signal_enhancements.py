from merchant_keys import action
from merchant_signal import MerchantSignal
from order_capable import Broker
from live_capable import LiveCapable
from utils import rand_select

import math
import logging
import os

class SignalEnhancement:
    def is_enabled(self) -> bool:
        return False
    
    def apply(self, signal:MerchantSignal, params:dict = {}) -> MerchantSignal:
        return signal

class RandomBracket(SignalEnhancement):

    def __init__(self):
        ### format is 
        ### <interval>: [(<stop loss>,<take profit), ...]
        ### interval is the LOW interval, not the HIGH
        self._BRACKETS = {
            "3": [
                (1.0, 0.5),
                (1.0, 1.0)
            ],
            "5": [
                (0.75, 0.25),
                (1.0, 0.5),
                (1.0, 1.0),
                (3.0, 1.0)
            ],
            "15": [
                (0.75, 0.25),
                (1.0, 0.5),
                (1.0, 1.0),
                (3.0, 1.0)
            ],
            "30": [
                (0.75, 0.25),
                (1.0, 0.5),
                (1.0, 1.0),
                (3.0, 1.0)
            ],
            "60": [
                (0.75, 0.25),
                (1.0, 0.5),
                (1.0, 1.0),
                (3.0, 1.0)
            ]
        }

    def is_enabled(self) -> bool:
        return os.environ.get("SIGNALENH_RANDOM_BRACKET", "false").lower() == "true"

    def apply(self, signal:MerchantSignal, params:dict = {}) -> MerchantSignal:
        if signal is None:
            raise ValueError("signal cannot be None")
        if not isinstance(signal, MerchantSignal):
            raise TypeError(f"signal must be an instance of MerchantSignal, not {type(signal)}")
        go_for_random = False
        if params.get("global_dry_run_mode", False):
            go_for_random = True
        else:
            if signal.dry_run():
                go_for_random = True
        if go_for_random:
            self.apply_random_bracket(signal=signal)
        else:
            ### don't use random values when money is at-stake
            if not self.is_set(signal.suggested_stoploss()):
                raise ValueError(f"setting a stoploss is required when using real money! ticker: {signal.ticker()}")
            if not self.is_set(signal.takeprofit_percent()):
                raise ValueError(f"setting a takeprofit is required when using real money! ticker: {signal.ticker()}")
        return signal
    
    def apply_random_bracket(self, signal:MerchantSignal) -> None:
        stop_loss, take_profit = self.random_bracket(signal=signal, brackets=self._BRACKETS)
        signal.flowmerchant["suggested_stoploss"] = stop_loss
        signal.flowmerchant["takeprofit_percent"] = take_profit
    
    def is_set(self, bracket:float) -> bool:
        return bracket != 0.0
    
    def random_bracket(self, signal:MerchantSignal, brackets:dict) -> tuple[float, float]:
        if signal.low_interval() not in brackets:
            raise ValueError(f"low_interval not supported: {signal.low_interval()}")
        brackets = brackets[signal.low_interval()]
        stop_loss, take_profit = rand_select(brackets)
        return (stop_loss, take_profit)
    

class CurrencyToContracts(SignalEnhancement):

    def apply(self, signal:MerchantSignal, params:dict = {}) -> MerchantSignal:
        if self.contracts_in_currency_format(signal=signal):
            if signal.action() == action.BUY():
                if "broker" not in params:
                    raise ValueError(f"expected key broker in params, but found {params}")
                broker = params.get("broker")
                self.check_broker(broker=broker)
                currency_amt = signal.contracts()
                current_price = self.fetch_current_price(broker=broker, symbol=signal.ticker())
                base_scale = self.fetch_base_scale(broker=broker, symbol=signal.ticker())
                new_contracts = self.calculate_contracts(
                                    currency_amt=currency_amt,
                                    current_price=current_price,
                                    base_scale=base_scale
                                )
                logging.info(f"[currency to contracts enhancement] for ticker {signal.ticker()}, currency amount was {currency_amt}, current price was {current_price}, base scale was {base_scale}, RESULTS: new contracts: {new_contracts}")
                signal.security["contracts"] = new_contracts
        else:
            logging.warning(f"currency to contracts is enabled but the signal has not enabled it: {signal}")
        return signal
        
    def is_enabled(self) -> bool:
        return os.environ.get("SIGNALENH_CURRENCY_TO_CONTRACTS", "false").lower() == "true"
    
    def check_broker(self, broker:Broker) -> None:
        if not isinstance(broker, LiveCapable):
            raise TypeError(f"Broker must be LiveCapable, got {type(broker)}")

    def contracts_in_currency_format(self, signal:MerchantSignal) -> bool:
        return signal.broker_params().get("currency_to_contracts", False)

    def fetch_current_price(self, symbol:str, broker:LiveCapable) -> float:
        return broker.get_current_prices(symbols=[symbol]).get(symbol)

    def fetch_base_scale(self, symbol:str, broker:LiveCapable) -> float:
        ### TODO - improvement would be to store these in a cache as they change
        ### infrequently. The beneift being we are not taxed on API calls to the broker
        result = broker.get_asset_info(symbols=[symbol])
        return result.base_scale
    
    def calculate_contracts(self, currency_amt: float, current_price: float, base_scale: float = 0.0) -> float:
        if currency_amt <= 0.0:
            raise ValueError("currency_amt must be positive")
        if current_price <= 0.0:
            raise ValueError("current_price must be positive")
        if current_price < base_scale:
            raise ValueError("current_price must be greater than base_scale")
        raw_qty = currency_amt / current_price
        ### the base scale is what the broker requires of multiples of the asset
        ### for example BTC must be in units of 0.0001 or similar
        ### just get as close as possible, and do not exceed the target contracts amount
        ### note, it may be zero
        if base_scale == 0.0:
            base_scale = self.scale_from_price(price=current_price)
        return self.truncate_to_scale(qty=raw_qty, base_scale=base_scale)
        
    def truncate_to_scale(self, qty:float, base_scale:float) -> float:
        decimals = abs(int(math.floor(math.log10(base_scale) + 1e-9)))
        factor = 10 ** decimals
        return math.floor(qty * factor) / factor
    
    def scale_from_price(self, price:float) -> float:
        if price == 0.0:
            return 0.0
        abs_value = abs(price)
        if abs_value >= 1.0:
            return 1.0
        decimal_places = math.ceil(-math.log10(abs_value))
        return 10 ** (-decimal_places)


def apply_all(signal: MerchantSignal, params:dict = {}) -> MerchantSignal:
    if signal is None:
        raise ValueError("signal cannot be None")
    if not isinstance(signal, MerchantSignal):
        raise TypeError(f"signal must be an instance of MerchantSignal, not {type(signal)}")
    if params is None:
        params = {}

    enhancements:list[SignalEnhancement] = [
        RandomBracket(), 
        CurrencyToContracts()
    ]
    for enhancement in enhancements:
        if enhancement.is_enabled():
            logging.info(f"applying signal enhancement: {enhancement.__class__.__name__}.  OLD SIGNAL: {str(signal)}")
            signal:MerchantSignal = enhancement.apply(signal=signal, params=params)
            logging.info(f"applied signal enhancement: {enhancement.__class__.__name__}.  NEW SIGNAL: {str(signal)}")
    return signal

if __name__ == "__main__":
    import unittest

    class Test(unittest.TestCase):
 
        def test_exact_multiple(self):
            # $100 at $10,000 with 0.0001 step = 0.01 BTC
            self.assertAlmostEqual(CurrencyToContracts().calculate_contracts(100, 10000, 0.0001), 0.01)

        def test_rounding_down(self):
            # $123.45 at $9876.54 with 0.0001 step
            # Raw: 0.0125..., truncated: 0.0125
            self.assertAlmostEqual(CurrencyToContracts().calculate_contracts(123.45, 9876.54, 0.0001), 0.0124)

        def test_no_base_scale(self):
            # No truncation, should return exact float
            self.assertAlmostEqual(CurrencyToContracts().calculate_contracts(50, 2500), 0.02)

        def test_high_precision_asset(self):
            # $100 at $0.00123 with step 0.00000001
            # Raw: 81300813.00813008, truncated: 81300813.00813008
            self.assertAlmostEqual(CurrencyToContracts().calculate_contracts(100, 0.00123, 0.00000001), 81300813.00813008)

        def test_zero_amount(self):
            with self.assertRaises(ValueError):
                CurrencyToContracts().calculate_contracts(0, 1234.56, 0.0001)

        def test_zero_price(self):
            with self.assertRaises(ZeroDivisionError):
                CurrencyToContracts().calculate_contracts(100, 0, 0.0001)

        def test_btc_real_case_100usd(self):
            # $100 at $82,000 with 0.00001 step
            # Raw: 0.00121951..., truncated: 0.00121
            self.assertAlmostEqual(CurrencyToContracts().calculate_contracts(100, 82000, 0.00001), 0.00121)

        def test_btc_real_case_250usd(self):
            # $250 at $81,345.67, truncated: 0.00307
            self.assertAlmostEqual(CurrencyToContracts().calculate_contracts(250, 81345.67, 0.00001), 0.00307)

        def test_pepe_case(self):
            # $50 at $0.0000012 with step 0.00000001
            # Raw: 41666666.666..., truncated: 41666666.66
            self.assertAlmostEqual(CurrencyToContracts().calculate_contracts(50, 0.0000012, 0.00000001), 41666666.66)

        def test_shib_case(self):
            # $20 at $0.0000251 with step 0.00000001
            # Raw: 796812.749, truncated: 796812.74
            self.assertAlmostEqual(CurrencyToContracts().calculate_contracts(20, 0.0000251, 0.00000001), 796812.74900398)

        def test_currency_convert_btc(self):
            enh = CurrencyToContracts()
            current_price = 71999
            base_scale = 0.001
            currency_amt = 200.0
            contracts = enh.calculate_contracts(
                            currency_amt=currency_amt, 
                            current_price=current_price, 
                            base_scale=base_scale
                        )
            self.assertEqual(contracts, 0.002)
            base_scale = 0.0005
            contracts = enh.calculate_contracts(
                            currency_amt=currency_amt, 
                            current_price=current_price, 
                            base_scale=base_scale
                        )
            self.assertEqual(contracts, 0.0025)
            
                        
    unittest.main()