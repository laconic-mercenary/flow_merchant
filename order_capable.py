
from abc import ABC, abstractmethod

from utils import null_or_empty

class Broker(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass

class MarketOrderable(ABC):
    @abstractmethod
    def place_market_order(self, ticker:str, action:str, contracts:float, broker_params:dict = {}, tracking_id = None) -> dict:
        pass

    @abstractmethod
    def standardize_market_order(self, market_order_result: dict) -> dict:
        pass

class LimitOrderable(ABC):

    @abstractmethod
    def place_limit_order(self, ticker:str, action:str, contracts:float, limit:float, broker_params: dict={}) -> dict:
        pass

    @abstractmethod
    def standardize_limit_order(self, limit_order_result: dict) -> dict:
        pass
        
##
# Stop Market Order
##

class StopMarketOrder:
    def __init__(self, timestamp:float, order_id:str):
        if timestamp is None:
            raise ValueError("timestamp is None")
        if timestamp < 0:
            raise ValueError("timestamp is negative")
        if null_or_empty(order_id):
            raise ValueError("order_id is None or empty")
        self.timestamp = timestamp
        self.order_id = order_id

class StopMarketOrderable(ABC):

    @abstractmethod
    def place_stop_market_order(self, ticker:str, action:str, contracts:float, stop:float, broker_params: dict={}) -> dict:
        pass

    @abstractmethod
    def standardize_stop_market_order(self, stop_market_order_result: dict) -> StopMarketOrder:
        pass

##
# Order Cancel
##

class OrderCancelable(ABC):

    @abstractmethod
    def cancel_order(self, ticker: str, order_id: str) -> dict:
        pass

class DryRunnable(ABC):

    @abstractmethod
    def place_market_order_test(self, ticker:str, action:str, contracts:float, broker_params:dict = {}, tracking_id = None) -> dict:
        pass

    @abstractmethod
    def place_limit_order_test(self, ticker:str, action:str, contracts:float, limit:float, broker_params: dict={}) -> dict:
        pass

    @abstractmethod
    def cancel_order_test(self, ticker: str, order_id: str) -> dict:
        pass
