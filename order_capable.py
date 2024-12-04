
from abc import ABC, abstractmethod

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
