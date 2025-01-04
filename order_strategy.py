
from abc import ABC, abstractmethod

from order_capable import Broker
from merchant_signal import MerchantSignal

class OrderStrategy(ABC):

    @abstractmethod
    def place_orders(self, broker: Broker, signal: MerchantSignal, merchant_state:dict, merchant_params:dict = {}) -> dict:
        pass

    @abstractmethod
    def handle_take_profit(self, broker: Broker, order:dict, merchant_params:dict = {}) -> dict:
        pass

    def name(self) -> str:
        return type(self).__name__