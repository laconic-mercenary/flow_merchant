
from abc import ABC, abstractmethod

from order_capable import Broker
from merchant_order import Order
from merchant_signal import MerchantSignal
from transactions import Transaction, TransactionAction

class HandleResult:
    def __init__(self, target_order:Order, additional_data:dict = {}, complete:bool = False, transaction:Transaction = None):
        if target_order is None:
            raise ValueError("target_order cannot be None")
        if not isinstance(target_order, Order):
            raise TypeError(f"target_order must be an Order, got {type(target_order)}")
        self.target_order = target_order
        self.complete = complete
        self.additional_data = additional_data
        self.transaction = transaction

class OrderStrategy(ABC):

    @abstractmethod
    def place_orders(self, broker: Broker, signal: MerchantSignal, merchant_state:dict, merchant_params:dict = {}) -> Order:
        pass

    @abstractmethod
    def handle_take_profit(self, broker: Broker, order:Order, merchant_params:dict = {}) -> HandleResult:
        pass

    @abstractmethod
    def handle_stop_loss(self, broker: Broker, order:Order, merchant_params:dict = {}) -> HandleResult:
        pass

    def name(self) -> str:
        return type(self).__name__