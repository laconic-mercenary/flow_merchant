import uuid

from abc import ABC, abstractmethod

from utils import unix_timestamp_ms

class OrderCapable(ABC):

    @abstractmethod
    def place_limit_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float, broker_params={}) -> dict:
        pass

    @abstractmethod
    def cancel_order(self, ticker: str, order_id: str) -> dict:
        pass

    @abstractmethod
    def place_sell_order(self, ticker:str, contracts:float, tracking_id: str = None) -> dict:
        pass

    def create_event(self, type: str, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float) -> dict:
        attributes = {
            "type": f"net.revanchist.flowmerchant.{type}",
            "source": "/api/flow_merchant",
            "id": f"{source}-{str(uuid.uuid4())}",
            "datacontenttype": "application/json",
            "subject": f"{source}"
        }
        payload = {
            "orders": {
                "market_order": {
                    "ticker": ticker,
                    "contracts": contracts,
                    "limit_price": limit
                },
                "stop_loss_order" : {
                    "stop_loss_price": stop_loss
                },
                "take_profit_order" : {
                    "take_profit_price": take_profit
                }
            }
        }
        return { 
            "metadata": attributes, 
            "data": payload
        }