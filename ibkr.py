import logging
import requests
import os

from order_capable import OrderCapable

def IBKR_ENV_GATEWAY_ENDPOINT():
    return "IBKR_GATEWAY_ENDPOINT" 

def IBKR_ENV_GATEWAY_PASSWD():
    return "IBKR_GATEWAY_PASSWORD"

class IBKRClient(OrderCapable):

    def cancel_order(self, ticker, order_id):
        return super().cancel_order(ticker, order_id)
    
    def place_sell_order(self, ticker, order_id):
        return super().place_sell_order(ticker, order_id)

    def place_limit_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float, broker_params={}) -> dict:
        event = self.create_event(type="IBKROrder", source=source, ticker=ticker, contracts=contracts, limit=limit, take_profit=take_profit, stop_loss=stop_loss)
        gateway_endpoint = self._cfg_gateway_endpoint()
        headers = {
            "Content-Type": "application/json",
            "X-Gateway-Password": self._cfg_gateway_passwd()
        }
        response = requests.post(gateway_endpoint, headers=headers, data=event, timeout=10)
        if response.status_code != 200:
            logging.error(f"Failed to place IBKR order: {response.status_code} - {response.text}")
            raise ValueError(f"Failed to place order: {response.text}")
        return {
            "status_code": response.status_code,
            "response": response.text,
            "source": gateway_endpoint
        }

    def _cfg_gateway_endpoint(self) -> str:
        gateway_endpoint = os.environ[IBKR_ENV_GATEWAY_ENDPOINT()]
        if gateway_endpoint is None or len(gateway_endpoint) == 0:
            raise ValueError(f"{IBKR_ENV_GATEWAY_ENDPOINT()} cannot be None")
        return gateway_endpoint
    
    def _cfg_gateway_passwd(self) -> str:
        gateway_passwd = os.environ[IBKR_ENV_GATEWAY_PASSWD()]
        if gateway_passwd is None or len(gateway_passwd) == 0:
            raise ValueError(f"{IBKR_ENV_GATEWAY_PASSWD()} cannot be None")
        return gateway_passwd

