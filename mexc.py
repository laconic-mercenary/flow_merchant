import hmac
import hashlib
import logging
import os
import requests
import time

from urllib.parse import urlencode

from live_capable import LiveCapable
from order_capable import OrderCapable
from utils import unix_timestamp_ms

def MEXC_ENV_API_ENDPOINT():
    return "MEXC_API_ENDPOINT" 

def MEXC_ENV_API_KEY():
    return "MEXC_API_KEY"

def MEXC_ENV_API_SECRET():
    return "MEXC_API_SECRET"

def MEXC_API_ENDPOINT():
    return "https://api.mexc.com"

def MEXC_API_RECEIVE_WINDOW_MILLIS():
    return 10000

class MEXC_API(OrderCapable, LiveCapable):

    def cancel_order(self, ticker: str, order_id: str) -> dict:
        return self._api_cancel_order(ticker=ticker, order_id=order_id)

    def place_limit_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float, broker_params={}) -> dict:
        if not self._api_ping():
            msg = "MEXC API is not available"
            logging.error(msg)
            raise ValueError(msg)
        
        action = "BUY"
        order_type = "MARKET"
        market_order_ts = unix_timestamp_ms()
        market_order_id = f"FM_{market_order_ts}"
        market_order_params = self._create_order_params(
            ticker=ticker, 
            action=action, 
            order_type=order_type, 
            contracts=contracts, 
            tracking_id=market_order_id
        )
        market_order_api_rx = self._api_place_order(market_order_params)

        """ TODO 
        The stop loss order may be too fast for the broker to catch up. 
        This is because the market order may not be placed immediately.
        Which means we might get an oversold error. 
        """
        exit_side = "SELL" if action == "BUY" else "BUY"
        stop_loss_order_ts = unix_timestamp_ms()
        stop_loss_order_id = f"{market_order_id}_s"
        try:
            stop_loss_order_api_rx = self._api_stop_limit(
                ticker=ticker,
                exit_side=exit_side,
                price=stop_loss,
                quantity=contracts,
                order_id=stop_loss_order_id
            )
        except Exception as e:
            logging.warning(f"error placing stop order, will retry...", exc_info=True)
            time.sleep(5)
            if "orderId" not in market_order_api_rx:
                msg = f"orderId key is not in API order response - received {market_order_api_rx}"
                raise ValueError(msg)
            market_order_api_rx = self._api_get_order(ticker, market_order_api_rx.get("orderId"))

            if "executedQty" not in market_order_api_rx:
                logging.warning(f"executedQty key not found in order response, will default to {contracts}. Order response: {market_order_api_rx}")
            else:
                contracts = float(market_order_api_rx.get("executedQty"))
            
            stop_loss_order_api_rx = self._api_stop_limit(
                ticker=ticker,
                exit_side=exit_side,
                price=stop_loss,
                quantity=contracts,
                order_id=stop_loss_order_id
            )

        return {
            "broker": { 
                "name": "mexc",
                "params": broker_params,
            },
            "ticker": ticker,
            "orders": {
                "main": {
                    "id": market_order_id,
                    "api_response": market_order_api_rx,
                    "time": market_order_ts,
                    "contracts": contracts,
                    "price": limit
                },
                "stop_loss": {
                    "id": stop_loss_order_id,
                    "api_response": stop_loss_order_api_rx,
                    "time": stop_loss_order_ts,
                    "price": stop_loss
                }
            }
        }
    
    def place_sell_order(self, ticker:str, contracts:float, tracking_id: str = None) -> dict:
        action = "SELL"
        order_type = "MARKET"
        order_id = tracking_id if tracking_id is not None else f"{ticker}_{unix_timestamp_ms()}"
        sell_order_params = self._create_order_params(
            ticker=ticker,
            action=action,
            order_type=order_type,
            contracts=contracts,
            tracking_id=order_id
        )
        return self._api_place_order(sell_order_params)
    
    def get_order(self, ticker: str, order_id: str) -> dict:
        logging.debug("get_order")
        order = self._api_get_order(ticker, order_id)
        if "status" not in order:
            raise ValueError(f"expected key status to be in {order}")
        if "executedQty" not in order:
            raise ValueError(f"expected key executedQty to be in {order}")
        if "price" not in order:
            raise ValueError(f"expected key price to be in {order}")
        if "time" not in order:
            raise ValueError(f"expected key time to be in {order}")
        return {
            "timestamp": order.get("time"),
            "contracts": order.get("executedQty"),
            "price": order.get("price"),
            "ready": True if order.get("status").upper() == "FILLED" else False
        }
    
    def get_current_prices(self, symbols: list[str]) -> dict:
        prices = self._api_get_current_prices()
        logging.debug(f"Received response for get prices: {prices}")
        result = { }
        for price in prices:
            ticker = price["symbol"]
            if ticker in symbols:
                result[ticker] = float(price["price"])
        return result 
    
    def _cfg_api_key(self) -> str:
        api_key = os.environ[MEXC_ENV_API_KEY()]
        if api_key is None or len(api_key) == 0:
            raise ValueError(f"{MEXC_ENV_API_KEY()} cannot be None")
        return api_key
    
    def _cfg_api_secret(self) -> str:
        api_secret = os.environ[MEXC_ENV_API_SECRET()]
        if api_secret is None or len(api_secret) == 0:
            raise ValueError(f"{MEXC_ENV_API_SECRET()} cannot be None")
        return api_secret

    def _cfg_api_endpoint(self) -> str:
        return MEXC_API_ENDPOINT()

    def _cfg_recv_window_ms(self) -> int:
        return MEXC_API_RECEIVE_WINDOW_MILLIS()
    
    def _request_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "X-MEXC-APIKEY": self._cfg_api_key()
        }

    def _sign(self, params: dict) -> str:
        query_string = urlencode(params, doseq=True)
        return hmac.new(
            self._cfg_api_secret().encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _api_ping(self) -> bool:
        url = f"{self._cfg_api_endpoint()}/api/v3/ping"
        response = requests.get(url)
        return response.status_code == 200
    
    def _api_get_current_prices(self) -> dict:
        url = f"{self._cfg_api_endpoint()}/api/v3/ticker/price"
        response = requests.get(url)
        return response.json()

    def _api_get_server_time(self) -> str:
        url = f"{self._cfg_api_endpoint()}/api/v3/time"
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f"Failed to get server time: {response.status_code} - {response.text}")
            raise ValueError(f"Failed to get server time: {response.status_code} - {response.text}")
        response = response.json()
        logging.info(f"Server time (response): {response}")
        if "serverTime" not in response:
            logging.error(f"Failed to get server time")
            raise ValueError(f"Failed to get server time")
        return response.get("serverTime")
    
    def _api_stop_limit(self, ticker: str, exit_side: str, quantity: float, price: float, order_id: str) -> dict:
        params = self._create_order_params(
            ticker=ticker,
            action=exit_side,
            order_type="LIMIT",
            contracts=quantity,
            target_price=price,
            tracking_id=order_id
        )
        return self._api_place_order(params, dry_run=False)
    
    def _api_get_account_info(self) -> dict:
        """ NOTE
        This requires special permissions in the MEXC token. 
        """
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/account"
        """ NOTE
        unix_timestamp() was out of sync with the MEXC server and it was getting rejected.
        Check on this later as we don't want to get throttled by the API for too many calls.
        For now just use remote server time.
        """
        remote_server_time = self._timestamp()
        params = {
            "timestamp": remote_server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)
        headers = self._request_headers()
        response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"MEXC API account info response: {response.status_code} - {response.text}")
        
        if response.status_code != 200:
            msg = f"Failed to get account info: {response.status_code} - {response.text}"
            logging.error(msg)
            raise ValueError(msg)
        
        return response.json()
    
    def _api_get_order(self, symbol: str, order_id: str) -> dict:
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/order"
        """ NOTE
        unix_timestamp() was out of sync with the MEXC server and it was getting rejected.
        Check on this later as we don't want to get throttled by the API for too many calls.
        For now just use remote server time.
        """
        remote_server_time = self._timestamp()
        params = {
            "symbol": symbol,
            "origClientOrderId": order_id,
            "timestamp": remote_server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)
        headers = self._request_headers()
        response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"MEXC API get order status response: {response.status_code} - {response.text}")
        
        if response.status_code != 200:
            msg = f"Failed to get order status: {response.status_code} - {response.text}"
            logging.error(msg)
            raise ValueError(msg)
        
        return response.json()

    def _api_get_orders(self, symbol: str) -> dict:
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/allOrders"
        """ NOTE
        unix_timestamp() was out of sync with the MEXC server and it was getting rejected.
        Check on this later as we don't want to get throttled by the API for too many calls.
        For now just use remote server time.
        """
        remote_server_time = self._timestamp()
        params = {
            "symbol": symbol,
            "timestamp": remote_server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)
        headers = self._request_headers()
        response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        
        logging.info(f"MEXC API  get order response: {response.status_code} - {response.text}")
        
        if response.status_code != 200:
            msg = f"Failed to get orders: {response.status_code} - {response.text}"
            logging.error(msg)
            raise ValueError(msg)
        
        return response.json()
    

    def _api_cancel_order(self, ticker: str, order_id: str) -> dict:
        if ticker is None or len(ticker) == 0:
            raise ValueError("ticker is required")
        if order_id is None or len(order_id) == 0:
            raise ValueError("order_id is required")
        
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/order"        
        server_time = self._timestamp()
        params = {
            "symbol": ticker,
            "origClientOrderId": order_id,
            "timestamp": server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)

        headers = self._request_headers()

        response = requests.delete(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"MEXC API cancel order for {ticker} response: {response.status_code} - {response.text}")
        
        if response.status_code == 404:
            logging.warning(f"Orders were not found for {ticker} : {response.text}")
        
        if response.status_code != 200:
            msg = f"MEXC API error in cancelling orders for {ticker}: {response.status_code} - {response.text}"
            logging.error(msg)
            raise ValueError(msg)
        
        return response.json()

    
    def _api_cancel_all_orders(self, ticker: str) -> dict:
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/openOrders"        
        server_time = self._timestamp()
        
        params = {
            "symbol": ticker,
            "timestamp": server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)

        headers = self._request_headers()

        response = requests.delete(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"MEXC API cancel order for {ticker} response: {response.status_code} - {response.text}")
        
        if response.status_code == 404:
            logging.warning(f"Orders were not found for {ticker} : {response.text}")
        
        if response.status_code != 200:
            msg = f"MEXC API error in cancelling orders for {ticker}: {response.status_code} - {response.text}"
            logging.error(msg)
            raise ValueError(msg)
        
        return response.json()

    def _create_order_params(self, ticker: str, action: str, order_type: str, contracts: float, target_price: float = None, tracking_id: str = None) -> dict:
        action = action.upper()
        order_type = order_type.upper()
        if action not in ["BUY", "SELL"]:
            raise ValueError(f"Invalid action: {action}")
        if order_type not in ["LIMIT", "MARKET"]:
            raise ValueError(f"Invalid order type: {order_type}")
        if contracts <= 0.0:
            raise ValueError(f"Invalid number of contracts: {contracts}")
        if target_price is not None and target_price <= 0.0:
            raise ValueError(f"Invalid target price: {target_price}")
        
        timestamp = unix_timestamp_ms() ## self._timestamp()
        params = { 
            "symbol": ticker,
            "side": action,
            "type": order_type,
            "quantity": str(contracts),
            "timestamp": timestamp,
            "recvWindow": self._cfg_recv_window_ms(),
            "newClientOrderId": f"{ticker}_{timestamp}" if tracking_id is None else tracking_id,
        }
            
        if order_type in ["LIMIT"]:
            if target_price is None:
                raise ValueError(f"Invalid target price: {target_price}")
            params["timeInForce"] = "GTC"
            params["price"] = f"{target_price:.8f}"
            
        params["signature"] = self._sign(params)
        
        return params
    
    def _timestamp(self) -> int:
        """ NOTE - there was a problem with the MEXC server being out of sync when I used
        unix_timestamp(), but that was in seconds. I have changed it to ms. 
        If it doesn't work - revert back to the MEXC get server time API call.
        """
        ## timestamp = self._timestamp()
        timestamp = unix_timestamp_ms()
        return timestamp

    def _api_place_order(self, params: dict, dry_run: bool = False) -> dict:
        logging.debug("_api_place_order")
        base_url = self._cfg_api_endpoint()
        order_endpoint = "/api/v3/order/test" if dry_run else "/api/v3/order"

        ticker = params["symbol"]
        quantity = params["quantity"]
        order_type = params["type"]

        headers = self._request_headers()

        logging.info(f"MEXC API - placing {order_type} order for {ticker} with {quantity}. Parameters are: {params}")

        response = requests.post(f"{base_url}{order_endpoint}", headers=headers, params=params)
        
        logging.info(f"MEXC API response: {response.status_code} - {response.text}")

        if response.status_code != 200:
            msg = f"Error placing MEXC API {order_type} order: {response.status_code} - {response.text}"
            raise ValueError(msg)

        return response.json()
    