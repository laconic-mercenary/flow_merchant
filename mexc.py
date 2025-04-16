import enum
import hmac
import hashlib
import json     
import logging
import os
import requests

from urllib.parse import urlencode

from broker_exceptions import ApiError, OrderAlreadyFilledError, OversoldError, InvalidQuantityScale
from live_capable import LiveCapable
from order_capable import Broker, MarketOrderable, LimitOrderable, OrderCancelable, DryRunnable
from utils import unix_timestamp_ms, unix_timestamp_secs, null_or_empty

### NOTES
### for a list of supported symbols: https://api.mexc.com/api/v3/defaultSymbols

def MEXC_ENV_API_KEY():
    return "MEXC_API_KEY"

def MEXC_ENV_API_SECRET():
    return "MEXC_API_SECRET"

def MEXC_API_ENDPOINT():
    return "https://api.mexc.com"

def MEXC_API_RECEIVE_WINDOW_MILLIS():
    return 10000

class ApiErrors(enum.Enum):
    ORDER_ALREADY_FILLED = -2011
    OVERSOLD = 30005
    INVALID_QUANTITY_SCALE = 400

class ApiErrorResponse:
    def __init__(self, rx: dict) -> None:
        self.msg = rx.get("msg")
        self.code = rx.get("code")

class MEXC_API(Broker, MarketOrderable, LimitOrderable, OrderCancelable, LiveCapable, DryRunnable):

    def get_name(self) -> str:
        return "MEXC"

    def cancel_order(self, ticker: str, order_id: str) -> dict:
        return self._api_cancel_order(
            ticker=ticker, 
            order_id=order_id
        )
    
    def place_market_order(self, ticker:str, action:str, contracts:float, tracking_id = None) -> dict:
        return self._place_market_order(
            ticker=ticker, 
            action=action, 
            contracts=contracts, 
            tracking_id=tracking_id, 
            dry_run=False
        )
    
    def standardize_market_order(self, market_order_result: dict) -> dict:
        override_with_cur_price:bool = True

        if "clientOrderId" not in market_order_result:
            raise ValueError(f"expected key clientOrderId to be in {market_order_result}")
        if "orderId" not in market_order_result:
            raise ValueError(f"expected key orderId to be in {market_order_result}")
        if "transactTime" not in market_order_result:
            raise ValueError(f"expected key transactTime to be in {market_order_result}")
        if "origQty" not in market_order_result:
            raise ValueError(f"expected key origQty to be in {market_order_result}")
        if "price" not in market_order_result:
            raise ValueError(f"expected key price to be in {market_order_result}")
        if override_with_cur_price:
            ### TODO
            ### ### There is a bug in the MEXC API where the market order price is not correct
            ### This is true for BOTH SELLs and BUYs
            ### https://github.com/mexcdevelop/mexc-api-sdk/issues/77
            ### temporarily use current price instead, this will incur a cost to our API usage
            if "__ticker" not in market_order_result:
                raise ValueError(f"expected key __ticker to be in {market_order_result}")
            original_price = market_order_result.get("price")
            ticker = market_order_result.get("__ticker")
            symbols = [ ticker ]
            prices = self.get_current_prices(symbols=symbols)
            market_order_result["price"] = prices.get(ticker)
            logging.warning(f"Due to MEXC bug - https://github.com/mexcdevelop/mexc-api-sdk/issues/77 - overriding the ORDER price from {original_price} to {prices.get(ticker)} for {ticker}. In dry run mode, these values will be equal.")
        return {
            "id": market_order_result.get("clientOrderId"),
            "broker_order_id": market_order_result.get("orderId"),
            "timestamp": market_order_result.get("transactTime"),
            "contracts": float(market_order_result.get("origQty")),
            "price": float(market_order_result.get("price"))
        }
    
    def place_limit_order(self, ticker: str, action: str, contracts: float, limit: float, broker_params: dict = {}) -> dict:
        return self._place_limit_order(
            ticker=ticker, 
            action=action, 
            contracts=contracts, 
            limit=limit, 
            broker_params=broker_params, 
            dry_run=False
        )
    
    def standardize_limit_order(self, limit_order_result: dict) -> dict:
        if "clientOrderId" not in limit_order_result:
            raise ValueError(f"expected key clientOrderId to be in {limit_order_result}")
        if "orderId" not in limit_order_result:
            raise ValueError(f"expected key orderId to be in {limit_order_result}")
        if "transactTime" not in limit_order_result:
            raise ValueError(f"expected key transactTime to be in {limit_order_result}")
        if "origQty" not in limit_order_result:
            raise ValueError(f"expected key origQty to be in {limit_order_result}")
        if "price" not in limit_order_result:
            raise ValueError(f"expected key price to be in {limit_order_result}")
        return {
            "id": limit_order_result.get("clientOrderId"),
            "broker_order_id": limit_order_result.get("orderId"),
            "timestamp": limit_order_result.get("transactTime"),
            "contracts": float(limit_order_result.get("origQty")),
            "price": float(limit_order_result.get("price"))
        }
    
    # def place_stop_market_order(self, ticker:str, action:str, contracts:float, stop:float, broker_params:dict = {}) -> dict:
    #     if null_or_empty(ticker):
    #         raise ValueError(f"ticker is required")
    #     if null_or_empty(action):
    #         raise ValueError(f"action is required")
    #     if contracts is None:
    #         raise ValueError(f"contracts is required")
    #     if contracts <= 0.0:
    #         raise ValueError(f"contracts must be greater than 0")
    #     if stop is None:
    #         raise ValueError(f"stop is required")
    #     if stop <= 0.0:
    #         raise ValueError(f"stop must be greater than 0")
    #     tracking_id = f"FM{unix_timestamp_ms()}"
    #     params = self._create_order_params(
    #         ticker=ticker,
    #         action="SELL",
    #         order_type="STOP_LOSS",
    #         contracts=contracts, 
    #         target_price=stop,
    #         tracking_id=tracking_id
    #     )
    #     return self._api_place_order(params=params, dry_run=False)
    #     # import ccxt
    #     # exchange = ccxt.mexc({
    #     #     'apiKey': self._cfg_api_key(),
    #     #     'secret': self._cfg_api_secret(),
    #     #     'options': {
    #     #         'createMarketBuyOrderRequiresPrice': False,
    #     #     }
    #     # })
    #     # return exchange.create_stop_market_order(
    #     #     symbol=ticker,
    #     #     side="sell",
    #     #     amount=contracts,
    #     #     triggerPrice=stop
    #     # )


    # def standardize_stop_market_order(self, stop_market_order_result:dict) -> StopMarketOrder:
    #     if "price" not in stop_market_order_result:
    #         raise ValueError(f"expected key price to be in {stop_market_order_result}")
    #     if "time" not in stop_market_order_result:
    #         raise ValueError(f"expected key time to be in {stop_market_order_result}")
    #     if "clientOrderId" not in stop_market_order_result:
    #         raise ValueError(f"expected key clientOrderId to be in {stop_market_order_result}")
    #     return StopMarketOrder(
    #         timestamp=int(stop_market_order_result.get("time")),
    #         order_id=stop_market_order_result.get("clientOrderId")
    #     )
    
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
            "_original": str(order),
            "id": order_id,
            "status": order.get("status"),
            "timestamp": order.get("time"),
            "contracts": float(order.get("executedQty")),
            "price": float(order.get("price")),
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
        result.update({ "_timechecked": unix_timestamp_ms() })
        return result 
    
    def place_market_order_test(self, ticker: str, action: str, contracts: float, broker_params: dict = {}, tracking_id:str=None) -> dict:
        result = self._place_market_order(
            ticker=ticker, 
            action=action, 
            contracts=contracts, 
            tracking_id=tracking_id, 
            dry_run=True
        )
        logging.info(f"place_market_order_test result: {result}")
        """ NOTE - Although not very authentic, this is as close to a fill-price for a market order as we can get. """
        current_prices = self.get_current_prices(symbols=[ticker])
        if ticker not in current_prices:
            raise ValueError(f"expected key {ticker} to be in {current_prices}")
        tracking_id = f"{ticker}-{unix_timestamp_ms()}" if tracking_id is None else tracking_id
        dryrun_tracking_id = f"{tracking_id}_DRYRUN"
        return {
            "clientOrderId": dryrun_tracking_id,
            "orderId": dryrun_tracking_id,
            "transactTime": unix_timestamp_ms(),
            "origQty": float(contracts),
            "price": float(current_prices.get(ticker)),
            "__ticker": ticker
        }

    def place_limit_order_test(self, ticker:str, action:str, contracts:float, limit:float, broker_params:dict = {}, tracking_id:str = None) -> dict:
        results = self._place_limit_order(
            ticker=ticker, 
            action=action, 
            contracts=contracts, 
            limit=limit, 
            dry_run=True
        )
        current_prices = self.get_current_prices(symbols=[ticker])
        if ticker not in current_prices:
            raise ValueError(f"expected key {ticker} to be in {current_prices}")
        tracking_id = f"{ticker}-{unix_timestamp_ms()}" if tracking_id is None else tracking_id
        dryrun_tracking_id = f"{tracking_id}_l_DRYRUN"
        return {
            "clientOrderId": dryrun_tracking_id,
            "orderId": dryrun_tracking_id,
            "transactTime": unix_timestamp_ms(),
            "origQty": contracts,
            "price": limit
        }
    
    def cancel_order_test(self, ticker, order_id) -> dict:
        self._api_cancel_order(ticker=ticker, order_id=order_id, dry_run=True)

    ## PRIVATE METHODS
    
    def _place_market_order(self, ticker:str, action:str, contracts:float, tracking_id = None, dry_run:bool = False) -> dict:
        # if not self._api_ping():
        #     msg = "MEXC API is not available"
        #     logging.error(msg)
        #     raise ValueError(msg)
        if null_or_empty(ticker):
            msg = "ticker is required"
            logging.error(msg)
            raise ValueError(msg)
        if contracts <= 0.0:
            msg = f"Invalid contracts: {contracts}"
            logging.error(msg)
            raise ValueError(msg)
        order_type = "MARKET"
        market_order_ts = unix_timestamp_secs()
        market_order_id = f"FM{market_order_ts}" if tracking_id is None else tracking_id
        action = action.upper()
        market_order_params = self._create_order_params(
            ticker=ticker, 
            action=action, 
            order_type=order_type, 
            contracts=contracts, 
            tracking_id=market_order_id
        )
        results = self._api_place_order(params=market_order_params,dry_run=dry_run)
        results.update({"__ticker": ticker})
        return results
    
    def _place_limit_order(self, ticker: str, action: str, contracts: float, limit: float, broker_params: dict = {}, dry_run:bool = False) -> dict:
        # if not self._api_ping():
        #     msg = "MEXC API is not available"
        #     logging.error(msg)
        #     raise ValueError(msg)
        if null_or_empty(ticker):
            msg = "ticker is required"
            logging.error(msg)
            raise ValueError(msg)
        if contracts <= 0.0:
            msg = f"Invalid contracts: {contracts}"
            logging.error(msg)
            raise ValueError(msg)
        if limit <= 0.0:
            msg = f"Invalid limit: {limit}"
            logging.error(msg)
            raise ValueError(msg)
        action = action.upper()
        if action not in ["BUY", "SELL"]:
            msg = f"Invalid action: {action}"
            logging.error(msg)
            raise ValueError(msg)
        order_type = "LIMIT"
        limit_order_ts = unix_timestamp_ms()
        limit_order_id = f"FM{limit_order_ts}"
        limit_order_params = self._create_order_params(
            ticker=ticker,
            action=action,
            order_type=order_type,
            contracts=contracts,
            tracking_id=limit_order_id,
            target_price=limit
        )
        return self._api_place_order(
            params=limit_order_params, 
            dry_run=dry_run
        )

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
    
    def _api_get_current_prices(self, ticker: str = None) -> dict:
        url = f"{self._cfg_api_endpoint()}/api/v3/ticker/price"
        if not null_or_empty(ticker):
            params = {
                "symbol": ticker,
            }
            headers = self._request_headers()
            response = requests.get(f"{url}", headers=headers, params=params)
        else:
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
    
    def _api_get_open_orders(self, symbol: str) -> dict:
        if null_or_empty(symbol):
            raise ValueError("symbol parameter is required")
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/openOrders"
        remote_server_time = self._timestamp()
        params = {
            "symbol": symbol,
            "timestamp": remote_server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)
        headers = self._request_headers()
        response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"MEXC API get open orders response: {response.status_code} - {response.text}")
        if response.status_code != 200:
            msg = f"Failed to get open orders: {response.status_code} - {response.text}"
            logging.error(msg)
            raise ValueError(msg)
        return response.json()

    def _api_get_orders(self, symbol: str) -> dict:
        if null_or_empty(symbol):
            raise ValueError("symbol parameter is required")
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/allOrders"
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

    def _api_cancel_order(self, ticker: str, order_id: str, dry_run:bool = False) -> dict:
        if null_or_empty(ticker):
            raise ValueError("ticker is required")
        if null_or_empty(order_id):
            raise ValueError("order_id is required")
        if dry_run:
            logging.info(f"Dry run: cancel order {order_id} for {ticker}")
            return { 
                "result": "no action due to DRY RUN mode being set", 
                "ticker": ticker, 
                "order_id": order_id 
            }
        
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
            rx_dict = json.loads(response.text)
            mexc_api_err = ApiErrorResponse(rx_dict)
            if mexc_api_err.code == ApiErrors.ORDER_ALREADY_FILLED.value:
                logging.error(f"Order {order_id} for {ticker} was already filled")
                raise OrderAlreadyFilledError(f"Order {order_id} for {ticker} was already filled")
            else:
                raise ApiError(msg)
        
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
        if order_type not in ["LIMIT", "MARKET", "STOP_LOSS"]:
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
            "newClientOrderId": f"FM{timestamp}" if tracking_id is None else tracking_id,
        }
        
        if order_type in ["LIMIT"]:
            if target_price is None:
                raise ValueError(f"Invalid target price: {target_price}")
            params["timeInForce"] = "GTC"
            params["price"] = f"{target_price:.8f}"
        elif order_type in ["STOP_LOSS"]:
            params["type"] = "MARKET"
            params["stopPrice"] = target_price
            params["price"] = target_price
            
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

    def _api_place_order(self, params: dict, dry_run: bool = False, content_type:str = "application/json") -> dict:
        logging.debug("_api_place_order")
        base_url = self._cfg_api_endpoint()
        order_endpoint = "/api/v3/order/test" if dry_run else "/api/v3/order"

        headers = self._request_headers()
        headers.update({"Content-Type": content_type})

        ticker = params["symbol"]
        quantity = params["quantity"]
        order_type = params["type"]

        target = f"{base_url}{order_endpoint}"

        logging.info(f"MEXC API - {target} - placing {order_type} order for {ticker} with {quantity}. Parameters are: {params}")

        response = requests.post(url=target, headers=headers, params=params)

        if response.status_code != 200:
            msg = f"error in placing order {order_type} for {ticker}. API response: {response.text}"
            logging.error(msg)
            rx_dict = json.loads(response.text)
            mexc_api_err = ApiErrorResponse(rx_dict)
            if mexc_api_err.code == ApiErrors.OVERSOLD.value:
                msg = f"{ticker} - is oversold for quantity {quantity}. All parameters: {params}"
                logging.error(msg)
                raise OversoldError(msg)
            elif mexc_api_err.code == ApiErrors.INVALID_QUANTITY_SCALE.value:
                msg = f"{ticker} - invalid quantity scale {quantity}. Please consider selling manually."
                logging.error(msg)
                raise InvalidQuantityScale(msg)
            else:
                raise ApiError(msg)
        
        logging.info(f"MEXC API response: {response.status_code} - {response.text}")

        if response.status_code != 200:
            msg = f"Error placing MEXC API {order_type} order: {response.status_code} - {response.text}"
            raise ValueError(msg)

        return response.json()
    
    ## TEST

    def _api_get_spot_orders(self, ticker: str = None) -> dict:
        url = f"{self._cfg_api_endpoint()}/api/v3/ticker/price"
        params = {
            "method": "SUBSCRIPTION",
            "params": [
                "spot@private.orders.v3.api.pb"
            ]
        }
        headers = self._request_headers()
        response = requests.get(f"{url}", headers=headers, params=params)
        return response.json()

if __name__ == "__main__":
    import unittest

    class Test(unittest.TestCase):
        def test_e2e(self):
            params = {
                "p1": "v1",
                "p2": "v2",
            }
            path_params = "&".join([f"{k}={v}" for k, v in params.items()])
            path_params = "?" + path_params
            print(path_params)

    unittest.main()

