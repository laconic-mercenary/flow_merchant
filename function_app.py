import json
import logging
import os
import uuid
import time

import azure.functions as func
from azure.data.tables import TableServiceClient

app = func.FunctionApp()

#####################################
#####################################
### Merchant API
#####################################
#####################################


def APP_ENV_APITOKEN():
    return "MERCHANT_API_TOKEN"

def APP_ENV_STORAGEACCTCS():
    return "storageAccountConnectionString"signal

@app.route(route="merchant_api", 
            auth_level=func.AuthLevel.ANONYMOUS)
def merchant_api(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    is_valid, result = validate_req(req)
    if not is_valid:
        return result
    try:
        if req.method == "GET":
            return handle_get(req)
        elif req.method == "POST":
            return handle_post(req)
    except Exception as e:
        logging.error(f"error handling market signal - {e}", exc_info=True)
        default_event_logger().log_error("Error", f"error handling market signal, {e}")

def validate_req(req: func.HttpRequest) -> tuple[bool, func.HttpResponse]:
    if req.method not in ["GET", "POST"]:
        return False, func.HttpResponse(f"Invalid operation", status_code=405)
    return True, None
    
def handle_get(req: func.HttpRequest) -> func.HttpResponse:
    if 'health' in req.params:
        return func.HttpResponse("ok", status_code=200)
    elif "transactions" in req.params:
        with TableServiceClient.from_connection_string(os.environ[APP_ENV_STORAGEACCTCS()]) as table_service:
            table_client = table_service.get_table_client(table_name="flowmerchant")
            entities = table_client.list_entities()
            rows = [dict(entity) for entity in entities]
            return func.HttpResponse(json.dumps(rows), mimetype="application/json")
    return func.HttpResponse("not found", status_code=404)

def handle_post(req: func.HttpRequest) -> func.HttpResponse:
    body = req.get_body().decode("utf-8")
    headers = dict(req.headers)
    logging.info(f"received merchant signal: {body}")
    logging.debug(f"headers: {headers}")
    
    with TableServiceClient.from_connection_string(os.environ[APP_ENV_STORAGEACCTCS()]) as table_service:
        message_body = json.loads(body)
        signal = MerchantSignal.parse(message_body)
        if signal.api_token() != os.environ[APP_ENV_APITOKEN()]:
            return func.HttpResponse(f"Unauthorized", status_code=401)
        
        event_logger = default_event_logger()
        event_logger.log_notice("Notice",f"received market signal: {body} - which is {signal.info()}")
        
        broker_repo = BrokerRepository()
        broker = broker_repo.get_for_security(signal.security_type())
        
        merchant = Merchant(table_service, broker, event_logger)
        merchant.handle_market_signal(signal)
        
#####################################
#####################################
### UTILS
#####################################
#####################################

def unix_timestamp() -> int:
    return int(time.time())

#####################################
#####################################
### Event Logger
#####################################
#####################################

from abc import ABC, abstractmethod
import requests
import datetime

def DISCORD_ENV_WEBHOOK_URL():
    return "DISCORD_WEBHOOK_URL"

def DISCORD_COLOR_GREEN():
    return 3066993

def DISCORD_COLOR_RED():
    return 15158332

def DISCORD_COLOR_BLUE():
    return 3447003

class EventLoggable(ABC):
    @abstractmethod
    def log_notice(self, title, message):
        pass
    @abstractmethod
    def log_error(self, title, message):
        pass
    @abstractmethod
    def log_success(self, title, message):
        pass

class DiscordClient(EventLoggable):
    def __init__(self):
        self.base_url = os.environ[DISCORD_ENV_WEBHOOK_URL()]

    def log_notice(self, title, message):
        self.send_message(title, message, DISCORD_COLOR_BLUE())

    def log_error(self, title, message):
        self.send_message(title, message, DISCORD_COLOR_RED())

    def log_success(self, title, message):
        self.send_message(title, message, DISCORD_COLOR_GREEN())

    def send_message(self, title, message, color=DISCORD_COLOR_BLUE()):
        url = f"{self.base_url}"
        if not color in [DISCORD_COLOR_GREEN(), DISCORD_COLOR_RED(), DISCORD_COLOR_BLUE()]:
            color = DISCORD_COLOR_BLUE()
        if title is None or len(title) == 0:
            raise ValueError("title cannot be None")
        if message is None or len(message) == 0:
            raise ValueError("message cannot be None")
        payload = {
            "embeds": [
                {
                    "title": title,
                    "description": message,
                    "color": color,
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                }
            ]
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, json=payload, timeout=7)
        if response.status_code > 302:
            logging.error(f"Failed to send message: {response.text}")

def default_event_logger() -> EventLoggable:
    return DiscordClient()

#####################################
#####################################
### BROKER
#####################################
#####################################

from abc import ABC, abstractmethod

class LiveCapable(ABC):
    
    @abstractmethod
    def update_all_holdings(self) -> dict:
        pass

class OrderCapable(ABC):

    @abstractmethod
    def place_limit_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float, broker_params={}) -> dict:
        pass

    @abstractmethod
    def place_test_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float, broker_params={}) -> dict:
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

class BrokerRepository:
    def __init__(self):
        self.__repository = {
            "stock": IBKRClient(),
            "crypto": MEXCClient(),
            "forex": None
        }

    def get_for_security(self, security_type: str) -> OrderCapable:
        if security_type not in self.__repository:
            raise ValueError(f"security type {security_type} not supported")
        return self.__repository[security_type]
    
##
# IBKR
    
def IBKR_ENV_GATEWAY_ENDPOINT():
    return "IBKR_GATEWAY_ENDPOINT" 

def IBKR_ENV_GATEWAY_PASSWD():
    return "IBKR_GATEWAY_PASSWORD"

class IBKRClient(OrderCapable):

    def place_test_order(self, source, ticker, contracts, limit, take_profit, stop_loss, broker_params={}) -> dict:
        logging.warning(f"IBKR test order - not implemented")

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

##
# MEXC

from urllib.parse import urlencode

import hmac
import hashlib

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

class MEXCClient(OrderCapable):

    def place_test_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float, broker_params={}) -> dict:
        logging.debug("place_test_order")

        result = ""
        result += f">>>> TEST ORDER START"
        result += f"\n>> (BEFORE) Get Orders: {self._api_get_orders(ticker)}"
        result += f"\n>> (BEFORE) Get Account: {self._api_get_account_info()}"

        """ NOTE
        be very careful here about how you interpret gap percent for TP and SL
        If the bias is bullish, then it would be 
        
        >> TP TRIGGER = TP - (TP * TP_GAP_PERCENT)

        FOr example if the current price is $5 and the TP is $7, you want the trigger price to be less than $7
        """
        if "take_profit_gap_percent" not in broker_params:
            raise ValueError("take_profit_gap_percent is required")
        take_profit_trigger = take_profit - (take_profit * broker_params["take_profit_gap_percent"])

        if "stop_loss_gap_percent" not in broker_params:
            raise ValueError("stop_loss_gap_percent is required")
        stop_loss_trigger = stop_loss - (stop_loss * broker_params["stop_loss_gap_percent"])

        result += f"\n>> (BEFORE) Params Listing: ticker={ticker}, qty={contracts}, tp={take_profit}, tp-trigger={take_profit_trigger}, sl={stop_loss}, sl-trigger={stop_loss_trigger}"

        order_result = self._place_advanced_order(
            ticker=ticker, 
            contracts=contracts, 
            limit_price=None,
            take_profit=take_profit, 
            take_profit_trigger=take_profit_trigger,
            stop_loss=stop_loss, 
            stop_loss_trigger=stop_loss_trigger,
            action="BUY",
            dry_run=False
        )
        result += f"\n>> Place Advanced Order: {order_result}"
        result += f"\n>> (AFTER) Get Orders: {self._api_get_orders(ticker)}"
        result += f"\n>> (AFTER) Get Account: {self._api_get_account_info()}"

        time.sleep(5.0)
    
        executed_quantity = float(order_result["market_order"]["executedQty"])
        sell_result = self._api_place_order(
            ticker=ticker,
            contracts=executed_quantity,
            limit_price=None,
            take_profit=take_profit,
            stop_loss=stop_loss,
            action="SELL",
            dry_run=False
        )
        result += f"\n>> Place Sell Order: {sell_result}"
        result += f"\n>> (AFTER SELL) Get Orders: {self._api_get_orders(ticker)}"
        result += f"\n>> (AFTER SELL) Get Account: {self._api_get_account_info()}"
        
        result += f"\n>>>> TEST ORDER END"
        default_event_logger().log_notice(f"MEXC test order overall result: {result}")

    def place_limit_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float, broker_params={}) -> dict:
        logging.debug("place_limit_order")
        """ NOTE
        For now we are placing a market order because MEXC's API doesn't seem to support OCO orders.
        limit_price parameter is ignored and set to None in the subsequent call.
        The source parameter is also not used because we can make API calls directly to MEXC, as opposed to using a 
        middle-man like IBKR.
        """
        logging.info(f"Placing MEXC order for {ticker} with {contracts} contracts")
        if not self._api_ping():
            logging.error(f"MEXC API is not available")
            raise ValueError(f"MEXC API is not available")
        
        return self._place_advanced_order(
            ticker=ticker, 
            contracts=contracts, 
            limit_price=None, 
            take_profit=take_profit, 
            stop_loss=stop_loss, 
            action="BUY",
            dry_run=False
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
    
    def _api_get_current_prices(self, symbols: list) -> dict:
        url = f"{self._cfg_api_endpoint()}/api/v3/ticker/price"
        response = requests.get(url)
        all_prices = response.json()
        symbol_set = set(symbols)
        filtered_prices = [price for price in all_prices if price["symbol"] in symbol_set]
        return filtered_prices

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
        return response["serverTime"]
    
    def _api_stop_limit(self, ticker: str, exit_side: str, quantity: float, price: float, trigger_price: float, order_id: str) -> dict:
        params = self._create_order_params(
            ticker=ticker,
            action=exit_side,
            order_type="LIMIT",
            contracts=quantity,
            target_price=price,
            stop_price=trigger_price,
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
        remote_server_time = self._api_get_server_time()
        params = {
            "timestamp": remote_server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)
        headers = self._request_headers()
        response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"Get account info (response): {response.status_code} - {response.text}")
        if response.status_code != 200:
            logging.error(f"Failed to get account info: {response.status_code} - {response.text}")
            raise ValueError(f"Failed to get account info: {response.status_code} - {response.text}")
        return response.json()
    
    def _api_get_order(self, symbol: str, order_id: str) -> dict:
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/order"
        """ NOTE
        unix_timestamp() was out of sync with the MEXC server and it was getting rejected.
        Check on this later as we don't want to get throttled by the API for too many calls.
        For now just use remote server time.
        """
        remote_server_time = self._api_get_server_time()
        params = {
            "symbol": symbol,
            "orderId": order_id,
            "timestamp": remote_server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)
        headers = self._request_headers()
        response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"Get order status (response): {response.status_code} - {response.text}")
        if response.status_code != 200:
            logging.error(f"Failed to get order status: {response.status_code} - {response.text}")
            raise ValueError(f"Failed to get order status: {response.status_code} - {response.text}")
        return response.json()

    def _api_get_orders(self, symbol: str) -> dict:
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/allOrders"
        """ NOTE
        unix_timestamp() was out of sync with the MEXC server and it was getting rejected.
        Check on this later as we don't want to get throttled by the API for too many calls.
        For now just use remote server time.
        """
        remote_server_time = self._api_get_server_time()
        params = {
            "symbol": symbol,
            "timestamp": remote_server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)
        headers = self._request_headers()
        response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"Get order (response): {response.status_code} - {response.text}")
        if response.status_code != 200:
            logging.error(f"Failed to get orders: {response.status_code} - {response.text}")
            raise ValueError(f"Failed to get orders: {response.status_code} - {response.text}")
        return response.json()
    
    def _api_cancel_all_orders(self, symbol: str) -> dict:
        base_url = self._cfg_api_endpoint()
        endpoint = "/api/v3/openOrders"        
        server_time = self._api_get_server_time()
        
        params = {
            "symbol": symbol,
            "timestamp": server_time,
            "recvWindow": self._cfg_recv_window_ms()
        }
        params["signature"] = self._sign(params)

        headers = self._request_headers()

        response = requests.delete(f"{base_url}{endpoint}", headers=headers, params=params)
        logging.info(f"Delete (response): {response.text}")
        
        if response.status_code == 404:
            logging.warning(f"Orders were not found for {symbol} : {response.text}")
        if response.status_code != 200:
            logging.error(f"Failed to cancel all orders: {response.text}")
            raise ValueError(f"Failed to cancel all orders: {response.text}")
        return response

    def _create_order_params(self, ticker: str, action: str, order_type: str, contracts: float, target_price: float = None, stop_price: float = None, tracking_id: str = None) -> dict:
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
        if tracking_id is None or len(tracking_id) == 0:
            raise ValueError(f"Invalid tracking ID: {tracking_id}")
        
        params = { 
            "symbol": ticker,
            "side": action,
            "type": order_type,
            "quantity": str(contracts),
            "timestamp": self._api_get_server_time(),
            "recvWindow": self._cfg_recv_window_ms(),
            "newClientOrderId": f"{ticker}_{unix_timestamp()}" if tracking_id is None else tracking_id,
        }
            
        if order_type in ["LIMIT"]:
            if target_price is None:
                raise ValueError(f"Invalid target price: {target_price}")
            params["timeInForce"] = "GTC"
            params["price"] = f"{target_price:.8f}"
            if stop_price is not None:
                params["stopPrice"] = f"{stop_price:.8f}"

        params["signature"] = self._sign(params)
        
        return params

    def _api_place_order(self, params: dict, dry_run: bool = True) -> dict:
        logging.debug("_place_order")
        base_url = self._cfg_api_endpoint()
        order_endpoint = "/api/v3/order/test" if dry_run else "/api/v3/order"
        ticker = params["symbol"]
        quantity = params["quantity"]
        order_type = params["type"]

        headers = self._request_headers()

        logging.info(f"Placing {order_type} order for {ticker} with {quantity}. Parameters are: {params}")

        response = requests.post(f"{base_url}{order_endpoint}", headers=headers, params=params)
        
        logging.info(f"API response: {response.status_code} - {response.text}")

        if response.status_code != 200:
            logging.error(f"Error placing market order: {response.status_code} - {response.text}")
            raise ValueError(f"Error placing market order: {response.status_code} - {response.text}")

        return response.json()
    
    def _place_advanced_order(self, ticker: str, action: str, contracts: float, limit_price: float, stop_loss: float, stop_loss_trigger: float, take_profit: float, take_profit_trigger: float, dry_run: bool = True) -> dict:
        logging.debug("_place_advanced_order")
        market_order_id = f"{ticker}_{unix_timestamp()}"
        market_order_params = self._create_order_params(
            ticker=ticker, 
            action=action, 
            order_type="MARKET", 
            contracts=contracts, 
            tracking_id=market_order_id
        )        
        market_order = self._api_place_order(market_order_params, dry_run)
        market_order = self._api_get_order(ticker, market_order["orderId"])
        
        """ NOTE
        the executed quantity is seldom the contracts amount when placing market orders. 
        The only way to know the actual amount is to query the order.
        """
        executed_qty = contracts
        if "executedQty" not in market_order:
            logging.warning(f"executedQty key not found in order response, will default to {contracts}. Order response: {market_order}")
        else:
            executed_qty = float(market_order.get("executedQty"))

        exit_side = "SELL" if action.upper() == "BUY" else "BUY"

        """ NOTE
        At this point our market order is likely filled, so we need to decide what to do if TP and SL orders fail. 
        Again - this is due to the API limitation of not using OCO orders.
        Option 1 - Sell right away at a potential small loss
        Option 2 - Leave the market order open and retry.
        
        Currently will do the following
        1. Attempt to place a stop loss order
        2. If it fails, pause for a bit
        3. Retry order again
        4. If it fails, sell all and exit
        """
        try:
            stop_loss_order = self._api_stop_limit(
                ticker=ticker,
                exit_side=exit_side,
                price=stop_loss,
                trigger_price=stop_loss_trigger,
                quantity=executed_qty,
                order_id=f"{market_order_id}_s"
            )

        # stop_loss_order_id = f"{market_order_id}_s"
        # stop_loss_params = self._create_order_params(
        #     ticker=ticker,
        #     action=exit_side,
        #     order_type="STOP_LIMIT",
        #     stop_price=stop_loss_trigger,
        #     contracts=executed_qty,
        #     target_price=stop_loss,
        #     tracking_id=stop_loss_order_id
        # )

        # stop_loss_order = None
        # try:
        #     stop_loss_order = self._api_place_order(stop_loss_params, dry_run)
        except Exception as e:
            logging.warning(f"Error placing stop loss order: {e}.", stack_info=True)
            pause_period_seconds = 15
            logging.info(f"Pausing for {pause_period_seconds} second(s) before retrying the stop loss order")
            time.sleep(pause_period_seconds)
            try:
                stop_loss_order = self._api_stop_limit(
                    ticker=ticker,
                    exit_side=exit_side,
                    price=stop_loss,
                    trigger_price=stop_loss_trigger,
                    quantity=executed_qty,
                    order_id=f"{market_order_id}_s2"
                )
            except Exception as e:
                logging.error(f"Retry failed when placing stop loss order: {e}. Sell {executed_qty} for {ticker} immediately")
                params = self._create_order_params(
                    ticker=ticker,
                    action=exit_side,
                    order_type="MARKET",
                    contracts=executed_qty,
                    tracking_id=f"{market_order_id}_s_err"
                )
                self._api_place_order(params, dry_run)
                raise e

        """NOTE
        For take profit, dont bother with the retry logic. Just try once and if it fails, sell all and exit.
        Also remebering to cancel the stop order.
        """
        try:
            take_profit_order = self._api_stop_limit(
                ticker=ticker,
                exit_side=exit_side,
                price=take_profit,
                trigger_price=take_profit_trigger,
                quantity=executed_qty,
                order_id=f"{market_order_id}_t"
            )
        # take_profit_order_id = f"{market_order_id}_t"
        # take_profit_params = self._create_order_params(
        #     ticker=ticker, 
        #     action=exit_side, 
        #     order_type="STOP_LIMIT", 
        #     contracts=executed_qty,
        #     stop_price=take_profit_trigger, 
        #     target_price=take_profit, 
        #     tracking_id=take_profit_order_id
        # )
        # try:
        #     take_profit_order = self._api_place_order(take_profit_params, dry_run)
        except Exception as e:
            logging.error(f"Error placing take profit order: {e}. Sell {executed_qty} for {ticker} immediately")
            params = self._create_order_params(
                ticker=ticker,
                action=exit_side,
                order_type="MARKET",
                contracts=executed_qty,
                target_price=None,
                tracking_id=f"{market_order_id}_t_err"
            )
            self._api_place_order(params, dry_run)
            self._api_cancel_all_orders(ticker)
            raise e

        return {
            "market_order": market_order,
            "take_profit_order": take_profit_order,
            "stop_loss_order": stop_loss_order
        }


#####################################
#####################################
### Merchant 
#####################################
#####################################

def S_ACTION_BUY():
    return "buy"

def S_ACTION_SELL():
    return "sell"

##
# Keys that are stored in the merchant state (Azure Storage)

def M_STATE_SHOPPING():
    return "shopping"

def M_STATE_BUYING():
    return "buying"

def M_STATE_SELLING():
    return "selling"

def M_STATE_RESTING():
    return "resting"

def M_STATE_KEY_PARTITIONKEY():
    return "PartitionKey"

def M_STATE_KEY_ROWKEY():
    return "RowKey"

def M_STATE_KEY_STATUS():
    return "status"

def M_STATE_KEY_POSITION_DATA():
    return "position_data"

def M_STATE_KEY_LAST_ACTION_TIME():
    return "merchant_lastaction_time"

def M_STATE_KEY_TICKER():
    return "ticker"

def M_STATE_KEY_INTERVAL():
    return "interval"

def M_STATE_KEY_REST_INTERVAL():
    return "rest_interval_minutes"

def M_STATE_KEY_HIGH_INTERVAL():
    return "high_interval"

def M_STATE_KEY_LOW_INTERVAL():
    return "low_interval"

def M_STATE_KEY_ID():
    return "id"

def M_STATE_KEY_VERSION():
    return "version"

def M_STATE_KEY_ACTION():
    return "action"

def M_STATE_KEY_CLOSE():
    return "close"

def M_STATE_KEY_SUGGESTED_STOPLOSS():
    return "suggested_stoploss"

def M_STATE_KEY_HIGH():
    return "high"

def M_STATE_KEY_LOW():
    return "low"

def M_STATE_KEY_TAKEPROFIT_PERCENT():
    return "takeprofit_percent"

def M_STATE_KEY_MERCHANT_ID():
    return "merchant_id"

##
# Keys found in the trading view alerts JSON

import uuid
import logging

class MerchantSignal:

    def __init__(self, msg_body):
        if not msg_body:
            raise ValueError("Message body cannot be null")
        self.msg = msg_body
        self.metadata = msg_body.get("metadata", {})
        self.security = msg_body.get("security", {})
        self.flowmerchant = msg_body.get("flowmerchant", {})
        self._notes = msg_body.get("notes", "")
        self.TABLE_NAME = "flowmerchant"
        self.__id = str(uuid.uuid4())

    @staticmethod
    def parse(msg_body):
        if not msg_body:
            raise ValueError("Message body cannot be null")

        # Validate metadata
        metadata = msg_body.get("metadata")
        if not metadata:
            logging.error("Metadata is missing")
            raise ValueError("Metadata is required")
        if "key" not in metadata:
            logging.error("API key is missing in metadata")
            raise ValueError("API key is required in metadata")

        # Validate security
        security = msg_body.get("security")
        if not security:
            logging.error("Security information is missing")
            raise ValueError("Security information is required")
        required_security_keys = ["ticker", "exchange", "type", "contracts", "interval", "price"]
        for key in required_security_keys:
            if key not in security:
                logging.error(f"Missing required security key: {key}")
                raise ValueError(f"Missing required security key: {key}")

        # Validate price
        price = security.get("price")
        if not price:
            logging.error("Price information is missing in security")
            raise ValueError("Price information is required in security")
        required_price_keys = ["high", "low", "open", "close"]
        for key in required_price_keys:
            if key not in price:
                logging.error(f"Missing required price key: {key}")
                raise ValueError(f"Missing required price key: {key}")

        # Validate flowmerchant
        flowmerchant = msg_body.get("flowmerchant")
        if not flowmerchant:
            logging.error("Flowmerchant information is missing")
            raise ValueError("Flowmerchant information is required")
        required_flowmerchant_keys = ["suggested_stoploss", "takeprofit_percent", "rest_interval_minutes", "version", "action"]
        for key in required_flowmerchant_keys:
            if key not in flowmerchant:
                logging.error(f"Missing required flowmerchant key: {key}")
                raise ValueError(f"Missing required flowmerchant key: {key}")

        # Validate action
        if flowmerchant["action"] not in ["buy", "sell"]:
            logging.error(f"Invalid action: {flowmerchant['action']}")
            raise ValueError("Invalid action")

        # Validate data types
        try:
            float(security["price"]["high"])
            float(security["price"]["low"])
            float(security["price"]["open"])
            float(security["price"]["close"])
        except ValueError as e:
            logging.error(f"Price values must be numbers: {e}")
            raise ValueError("Price values must be numbers")

        if not isinstance(security["contracts"], float):
            if not isinstance(security["contracts"], int):
                logging.error(f"Contracts must be numeric: {security['contracts']}")
                raise ValueError("Contracts must be numeric")
        
        if security["contracts"] <= 0.0:
            logging.error(f"Contracts must be greater than 0: {security['contracts']}")
            raise ValueError("Contracts must be greater than 0")

        if not isinstance(flowmerchant["version"], int):
            logging.error(f"Version must be an integer: {flowmerchant['version']}")
            raise ValueError("Version must be an integer")
        
        try:
            float(flowmerchant["suggested_stoploss"])
            float(flowmerchant["takeprofit_percent"])
            int(flowmerchant["rest_interval_minutes"])
        except ValueError as e:
            logging.error(f"Flowmerchant values must be numbers: {e}")
            raise ValueError("Flowmerchant values must be numbers")

        return MerchantSignal(msg_body)

    # Accessor methods for metadata
    def api_token(self) -> str:
        return self.metadata.get("key")

    # Accessor methods for security
    def ticker(self) -> str:
        return self.security.get("ticker")

    def exchange(self) -> str:
        return self.security.get("exchange")

    def security_type(self) -> str:
        return self.security.get("type")

    def contracts(self) -> int:
        return self.security.get("contracts")

    def interval(self) -> str:
        return self.security.get("interval")

    def high(self) -> float:
        return float(self.security["price"].get("high"))

    def low(self) -> float:
        return float(self.security["price"].get("low"))

    def open(self) -> float:
        return float(self.security["price"].get("open"))

    def close(self) -> float:
        return float(self.security["price"].get("close"))

    # Accessor methods for flowmerchant
    def suggested_stoploss(self) -> float:
        return float(self.flowmerchant.get("suggested_stoploss"))

    def takeprofit_percent(self) -> float:
        return float(self.flowmerchant.get("takeprofit_percent"))

    def rest_interval(self) -> int:
        return int(self.flowmerchant.get("rest_interval_minutes"))

    def version(self) -> int:
        return int(self.flowmerchant.get("version"))

    def action(self) -> str:
        return self.flowmerchant.get("action")
    
    def low_interval(self) -> str:
        return self.flowmerchant.get("low_interval")
    
    def high_interval(self) -> str:
        return self.flowmerchant.get("high_interval")

    def rest_after_buy(self) -> bool:
        self.flowmerchant.get("rest_after_buy", False)
        
    def dry_run(self) -> bool:
        return bool(self.flowmerchant.get("dry_run", False))
        
    def notes(self) -> str:
        return self._notes
    
    def id(self) -> str:
        return self.__id

    def broker_params(self) -> dict:
        return self.flowmerchant.get("broker_params", {})

    def __str__(self) -> str:
        return (
            f"MerchantSignal(id={self.id()}, "
            f"action={self.action()}, "
            f"ticker={self.ticker()}, "
            f"close={self.close()}, "
            f"interval={self.interval()}, "
            f"suggested_stoploss={self.suggested_stoploss()}, "
            f"high={self.high()}, "
            f"low={self.low()}, "
            f"takeprofit_percent={self.takeprofit_percent()}, "
            f"contracts={self.contracts()}, "
            f"version={self.version()}, "
            f"rest_interval={self.rest_interval()}"
            f"rest_after_buy={self.rest_after_buy()}, "
            f"notes={self.notes()}"
            f")"
        )

    def info(self) -> str:
        return str(self)
    
##
# Merchant

class Merchant:

    def __init__(self, table_service: TableServiceClient, broker: OrderCapable, events_logger: EventLoggable) -> None:
        if table_service is None:
            raise ValueError("TableService cannot be null")
        if broker is None:
            raise ValueError("Broker cannot be null")
        if events_logger is None:
            raise ValueError("EventsLogger cannot be null")
        self.state = None
        self.table_service = table_service
        self.broker = broker
        self.events_logger = events_logger
        self.TABLE_NAME = "flowmerchant"
        table_service.create_table_if_not_exists(table_name=self.TABLE_NAME)

    def handle_market_signal(self, signal: MerchantSignal) -> None:
        logging.debug(f"handle_market_signal() - {signal.id()}")
        logging.info(f"received signal - id={signal.id()} - {signal.info()}")
        handled = False
        try:
            self.load_config_from_signal(signal)
            self.load_config_from_env() # env should override signal configs
            self.load_state_from_storage(signal)
            if self.status() == M_STATE_SHOPPING():
                handled = self._handle_signal_when_shopping(signal)
            elif self.status() == M_STATE_BUYING():
                handled = self._handle_signal_when_buying(signal)
            elif self.status() == M_STATE_SELLING():
                handled = self._handle_signal_when_selling(signal)
            elif self.status() == M_STATE_RESTING():
                handled = self._handle_signal_when_resting(signal)
            else:
                raise ValueError(f"Unknown state {self.status()}")
        finally:
            if not handled:
                self._say(self.get_merchant_id(signal), f"Nothing for me to do, I'm in {self.state[M_STATE_KEY_STATUS()]} mode")
            logging.info(f"finished handling signal - id={signal.id()}")
    
    def load_state_from_storage(self, signal: MerchantSignal) -> None:
        logging.debug(f"load_state_from_storage()")
        merchant_id = self.get_merchant_id(signal)
        query_filter = f"{M_STATE_KEY_MERCHANT_ID()} eq '{merchant_id}'"
        client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        rows = list(client.query_entities(query_filter))
        if len(rows) > 1:
            raise ValueError(f"Multiple open merchants found for {merchant_id}")
        else:
            if len(rows) == 1:
                logging.info(f"found existing merchant - id={merchant_id}")
                row = rows[0]
                current_state = row[M_STATE_KEY_STATUS()]
                if not current_state in [M_STATE_SHOPPING(), M_STATE_BUYING(), M_STATE_SELLING(), M_STATE_RESTING()]:
                    raise ValueError(f"Unknown state found in storage {current_state}")
                self.state[M_STATE_KEY_STATUS()] = current_state
                self.state[M_STATE_KEY_PARTITIONKEY()] = row[M_STATE_KEY_PARTITIONKEY()]
                self.state[M_STATE_KEY_ROWKEY()] = row[M_STATE_KEY_ROWKEY()]
                self.state[M_STATE_KEY_LAST_ACTION_TIME()] = row[M_STATE_KEY_LAST_ACTION_TIME()]
                self.state[M_STATE_KEY_VERSION()] = row[M_STATE_KEY_VERSION()]
            else:
                logging.info(f"no open merchants found for {merchant_id}, creating new...")
                self.state[M_STATE_KEY_STATUS()] = M_STATE_SHOPPING()
                client.create_entity(entity=self.state)
                self._happily_say(self.get_merchant_id(signal), f"I'm the new guy! Time to go shopping for {signal.ticker()}")

    def load_config_from_env(self) -> None:
        """ NOTE
        currently no properties that need to be loaded from env. 
        env loaded config would be global to all merchant instances.
        so preferrable to put config in the signal, unless there are security implications
        """
        logging.debug(f"load_config_from_env()")

    def load_config_from_signal(self, signal: MerchantSignal) -> None:
        logging.debug(f"load_config_from_signal()")
        if self.state is None:
            self.state = {}
        self.state[M_STATE_KEY_PARTITIONKEY()] = self.get_merchant_id(signal)
        self.state[M_STATE_KEY_ROWKEY()] = f"stockton-{signal.ticker()}-{signal.id()}"
        self.state[M_STATE_KEY_ID()] = signal.id()
        self.state[M_STATE_KEY_MERCHANT_ID()] = self.get_merchant_id(signal)
        self.state[M_STATE_KEY_VERSION()] = signal.version()
        self.state[M_STATE_KEY_TICKER()] = signal.ticker()
        self.state[M_STATE_KEY_CLOSE()] = signal.close()
        self.state[M_STATE_KEY_SUGGESTED_STOPLOSS()] = signal.suggested_stoploss()
        self.state[M_STATE_KEY_HIGH()] = signal.high()
        self.state[M_STATE_KEY_LOW()] = signal.low()
        self.state[M_STATE_KEY_TAKEPROFIT_PERCENT()] = signal.takeprofit_percent()
        ## self.state[M_STATE_KEY_STATE()] = M_STATE_SHOPPING()
        self.state[M_STATE_KEY_HIGH_INTERVAL()] = signal.high_interval()
        self.state[M_STATE_KEY_LOW_INTERVAL()] = signal.low_interval()
        self.state[M_STATE_KEY_REST_INTERVAL()] = signal.rest_interval()
        self.state[M_STATE_KEY_LAST_ACTION_TIME()] = unix_timestamp()
        
    def _handle_signal_when_shopping(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_shopping()")
        if signal.low_interval() == signal.high_interval():
            ### if low and high are the same, then we assume we're ok with just buying a single trading view alert
            ### in this case we go straight to buying (shopping -> buying is for confluence)
            logging.warning(f"low and high are the same, going straight to buying")
            self._start_buying()
            self._handle_signal_when_buying(signal)
            self._say(self.get_merchant_id(signal), f"Without confluence, I'm looking to buy {signal.contracts()} of {signal.ticker()}, will let you know when I make a purchase")
            return True
        else:
            if signal.interval() == self.high_interval():
                if signal.action() == S_ACTION_BUY():
                    self._start_buying()
                    self._say(self.get_merchant_id(signal), f"With confluence, I'm looking to buy {signal.contracts()} of {signal.ticker()}, will let you know when I make a purchase")
                    return True
        return False

    def _handle_signal_when_buying(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_buying()")
        if signal.interval() == self.low_interval():
            if signal.action() == S_ACTION_BUY():
                self._place_order(signal)
                if signal.rest_after_buy():
                    self._start_resting()
                else:    
                    self._start_selling()
                self._happily_say(self.get_merchant_id(signal), f"I'm looking to sell my {signal.ticker()}, because I made a purchase!")
                return True
        elif signal.interval() == self.high_interval():
            if signal.action() == S_ACTION_SELL():
                self._start_shopping()
                self._say(self.get_merchant_id(signal), "I'm going shopping - because the high_interval triggered a SELL signal - better safe than sorry")
                return True
        return False
    
    def _handle_signal_when_selling(self, signal: MerchantSignal) -> bool:
        ### what to do here? just allow tne take profits and stop loss to trigger
        ### at least for now. This will become useful later when we include bearish and bullish bias
        logging.debug(f"_handle_signal_when_selling()")
        if signal.action() == S_ACTION_SELL():
            ## do nothing - allow take profit and stop loss to trigger
            self._start_resting()
            self._say(self.get_merchant_id(signal), f"Good night - I'm resting for {signal.rest_interval()} minutes")
            return True
        return False

    def _handle_signal_when_resting(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_resting()")
        now_timestamp_ms = unix_timestamp()
        rest_interval_ms = self.rest_interval_minutes() * 60 * 1000
        if (now_timestamp_ms > self.last_action_time() + rest_interval_ms):
            self._start_shopping()
            self._happily_say(self.get_merchant_id(signal), "Finished my rest - I am going shopping.")
        else:
            time_left_in_seconds = now_timestamp_ms - (self.last_action_time() + rest_interval_ms)
            time_left_in_seconds = time_left_in_seconds / 1000.0
            logging.info(f"Resting for another {time_left_in_seconds} seconds")
            self._say(self.get_merchant_id(signal), f"I'm resting for another {time_left_in_seconds} seconds")
        return True

    def _start_buying(self) -> None:
        logging.debug(f"_start_buying()")
        self.state[M_STATE_KEY_STATUS()] = M_STATE_BUYING()
        self.state[M_STATE_KEY_LAST_ACTION_TIME()] = unix_timestamp()
        self._sync_with_storage()

    def _start_shopping(self) -> None:
        logging.debug(f"_start_shopping()")
        self.state[M_STATE_KEY_STATUS()] = M_STATE_SHOPPING()
        self.state[M_STATE_KEY_LAST_ACTION_TIME()] = unix_timestamp()
        self._sync_with_storage()

    def _start_selling(self) -> None:
        logging.debug(f"_start_selling()")
        self.state[M_STATE_KEY_STATUS()] = M_STATE_SELLING()
        self.state[M_STATE_KEY_LAST_ACTION_TIME()] = unix_timestamp()
        self._sync_with_storage()

    def _start_resting(self) -> None:
        logging.debug(f"_start_resting()")
        self.state[M_STATE_KEY_STATUS()] = M_STATE_RESTING()
        self.state[M_STATE_KEY_LAST_ACTION_TIME()] = unix_timestamp()
        self._sync_with_storage()

    def _sync_with_storage(self) -> None:
        logging.debug(f"_sync_with_storage()")
        client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        logging.info(f"persisting the following state to storage: {self.state}")
        client.update_entity(entity=self.state)

    def _place_order(self, signal: MerchantSignal) -> None:
        logging.debug(f"_place_order()")

        def calculate_take_profit(signal: MerchantSignal) -> float:
            return signal.close() + (signal.close() * signal.takeprofit_percent())
        
        def calculate_stop_loss(signal: MerchantSignal) -> float:
            ## subjective ... may change based on evolving experience
            if signal.suggested_stoploss() > 0.3:
                raise ValueError(f"Suggested stoploss {signal.suggested_stoploss()} is greater than 30%")
            return signal.close() - (signal.close() * signal.suggested_stoploss())

        def safety_check(close, take_profit, stop_loss, quantity) -> None:
            if close < 0.0:
                raise ValueError(f"Close price {close} is less than 0.0")
            if take_profit < 0.0:
                raise ValueError(f"Take profit {take_profit} is less than 0.0")
            if stop_loss < 0.0:
                raise ValueError(f"Stop loss {stop_loss} is less than 0.0")
            if quantity <= 0.0:
                raise ValueError(f"Quantity {quantity} is less than or eq 0")
            if close < stop_loss:
                raise ValueError(f"Close price {close} is less than suggested stoploss {stop_loss}")
            if close > take_profit:
                raise ValueError(f"Close price {close} is greater than take profit {take_profit}")

        limit = signal.close()
        take_profit = calculate_take_profit(signal)
        stop_loss = calculate_stop_loss(signal)
        quantity = signal.contracts()
        safety_check(signal.close(), take_profit, stop_loss, quantity)
        
        execute_order = self.broker.place_test_order if signal.dry_run() else self.broker.place_limit_order
        result = execute_order(
            source=self.get_merchant_id(signal), 
            ticker=signal.ticker(), 
            contracts=signal.contracts(),
            limit=limit,
            take_profit=take_profit, 
            stop_loss=stop_loss,
            broker_params=signal.broker_params()
        )
        self._happily_say(self.get_merchant_id(signal), f"Will send the following order info to the broker: {result}")
        
    def _happily_say(self, merchant_id: str, message: str) -> None:
        logging.debug(f"_happily_say()")
        self._say(merchant_id, message, "happy")

    def _sadly_say(self, merchant_id: str, message: str) -> None:
        logging.debug(f"_sadly_say()")
        self._say(merchant_id, message, "sad")
    
    def _say(self, merchant_id: str, message: str, emotion: str="normal") -> None:
        logging.debug(f"_say()")
        title  = f"Robot-#{merchant_id}"
        if emotion == "happy":
            self.events_logger.log_success(title, message)
        elif emotion == "sad":
            self.events_logger.log_error(title, message)
        else:
            self.events_logger.log_notice(title, message)

    ## properties
    def get_merchant_id(self, signal: MerchantSignal) -> str:
        return f"{signal.ticker()}-{signal.low_interval()}-{signal.high_interval()}-{signal.version()}"

    def status(self) -> str:
        return self.state.get(M_STATE_KEY_STATUS())
    
    def high_interval(self) -> str:
        ## this should come from merchant config
        return self.state.get(M_STATE_KEY_HIGH_INTERVAL())
    
    def low_interval(self) -> str:
        ## this should come from merchant config
        return self.state.get(M_STATE_KEY_LOW_INTERVAL())
        
    def last_action_time(self) -> int:
        ## this should come from storage
        return int(self.state.get(M_STATE_KEY_LAST_ACTION_TIME()))
    
    def rest_interval_minutes(self) -> int:
        ## this should come from merchant config
        return int(self.state.get(M_STATE_KEY_REST_INTERVAL()))
    
#####################################
#####################################
### Tests
#####################################
#####################################

import unittest
from unittest.mock import Mock, patch

class TestFlowMerchant(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
