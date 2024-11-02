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
    return "storageAccountConnectionString"

@app.route(route="merchant_api", 
            auth_level=func.AuthLevel.ANONYMOUS)
def merchant_api(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
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
    body = req.get_body().decode('utf-8')
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

class MarketOrderable(ABC):
    @abstractmethod
    def place_buy_market_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float) -> dict:
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
            "crypto": IBKRClient(),
            "forex": None
        }

    def get_for_security(self, security_type: str) -> MarketOrderable:
        if security_type not in self.__repository:
            raise ValueError(f"security type {security_type} not supported")
        return self.__repository[security_type]
    
def IBKR_ENV_GATEWAY_ENDPOINT():
    return "IBKR_GATEWAY_ENDPOINT" 

def IBKR_ENV_GATEWAY_PASSWD():
    return "IBKR_GATEWAY_PASSWORD"

class IBKRClient(MarketOrderable):

    def place_buy_market_order(self, source: str, ticker: str, contracts: float, limit: float, take_profit: float, stop_loss: float) -> dict:
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

#####################################
#####################################
### Merchant 
#####################################
#####################################

def M_CFG_HIGH_INTERVAL():
    return "MERCHANT_HIGH_INTERVAL"

def M_CFG_LOW_INTERVAL():
    return "MERCHANT_LOW_INTERVAL"

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

def M_BIAS_BULLISH():
    return "bullish"

def M_BIAS_BEARISH():
    return "bearish"

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
        self.notes = msg_body.get("notes", "")
        self.TABLE_NAME = "flowmerchant"

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

        if not isinstance(security["contracts"], int):
            logging.error(f"Contracts must be an integer: {security['contracts']}")
            raise ValueError("Contracts must be an integer")

        try:
            float(flowmerchant["suggested_stoploss"])
            float(flowmerchant["takeprofit_percent"])
            int(flowmerchant["rest_interval_minutes"])
            int(flowmerchant["version"])
        except ValueError as e:
            logging.error(f"Flowmerchant values must be numbers: {e}")
            raise ValueError("Flowmerchant values must be numbers")

        return MerchantSignal(msg_body)

    # Accessor methods for metadata
    def api_token(self):
        return self.metadata.get("key")

    # Accessor methods for security
    def ticker(self):
        return self.security.get("ticker")

    def exchange(self):
        return self.security.get("exchange")

    def security_type(self):
        return self.security.get("type")

    def contracts(self):
        return self.security.get("contracts")

    def interval(self):
        return self.security.get("interval")

    def high(self):
        return float(self.security["price"].get("high"))

    def low(self):
        return float(self.security["price"].get("low"))

    def open(self):
        return float(self.security["price"].get("open"))

    def close(self):
        return float(self.security["price"].get("close"))

    # Accessor methods for flowmerchant
    def suggested_stoploss(self):
        return float(self.flowmerchant.get("suggested_stoploss"))

    def takeprofit_percent(self):
        return float(self.flowmerchant.get("takeprofit_percent"))

    def rest_interval(self):
        return int(self.flowmerchant.get("rest_interval_minutes"))

    def version(self):
        return int(self.flowmerchant.get("version"))

    def action(self):
        return self.flowmerchant.get("action")
    
    def low_interval(self):
        return self.flowmerchant.get("low_interval")
    
    def high_interval(self):
        return self.flowmerchant.get("high_interval")

    def notes(self):
        return self.notes

    def __str__(self) -> str:
        return (
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
        )

    def info(self) -> str:
        return str(self)
    
##
# Merchant

class Merchant:

    def __init__(self, table_service: TableServiceClient, broker: MarketOrderable, events_logger: EventLoggable) -> None:
        logging.debug(f"Merchant()")
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
        """
        currently no properties that need to be loaded from env. 
        env loaded config would be global to all merchant instances.
        so preferrable to config from the signal, unless there are security implications
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
            self._happily_say(self.get_merchant_id(signal), "I'm going shopping - because I am done resting.")
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
            if signal.close() < stop_loss:
                raise ValueError(f"Close price {signal.close()} is less than suggested stoploss {stop_loss}")
            if signal.close() > take_profit:
                raise ValueError(f"Close price {signal.close()} is greater than take profit {take_profit}")

        limit = signal.close()
        take_profit = calculate_take_profit(signal)
        stop_loss = calculate_stop_loss(signal)
        quantity = signal.contracts()
        safety_check(signal.close(), take_profit, stop_loss, quantity)
        result = self.broker.place_buy_market_order(
            source=self.get_merchant_id(signal), 
            ticker=signal.ticker(), 
            contracts=signal.contracts(),
            limit=limit,
            take_profit=take_profit, 
            stop_loss=stop_loss
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

    def create_state(self, id, version, action, ticker, close, suggested_stoploss, high, low, takeprofit_percent, status, high_interval, low_interval):
        return {
            M_STATE_KEY_ID(): id,
            M_STATE_KEY_VERSION(): version,
            M_STATE_KEY_ACTION(): action,
            M_STATE_KEY_TICKER(): ticker,
            M_STATE_KEY_CLOSE(): close,
            M_STATE_KEY_SUGGESTED_STOPLOSS(): suggested_stoploss,
            M_STATE_KEY_HIGH(): high,
            M_STATE_KEY_LOW(): low,
            M_STATE_KEY_TAKEPROFIT_PERCENT(): takeprofit_percent,
            M_STATE_KEY_STATUS(): status,
            M_STATE_KEY_HIGH_INTERVAL(): high_interval,
            M_STATE_KEY_LOW_INTERVAL(): low_interval
        }

    def test_merchant_e2e(self):
        table_client_mock = Mock()        
        table_client_mock.query_entities.return_value = [ ]
        broker_mock = Mock()
        
        # Optionally, you can verify it was called with specific arguments
        # table_client_mock.query_entities.assert_called_with(some_arg1, some_arg2)

        # If you want to check how many times it was called
        # self.assertEqual(table_client_mock.query_entities.call_count, 1)
        signal_data = """
        {
            "action" : "buy",
            "ticker" : "AAPL",
            "key" : "STOCKTON_KEY",
            "notes" : "ver=20240922;high=105.0;low=95.0;exchange=NASDAQ;open=98.0;interval=1h;high_interval=1h;low_interval=1m;suggested_stoploss=0.05;takeprofit_percent=0.05;rest_interval=3000",
            "close" : 103.0,
            "contracts" : 1
        }
        """
        first_signal = MerchantSignal(json.loads(signal_data))
        flow_merchant = Merchant(table_client_mock, broker=broker_mock)
        flow_merchant.handle_market_signal(first_signal)

        table_client_mock.create_table_if_not_exists.assert_called()
        table_client_mock.query_entities.assert_called()

        assert flow_merchant.status() == M_STATE_BUYING()
        assert flow_merchant.last_action_time() > 0
        assert flow_merchant.high_interval() == "1h"
        assert flow_merchant.low_interval() == "1m"

        second_signal_data = """
        {
            "action" : "buy",
            "ticker" : "AAPL",
            "key" : "STOCKTON_KEY",
            "notes" : "ver=20240922;high=105.0;low=95.0;exchange=NASDAQ;open=98.0;interval=1m;high_interval=1h;low_interval=1m;suggested_stoploss=0.05;takeprofit_percent=0.05;rest_interval=3000",
            "close" : 103.0,
            "contracts" : 1
        }
        """
        
        table_client_mock.query_entities.return_value = [
            {
                'PartitionKey': 'AAPL-1m', 
                'RowKey': unix_timestamp(), 
                'position_data': '{}', 
                'status': 'buying', 
                'merchant_lastaction_time': 1726990626, 
                'ticker': 'AAPL', 
                'high_interval': '1h', 
                'low_interval': '1m'
            }
        ]
        print(flow_merchant.state)
        second_signal = MerchantSignal(json.loads(second_signal_data))
        flow_merchant.handle_market_signal(second_signal)
        print(flow_merchant.state)

        assert flow_merchant.status() == M_STATE_SELLING()


        third_signal_data = """
        {
            "action" : "buy",
            "ticker" : "AAPL",
            "key" : "STOCKTON_KEY",
            "notes" : "ver=20240922;high=105.0;low=95.0;exchange=NASDAQ;open=98.0;interval=1m;high_interval=1h;low_interval=1m;suggested_stoploss=0.05;takeprofit_percent=0.05;rest_interval=3000",
            "close" : 103.0,
            "contracts" : 1
        }
        """

        table_client_mock.query_entities.return_value = [
            {
                'PartitionKey': 'AAPL-1m', 
                'RowKey': unix_timestamp(), 
                'position_data': '{}', 
                'status': 'selling', 
                'merchant_lastaction_time': 1727000626, 
                'ticker': 'AAPL', 
                'high_interval': '1h', 
                'low_interval': '1m'
            }
        ]

        third_signal = MerchantSignal(json.loads(third_signal_data))
        flow_merchant.handle_market_signal(third_signal)
        
        assert flow_merchant.status() == M_STATE_SELLING()

        forth_signal_data = """
        {
            "action" : "sell",
            "ticker" : "AAPL",
            "key" : "STOCKTON_KEY",
            "notes" : "ver=20240922;high=105.0;low=95.0;exchange=NASDAQ;open=98.0;interval=1m;high_interval=1h;low_interval=1m;suggested_stoploss=0.05;takeprofit_percent=0.05;rest_interval=3000",
            "close" : 103.0,
            "contracts" : 1
        }
        """

        table_client_mock.query_entities.return_value = [
            {
                'PartitionKey': 'AAPL-1m', 
                'RowKey': unix_timestamp(), 
                'position_data': '{}', 
                'status': 'selling', 
                'merchant_lastaction_time': 1727000626, 
                'ticker': 'AAPL', 
                'high_interval': '1h', 
                'low_interval': '1m'
            }
        ]

        forth_signal = MerchantSignal(json.loads(forth_signal_data))
        flow_merchant.handle_market_signal(forth_signal)
        
        assert flow_merchant.status() == M_STATE_RESTING()


if __name__ == '__main__':
    unittest.main()
