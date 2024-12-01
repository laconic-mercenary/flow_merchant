import copy
import json
import logging
import eventkit
import uuid

from azure.data.tables import TableServiceClient

from events import EventLoggable
from live_capable import LiveCapable
from merchant_signal import MerchantSignal
from order_capable import OrderCapable
from transactions import calculate_stop_loss_from_signal, calculate_take_profit_from_signal, safety_check, calculate_stop_loss, calculate_take_profit
from utils import unix_timestamp_secs, unix_timestamp_ms

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

def M_STATE_KEY_SUGGESTED_STOPLOSS():
    return "suggested_stoploss"

def M_STATE_KEY_TAKEPROFIT_PERCENT():
    return "takeprofit_percent"

def M_STATE_KEY_MERCHANT_ID():
    return "merchant_id"

def M_STATE_KEY_BROKER_DATA():
    return "broker_data"

def TABLE_NAME():
    return "flowmerchant"

class Merchant:

    def __init__(self, table_service: TableServiceClient, broker: OrderCapable) -> None:
        if table_service is None:
            raise ValueError("TableService cannot be null")
        if broker is None:
            raise ValueError("Broker cannot be null")
        
        self._id = None
        self.state = None
        self.table_service = table_service
        self.broker = broker
        
        self.TABLE_NAME = TABLE_NAME()
        table_service.create_table_if_not_exists(table_name=self.TABLE_NAME)

        self.on_signal_received = eventkit.Event("on_signal_received")
        self.on_state_change = eventkit.Event("on_state_change")
        self.on_order_placed = eventkit.Event("on_order_placed")
        self.on_positions_check = eventkit.Event("on_positions_check")

    ### Positions

    def check_positions(self) -> dict:
        logging.debug(f"_check_positions()")
        start_time_ms = unix_timestamp_ms()
        if not isinstance(self.broker, LiveCapable):
            logging.warning("Broker is not LiveCapable - will skip checking positions")
            return { }
        
        current_positions = self._query_current_positions()
        logging.info(f"Will check the following positions {current_positions}")

        """ NOTE
        it is easier to just get all the prices up front than query individually
        This will save API calls as well. 
        """
        tickers = [ position[M_STATE_KEY_TICKER()] for position in current_positions if M_STATE_KEY_TICKER() in position ]
        tickers.sort()
        
        current_prices = self.broker.get_current_prices(symbols=tickers)
        
        check_results = self._check_profitable_positions(positions=current_positions, current_prices=current_prices)

        """ TODO
        Need a way to get rid of old stored positions. 
        TAKE PROFIT - simple, just delete here.
        STOP LOSS - tricky because these are limit orders.. conisder:
        go through all stop loss limit orders and if they are FILLED, then they 
        triggered the stop loss. Delete the entries from the table service.
        """

        results = {
            "monitored_tickers": tickers,
            "current_positions": check_results,
            "elapsed_ms": unix_timestamp_ms() - start_time_ms
        }

        self.on_positions_check.emit(results)

        return results
    
    def _check_profitable_positions(self, positions: list, current_prices: dict) -> dict:
        logging.debug("_check_profitable_positions()")
        laggards = []
        winners = []
        losers = []
        leaders = []
        
        for position in positions:
            if M_STATE_KEY_TICKER() not in position:
                raise ValueError(f"critical key {M_STATE_KEY_TICKER()} not in stored position {position}")
            if M_STATE_KEY_BROKER_DATA() not in position:
                raise ValueError(f"critical key {M_STATE_KEY_BROKER_DATA()} not in stored position {position}")
            if M_STATE_KEY_TAKEPROFIT_PERCENT() not in position:
                raise ValueError(f"critical key {M_STATE_KEY_TAKEPROFIT_PERCENT()} not in stored position {position}")

            order_ticker = position.get(M_STATE_KEY_TICKER())
            order_list = json.loads(position.get(M_STATE_KEY_BROKER_DATA()))

            for order_data in order_list:
                if "orders" not in order_data:
                    raise ValueError(f"expected key orders to be in {order_data}")
                if "main" not in order_data.get("orders"):
                    raise ValueError(f"expected key main to be in {order_data}")
                if "stop_loss" not in order_data.get("orders"):
                    raise ValueError(f"expected key stop_loss to be in {order_data}")
                
                main_order_id = order_data["orders"]["main"].get("id")
                
                """ TODO - see if there is a way to get all orders in one call, then filter """
                main_order = self.broker.get_order(ticker=order_ticker, order_id=main_order_id)
                
                if not main_order.get("ready"):
                    """ NOTE - this is problematic but may be ok to just wait to the next round
                    with the risk that we miss a price movement
                    """
                    raise ValueError(f"order is not ready yet: {main_order}")
                
                if order_ticker not in current_prices:
                    raise ValueError(f"ticker is missing from current prices list {order_ticker} - {current_prices}")

                order_price = float(main_order.get("price"))
                order_contracts = float(main_order.get("contracts"))
                current_price = float(current_prices.get(order_ticker))

                """ NOTE 
                These all assume a bullish position, but it would be good to handle the 
                shorting case as well.
                """

                order_data.update({ "current_price": current_price })
                
                if current_price < order_price:
                    stop_loss_percent = float(position[M_STATE_KEY_SUGGESTED_STOPLOSS()])
                    stop_loss = calculate_stop_loss(close_price=order_price, stop_loss_percent=stop_loss_percent)

                    if stop_loss > current_price:
                        """ TODO 
                        Use the actual stop loss orders instead of this for reliability
                        We need to remove them at some point and the result of the stop loss order
                        is probably the most reliable way. 
                        """
                        losers.append(order_data)
                        self._handle_stop_loss(
                            position=position,
                            order_data=order_data,
                            ticker=order_ticker,
                            contracts=order_contracts
                        )
                    else:
                        laggards.append(order_data)
                else:
                    take_profit_percent = float(position[M_STATE_KEY_TAKEPROFIT_PERCENT()])
                    take_profit = calculate_take_profit(close_price=order_price, take_profit_percent=take_profit_percent)

                    if current_price >= take_profit:
                        winners.append(order_data)
                        self._handle_take_profit(
                            position=position,
                            order_data=order_data,
                            ticker=order_ticker,
                            contracts=order_contracts
                        )
                    else:
                        leaders.append(order_data)

        logging.warning(f"Beware - the following are behind {laggards}")
        logging.info(f"Take Heart - the following are going well {leaders}")
        logging.info(f"Rejoice - the following are winners {winners}")

        return {
            "winners": winners,
            "laggards": laggards,
            "leaders": leaders,
            "losers": losers
        }

    def _query_current_positions(self) -> list: 
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        return list(table_client.list_entities())
    
    def _handle_stop_loss(self, position:dict, order_data:dict, ticker:str, contracts:float) -> None:
        logging.warning(f"Stop loss reached for {ticker}")
        order_list:list = json.loads(position[M_STATE_KEY_BROKER_DATA()])
        if len(order_list) > 1:
            logging.info(f"multiple positions exist for ticker {ticker} ({len(order_list)}), will remove id {order_data['id']}")
            new_order_list = [ stored_order_data for stored_order_data in order_list if stored_order_data["id"] != order_data["id"] ]
            self._update_broker_data(position=position, new_order_list=new_order_list)
        else:
            self._delete_stored_position(stored_position=position)

    def _handle_take_profit(self, position:dict, order_data:dict, ticker:str, contracts:float) -> None:
        stoploss_order_id = order_data["orders"]["stop_loss"].get("id")
        
        """ TODO - use the batchOrder instead to avoid API limits, but it would be broker specific... """
        logging.info(f"Take profit reached for {ticker} - will cancel the stop loss order and SELL {contracts} of the asset")
        cancel_result = self.broker.cancel_order(ticker=ticker, order_id=stoploss_order_id)
        
        """ TODO - if this fails then we are in trouble because our stop loss is gone, consider a retry mechanism """
        sell_result = self.broker.place_sell_order(ticker=ticker, contracts=contracts)
        
        order_list:list = json.loads(position[M_STATE_KEY_BROKER_DATA()])
        if len(order_list) > 1:
            logging.info(f"multiple positions exist for ticker {ticker} ({len(order_list)}), will remove id {order_data['id']}")
            ## this is easier than removing the item from the original list
            new_order_list = [ stored_order_data for stored_order_data in order_list if stored_order_data["id"] != order_data["id"] ]
            self._update_broker_data(position=position, new_order_list=new_order_list)
        else:
            self._delete_stored_position(stored_position=position)
        
        logging.info(f"Results: cancel order - {cancel_result}, sell result - {sell_result}")

    def _update_broker_data(self, position:dict, new_order_list:list) -> None:
        position[M_STATE_KEY_BROKER_DATA()] = json.dumps(new_order_list)
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        table_client.update_entity(entity=position)

    def _delete_stored_position(self, stored_position:dict) -> None:
        if M_STATE_KEY_ROWKEY() not in stored_position:
            raise ValueError(f"expected {M_STATE_KEY_ROWKEY()} in {stored_position}")
        if M_STATE_KEY_PARTITIONKEY() not in stored_position:
            raise ValueError(f"expected {M_STATE_KEY_PARTITIONKEY()} in {stored_position}")
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        table_client.delete_entity(
            partition_key=stored_position[M_STATE_KEY_PARTITIONKEY()],
            row_key=stored_position[M_STATE_KEY_ROWKEY()]
        )
    
    # def _create_worker_pool(self) -> concurrent.futures.ThreadPoolExecutor:
    #     max_worker_count = min(os.cpu_count() * 2, 10)
    #     return concurrent.futures.ThreadPoolExecutor(
    #         max_workers=max_worker_count,
    #         thread_name_prefix="FlowMerc"
    #     )
    
    ### Signals

    def handle_market_signal(self, signal: MerchantSignal) -> None:
        logging.debug(f"handle_market_signal() - {signal.id()}")
        logging.info(f"received signal - id={signal.id()} - {signal.info()}")
        self.merchant_id(signal)
        self.on_signal_received.emit(self.merchant_id(), signal)
        try:
            self.load_config_from_signal(signal)
            self.load_config_from_env() # env should override signal configs
            self.load_state_from_storage()
            if self.status() == M_STATE_SHOPPING():
                self._handle_signal_when_shopping(signal)
            elif self.status() == M_STATE_BUYING():
                self._handle_signal_when_buying(signal)
            elif self.status() == M_STATE_SELLING():
                self._handle_signal_when_selling(signal)
            elif self.status() == M_STATE_RESTING():
                self._handle_signal_when_resting(signal)
            else:
                raise ValueError(f"Unknown state {self.status()}")
        finally:
            logging.info(f"finished handling signal - id={signal.id()}")
    
    def load_state_from_storage(self) -> None:
        logging.debug(f"load_state_from_storage()")
        query_filter = f"{M_STATE_KEY_MERCHANT_ID()} eq '{self.merchant_id()}'"
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        rows = list(table_client.query_entities(query_filter))
        if len(rows) > 1:
            raise ValueError(f"Multiple open merchants found for {self.merchant_id()}")
        else:
            if len(rows) == 1:
                logging.info(f"found existing merchant - id={self.merchant_id()}")
                row = rows[0]
                self.status(row.get(M_STATE_KEY_STATUS()))
                self.id(row.get(M_STATE_KEY_ID()))
                self.state[M_STATE_KEY_PARTITIONKEY()] = row.get(M_STATE_KEY_PARTITIONKEY())
                self.state[M_STATE_KEY_ROWKEY()] = row.get(M_STATE_KEY_ROWKEY())
                self.last_action_time(row.get(M_STATE_KEY_LAST_ACTION_TIME()))
                self.state[M_STATE_KEY_VERSION()] = row.get(M_STATE_KEY_VERSION())
                self.broker_data(row.get(M_STATE_KEY_BROKER_DATA()))
            else:
                logging.info(f"no open merchants found for {self.merchant_id()}, creating new...")
                self.status(M_STATE_SHOPPING())
                self.id(str(uuid.uuid4()))
                self.broker_data(json.dumps([ ]))
                table_client.create_entity(entity=self.state)

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
        self.state[M_STATE_KEY_PARTITIONKEY()] = self.merchant_id()
        self.state[M_STATE_KEY_ROWKEY()] = f"{signal.ticker()}-{signal.id()}"
        self.state[M_STATE_KEY_MERCHANT_ID()] = self.merchant_id()
        self.state[M_STATE_KEY_VERSION()] = signal.version()
        self.state[M_STATE_KEY_TICKER()] = signal.ticker()
        self.state[M_STATE_KEY_SUGGESTED_STOPLOSS()] = signal.suggested_stoploss()
        self.state[M_STATE_KEY_TAKEPROFIT_PERCENT()] = signal.takeprofit_percent()
        self.high_interval(signal.high_interval())
        self.low_interval(signal.low_interval())
        self.state[M_STATE_KEY_REST_INTERVAL()] = signal.rest_interval()
        """ NOTE 
        due to a bug in Azure - use seconds instead of millis - https://github.com/Azure/azure-sdk-for-python/issues/35554 
        """
        self.last_action_time(unix_timestamp_secs())
        
    def _handle_signal_when_shopping(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_shopping()")
        if signal.low_interval() == signal.high_interval():
            ### if low and high are the same, then we assume we're ok with just buying a single trading view alert
            ### in this case we go straight to buying (shopping -> buying is for confluence)
            logging.warning(f"low and high are the same, going straight to buying")
            self._start_buying()
            self._handle_signal_when_buying(signal)
            return True
        else:
            if signal.interval() == self.high_interval():
                if signal.action() == S_ACTION_BUY():
                    self._start_buying()
                    return True
        return False

    def _handle_signal_when_buying(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_buying()")
        if signal.interval() == self.low_interval():
            if signal.action() == S_ACTION_BUY():
                order_result = self._place_order(signal)
                order_list = json.loads(self.broker_data())
                order_list.append(order_result)
                self.broker_data(json.dumps(order_list))
                self.on_order_placed.emit(self.merchant_id(), order_result)
                if signal.rest_after_buy():
                    self._start_resting()
                else:
                    self._start_selling()
                return True
        elif signal.interval() == self.high_interval():
            if signal.action() == S_ACTION_SELL():
                self._start_shopping()
                return True
        return False
    
    def _handle_signal_when_selling(self, signal: MerchantSignal) -> bool:
        ### what to do here? just allow tne take profits and stop loss to trigger
        ### at least for now. This will become useful later when we include bearish and bullish bias
        logging.debug(f"_handle_signal_when_selling()")
        if signal.action() == S_ACTION_SELL():
            ## do nothing - allow take profit and stop loss to trigger
            self._start_resting()
            return True
        return False

    def _handle_signal_when_resting(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_resting()")
        now_timestamp_seconds = unix_timestamp_secs()
        rest_interval_seconds = self.rest_interval_minutes() * 60
        if (now_timestamp_seconds > self.last_action_time() + rest_interval_seconds):
            self._start_shopping()
        else:
            time_left_in_seconds = now_timestamp_seconds - (self.last_action_time() + rest_interval_seconds)
            logging.info(f"Resting for another {time_left_in_seconds} seconds")
        return True

    def _start_buying(self) -> None:
        logging.debug(f"_start_buying()")
        self.status(M_STATE_BUYING())
        self.last_action_time(unix_timestamp_secs())
        self._sync_with_storage()
        state_copy = copy.deepcopy(self.state)
        self.on_state_change.emit(self.merchant_id(), self.status(), state_copy)

    def _start_shopping(self) -> None:
        logging.debug(f"_start_shopping()")
        self.status(M_STATE_SHOPPING())
        self.last_action_time(unix_timestamp_secs())
        self._sync_with_storage()
        state_copy = copy.deepcopy(self.state)
        self.on_state_change.emit(self.merchant_id(), self.status(), state_copy)

    def _start_selling(self) -> None:
        logging.debug(f"_start_selling()")
        self.status(M_STATE_SELLING())
        self.last_action_time(unix_timestamp_secs())
        self._sync_with_storage()
        state_copy = copy.deepcopy(self.state)
        self.on_state_change.emit(self.merchant_id(), self.status(), state_copy)

    def _start_resting(self) -> None:
        logging.debug(f"_start_resting()")
        self.status(M_STATE_RESTING())
        self.last_action_time(unix_timestamp_secs())
        self._sync_with_storage()
        state_copy = copy.deepcopy(self.state)
        self.on_state_change.emit(self.merchant_id(), self.status(), state_copy)

    def _sync_with_storage(self) -> None:
        logging.debug(f"_sync_with_storage()")
        client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        logging.info(f"persisting the following state to storage: {self.state}")
        client.update_entity(entity=self.state)

    def _place_order(self, signal: MerchantSignal) -> dict:
        logging.debug(f"_place_order()")
        take_profit = calculate_take_profit_from_signal(signal)
        stop_loss = calculate_stop_loss_from_signal(signal)
        quantity = signal.contracts()
        safety_check(signal.close(), take_profit, stop_loss, quantity)
        
        execute_order = self.broker.place_test_order if signal.dry_run() else self.broker.place_limit_order
        result = execute_order(
            source=self.merchant_id(), 
            ticker=signal.ticker(), 
            contracts=signal.contracts(),
            limit=signal.close(),
            take_profit=take_profit, 
            stop_loss=stop_loss,
            broker_params=signal.broker_params()
        )
        result.update({"id": unix_timestamp_ms()})
        """ NOTE
        this will be stored in the tables service - it would be good to think about a schema 
        that would work for any broker
        """
        return result
        
    def _new_merchant_id_from_signal(self, signal: MerchantSignal) -> str:
        return self._create_merchant_id(signal.ticker(), signal.low_interval(), signal.high_interval(), signal.version())

    def _create_merchant_id(self, ticker: str, low_interval: str, high_interval: str, version: str) -> str:
        return f"{ticker}-{low_interval}-{high_interval}-{version}"

    ## properties
    def status(self, status: str = None) -> str:
        if status is not None:
            _allowed_states = [ M_STATE_BUYING(), M_STATE_SELLING(), M_STATE_RESTING(), M_STATE_SHOPPING() ] 
            if status not in _allowed_states:
                raise ValueError(f"status {status} is not supported, only {_allowed_states}")
            self.state[M_STATE_KEY_STATUS()] = status
        return self.state.get(M_STATE_KEY_STATUS())
    
    def id(self, id: str = None) -> str:
        if id is not None:
            new_id = id.strip()
            if len(new_id) == 0:
                raise ValueError("id must not be empty")
            self.state[M_STATE_KEY_ID()] = new_id
        return self.state.get(M_STATE_KEY_ID())
    
    def broker_data(self, broker_data: str = None) -> str:
        if broker_data is not None:
            json.loads(broker_data)
            self.state[M_STATE_KEY_BROKER_DATA()] = broker_data
        return self.state.get(M_STATE_KEY_BROKER_DATA())
    
    def high_interval(self, high_intv: str = None) -> str:
        ## this should come from merchant config
        if high_intv is not None:
            self.state[M_STATE_KEY_HIGH_INTERVAL()] = high_intv
        return self.state.get(M_STATE_KEY_HIGH_INTERVAL())
    
    def low_interval(self, low_intv: str = None) -> str:
        ## this should come from merchant config
        if low_intv is not None:
            self.state[M_STATE_KEY_LOW_INTERVAL()] = low_intv
        return self.state.get(M_STATE_KEY_LOW_INTERVAL())
        
    def last_action_time(self, latest: int = None) -> int:
        ## this should come from storage
        if latest is not None:
            self.state[M_STATE_KEY_LAST_ACTION_TIME()] = latest
        return int(self.state.get(M_STATE_KEY_LAST_ACTION_TIME()))
    
    def rest_interval_minutes(self) -> int:
        ## this should come from merchant config
        return int(self.state.get(M_STATE_KEY_REST_INTERVAL()))

    def merchant_id(self, signal: MerchantSignal=None) -> str:
        if signal is not None:
            self._id = self._new_merchant_id_from_signal(signal=signal)
        return self._id