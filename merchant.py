import copy
import json
import logging
import os
import eventkit
import uuid

from azure.data.tables import TableServiceClient

from events import EventLoggable
from live_capable import LiveCapable
from merchant_signal import MerchantSignal
from order_capable import Broker, MarketOrderable, LimitOrderable, OrderCancelable, DryRunnable
from transactions import calculate_stop_loss, calculate_take_profit
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

def M_STATE_KEY_DRYRUN():
    return "dry_run"

def TABLE_NAME():
    return "flowmerchant"

class env:
    @staticmethod
    def DRY_RUN():
        return "MERCHANT_DRY_RUN"

class Merchant:

    def __init__(self, table_service: TableServiceClient, broker: Broker) -> None:
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
        logging.debug(f"check_positions()")
        start_time_ms = unix_timestamp_ms()
        if not isinstance(self.broker, LiveCapable):
            logging.warning("Broker is not LiveCapable - will skip checking positions")
            return { }
        if not isinstance(self.broker, MarketOrderable):
            logging.warning("Broker is not MarketOrderable - will skip checking positions")
            return { }
        if not isinstance(self.broker, LimitOrderable):
            logging.warning("Broker is not LimitOrderable - will skip checking positions")
            return { }
        if not isinstance(self.broker, OrderCancelable):
            logging.warning("Broker is not OrderCancelable - will skip checking positions")
            return { }  
        
        current_positions = self._query_current_positions()
        logging.info(f"Will check the following positions {current_positions}")

        tickers = [ position[M_STATE_KEY_TICKER()] for position in current_positions if M_STATE_KEY_TICKER() in position ]
        tickers.sort()
        
        current_prices = self.broker.get_current_prices(symbols=tickers)
        
        check_results = self._check_profitable_positions(
            positions=current_positions, 
            current_prices=current_prices
        )

        results = {
            "monitored_tickers": tickers,
            "current_positions": check_results,
            "elapsed_ms": unix_timestamp_ms() - start_time_ms
        }

        self.on_positions_check.emit(results)

        self._purge_old_positions()

        return results
    
    def _check_order(self, ticker:str, order: dict, current_price: float, stop_loss_percent:float, take_profit_percent:float, running_results:dict) -> dict:
        if "orders" not in order:
            raise ValueError(f"expected key orders to be in {order}")
        if "main" not in order.get("orders"):
            raise ValueError(f"expected key main to be in {order}")
        if "stop_loss" not in order.get("orders"):
            raise ValueError(f"expected key stop_loss to be in {order}")
        if "take_profit" not in order.get("orders"):
            raise ValueError(f"expected key take_profit to be in {order}")
        
        dry_run_order = order["metadata"].get("is_dry_run")

        main_order = order["orders"].get("main")
        if dry_run_order:
            main_order.update({"ready": True})
        else:
            """ NOTE 
            This is mostly here in the case of the market order taking awhile to fill. 
            As in - a market order is made in the signals flow, but may not be filled immediately.
            So we would check it here instead. 
            see if there is a way to get all orders in one call, then filter
            """
            main_order = self.broker.get_order(ticker=ticker, order_id=main_order.get("id"))
            
        if not main_order.get("ready"):
            """ NOTE
            this is problematic but may be ok to just wait to the next round
            with the risk that we miss a price movement
            """
            raise ValueError(f"order is not ready yet: {main_order}")
        
        order_price = float(main_order.get("price"))
        order_contracts = float(main_order.get("contracts"))

        entry = {
                    "ticker": ticker,
                    "order": order,
                    "contracts": order_contracts,
                    "current_price": current_price,
                }

        if current_price < order_price:
            stop_loss = calculate_stop_loss(
                close_price=order_price, 
                stop_loss_percent=stop_loss_percent
            )
            if stop_loss > current_price:
                """ TODO 
                This method relies us detecting the current price dropping below the stop order
                price - at the time of this check. It doesn't mean anything - the stop could have 
                triggered already. 

                So, use the actual stop loss orders instead of this for reliability
                We need to remove them at some point and the result of the stop loss order
                is probably the most reliable way. 
                """
                running_results["losers"].append(entry)
            else:
                running_results["laggards"].append(entry)
        else:
            take_profit = calculate_take_profit(
                close_price=order_price, 
                take_profit_percent=take_profit_percent
            )
            if current_price >= take_profit:
                running_results["winners"].append(entry)
            else:
                running_results["leaders"].append(entry)

    def _check_position(self, position: dict, current_prices: dict, running_results: dict) -> dict:
        if M_STATE_KEY_TICKER() not in position:
            raise ValueError(f"critical key {M_STATE_KEY_TICKER()} not in stored position {position}")
        if M_STATE_KEY_BROKER_DATA() not in position:
            raise ValueError(f"critical key {M_STATE_KEY_BROKER_DATA()} not in stored position {position}")
        if M_STATE_KEY_TAKEPROFIT_PERCENT() not in position:
            raise ValueError(f"critical key {M_STATE_KEY_TAKEPROFIT_PERCENT()} not in stored position {position}")

        order_ticker = position.get(M_STATE_KEY_TICKER())
        
        if order_ticker not in current_prices:
            raise ValueError(f"ticker is missing from current prices list {order_ticker} - {current_prices}")

        current_price = current_prices.get(order_ticker)
        stop_loss_percent = float(position.get(M_STATE_KEY_SUGGESTED_STOPLOSS()))
        take_profit_percent = float(position.get(M_STATE_KEY_TAKEPROFIT_PERCENT()))
        order_list = json.loads(position.get(M_STATE_KEY_BROKER_DATA()))

        for order in order_list:
            self._check_order(
                ticker=order_ticker, 
                order=order, 
                current_price=current_price,
                stop_loss_percent=stop_loss_percent,
                take_profit_percent=take_profit_percent,
                running_results=running_results
            )
    
    def _check_profitable_positions(self, positions: list, current_prices: dict) -> dict:
        logging.debug("_check_profitable_positions()")
        results = {
            "winners": [],
            "laggards": [],
            "leaders": [],
            "losers": []
        }
        for position in positions:
            self._check_position(
                position=position,
                current_prices=current_prices,
                running_results=results
            )
        for winner in results["winners"]:
            results_if_any = self._handle_take_profit(
                order_data=winner.get("order"),
                ticker=winner.get("ticker"),
                contracts=winner.get("contracts"),
                dry_run_mode=self.dry_run()
            )
            results.update({ "results": results_if_any })
        for loser in results["losers"]:
            results_if_any = self._handle_stop_loss(
                order_data=loser.get("order"),
                ticker=loser.get("ticker")
            )
            results.update({ "results": results_if_any })
        return results

    def _query_current_positions(self) -> list: 
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        return list(table_client.list_entities())
    
    def _query_position_by_ticker(self, ticker:str) -> dict:
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        query_filter = f"ticker eq '{ticker}'"
        results = list(table_client.query_entities(query_filter))
        if len(results) > 1:
            raise ValueError(f"expected one position for {ticker} but got {len(results)}")
        elif len(results) == 0:
            raise ValueError(f"expected one position for {ticker} but got none")
        else:
            return results[0]
    
    def _handle_stop_loss(self, order_data:dict, ticker:str) -> dict:
        logging.warning(f"Stop loss reached for {ticker}")
        position = self._query_position_by_ticker(ticker=ticker)
        order_list = json.loads(position[M_STATE_KEY_BROKER_DATA()])
        if len(order_list) == 0:
            raise ValueError(f"expected at least one order in the order list in position {ticker} - at the point of stop loss trigger")
        
        new_order_list = []
        for stored_order in order_list:
            stored_order_id = stored_order["metadata"].get("id")
            removal_order_id = order_data["metadata"].get("id")
            if stored_order_id != removal_order_id:
                new_order_list.append(stored_order)
            else:
                logging.info(f"stop loss triggered - removing stored order {order_data} for ticker {ticker}")
        
        if len(new_order_list) == len(order_list):
            raise ValueError(f"order not found in stored order list {order_data}")
        
        self._update_broker_data(position=position, new_order_list=new_order_list)
        
        return {
            "ticker": ticker,
            "order": order_data
        }

    def _handle_take_profit(self, order_data:dict, ticker:str, contracts:float, dry_run_mode:bool = False) -> dict:
        if not isinstance(self.broker, MarketOrderable):
            raise ValueError("Broker is not market orderable")
        if not isinstance(self.broker, OrderCancelable):
            raise ValueError("Broker is not order cancelable")
        if dry_run_mode:
            if not isinstance(self.broker, DryRunnable):
                raise ValueError("Running in dry run mode but broker is NOT dry runnable")
            
        execute_market_order = self.broker.place_market_order_test if dry_run_mode else self.broker.place_market_order
        execute_cancel_order = self.broker.cancel_order_test if dry_run_mode else self.broker.cancel_order

        stoploss_order_id = order_data["orders"]["stop_loss"].get("id")
        
        logging.info(f"Take profit reached for {ticker} - will cancel the stop loss order and SELL")
        cancel_result = execute_cancel_order(
            ticker=ticker, 
            order_id=stoploss_order_id
        )
        
        """ TODO - if this fails then we are in trouble because our stop loss is gone, consider a retry mechanism """
        sell_result = execute_market_order(
            ticker=ticker, 
            action="SELL", 
            contracts=contracts
        )

        position = self._query_position_by_ticker(ticker=ticker)
        
        order_list = json.loads(position.get(M_STATE_KEY_BROKER_DATA()))
        if len(order_list) == 0:
            raise ValueError(f"expected there to be at least one order in position {position} - at the point of take profit")
        
        new_order_list = []
        for stored_order in order_list:
            stored_order_id = stored_order["metadata"].get("id")
            removal_order_id = order_data["metadata"].get("id")
            if stored_order_id != removal_order_id:
                new_order_list.append(stored_order)
            else:
                logging.info(f"take profit triggered - removing stored order {order_data} for ticker {ticker}")
        
        if len(new_order_list) == len(order_list):
            raise ValueError(f"order not found in stored order list: order={order_data}, order_list={order_list}")
        
        self._update_broker_data(position=position, new_order_list=new_order_list)
        
        logging.info(f"Results: cancel order - {cancel_result}, sell result - {sell_result}")
        return {
            "ticker": ticker,
            "order": order_data,
            "cancel_result": cancel_result,
            "sell_result": sell_result
        }

    def _update_broker_data(self, position:dict, new_order_list:list) -> None:
        position[M_STATE_KEY_BROKER_DATA()] = json.dumps(new_order_list)
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        table_client.update_entity(entity=position)

    def _purge_old_positions(self) -> dict:
        table_client =  self.table_service.get_table_client(table_name=self.TABLE_NAME)
        all_positions = list(table_client.list_entities())
        one_year_old_ts = unix_timestamp_secs() - (365 * 24 * 60 * 60)
        for position in all_positions:
            last_action_time = position.get(M_STATE_KEY_LAST_ACTION_TIME())
            if one_year_old_ts > last_action_time:
                orders = position.get(M_STATE_KEY_BROKER_DATA())
                orders = json.loads(orders)
                if len(orders) != 0:
                    logging.warning(f"position {position} has orders {orders} - not deleting!")
                else:
                    logging.info(f"deleting old position {position}")
                    table_client.delete_entity(
                        partition_key=position.get(M_STATE_KEY_PARTITIONKEY()),
                        row_key=position.get(M_STATE_KEY_ROWKEY())
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
                self.partition_key(row.get(M_STATE_KEY_PARTITIONKEY()))
                self.row_key(row.get(M_STATE_KEY_ROWKEY()))
                self.last_action_time(row.get(M_STATE_KEY_LAST_ACTION_TIME()))
                self.version(row.get(M_STATE_KEY_VERSION()))
                self.broker_data(row.get(M_STATE_KEY_BROKER_DATA()))
            else:
                logging.info(f"no open merchants found for {self.merchant_id()}, creating new...")
                self.status(M_STATE_SHOPPING())
                self.id(str(uuid.uuid4()))
                self.broker_data(json.dumps([ ]))
                table_client.create_entity(entity=self.state)

    def load_config_from_env(self) -> None:
        """ NOTE - env will OVERRIDE signal configs """
        logging.debug(f"load_config_from_env()")
        if self.dry_run():
            logging.warning(f"DRY RUN MODE - will not execute actual trades but will store state")

    def load_config_from_signal(self, signal: MerchantSignal) -> None:
        logging.debug(f"load_config_from_signal()")
        if self.state is None:
            self.state = {}
        self.state[M_STATE_KEY_MERCHANT_ID()] = self.merchant_id()
        self.partition_key(self.merchant_id())
        self.row_key(f"{signal.ticker()}-{signal.id()}")
        self.version(signal.version())
        self.ticker(signal.ticker())
        self.suggested_stoploss(signal.suggested_stoploss())
        self.takeprofit_percent(signal.takeprofit_percent())
        self.high_interval(signal.high_interval())
        self.low_interval(signal.low_interval())
        self.rest_interval_minutes(signal.rest_interval())
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
                self._handle_orders(signal=signal)
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

    def _handle_orders(self, signal: MerchantSignal) -> None:
        order_result = self._place_orders(signal)
        order_list = json.loads(self.broker_data())
        order_list.append(order_result)
        new_order_list = json.dumps(order_list)
        self.broker_data(broker_data=new_order_list)
        self.on_order_placed.emit(self.merchant_id(), order_result)

    def _place_orders(self, signal: MerchantSignal) -> dict:
        ticker = signal.ticker()
        contracts = signal.contracts()
        take_profit_percent = signal.takeprofit_percent()
        stop_loss_percent = signal.suggested_stoploss()
        dry_run_mode = self.dry_run()

        if dry_run_mode:
            if not isinstance(self.broker, DryRunnable):
                raise ValueError("Broker is set to dry run mode but is not a DryRunnable")
        else:
            if not isinstance(self.broker, MarketOrderable):
                raise ValueError("Broker is not a MarketOrderable")
            if not isinstance(self.broker, LimitOrderable):
                raise ValueError("Broker is not a LimitOrderable")
            if not isinstance(self.broker, LiveCapable):
                raise ValueError("Broker is not a LiveCapable")

        execute_market_order = self.broker.place_market_order_test if self.dry_run() else self.broker.place_market_order
        execute_limit_order = self.broker.place_limit_order_test if self.dry_run() else self.broker.place_limit_order
        
        market_order_rx = execute_market_order(
            ticker=ticker,
            contracts=contracts,
            action="BUY"
        )
        market_order_info = self.broker.standardize_market_order(market_order_rx)

        if not dry_run_mode:
            market_order_info = self.broker.get_order(
                ticker=ticker, 
                order_id=market_order_info.get("id")
            )

        main_order_price = market_order_info.get("price")
        main_order_contracts = market_order_info.get("contracts")

        stop_loss_price = calculate_stop_loss(
            close_price=main_order_price,
            stop_loss_percent=stop_loss_percent
        )

        stop_loss_order_rx = execute_limit_order(
            ticker=ticker,
            action="SELL",
            contracts=main_order_contracts,
            limit=stop_loss_price
        )
        stop_loss_order_info = self.broker.standardize_limit_order(stop_loss_order_rx)

        stop_loss_order_price = stop_loss_order_info.get("price")

        take_profit_price = calculate_take_profit(
            close_price=main_order_price,
            take_profit_percent=take_profit_percent
        )

        return {
            "metadata": {
                "id": str(uuid.uuid4()),
                "time_created": unix_timestamp_ms(),
                "is_dry_run": dry_run_mode,
            },
            "ticker": ticker,
            "orders": {
                "main": {
                    "id": market_order_info.get("id"),
                    "api_response": market_order_rx,
                    "time": market_order_info.get("timestamp"),
                    "contracts": main_order_contracts,
                    "price": main_order_price,
                },
                "stop_loss": {
                    "id": stop_loss_order_info.get("id"),
                    "api_response": stop_loss_order_rx,
                    "time": stop_loss_order_info.get("timestamp"),
                    "price": stop_loss_order_price
                },
                "take_profit": {
                    "price": take_profit_price
                }
            },
            "projections": {
                "profit_without_fees": (take_profit_price * main_order_contracts) - (main_order_price * main_order_contracts),
                "loss_without_fees" : (stop_loss_price * main_order_contracts) - (main_order_price * main_order_contracts)
            }
        }
        
    def _new_merchant_id_from_signal(self, signal: MerchantSignal) -> str:
        return self._create_merchant_id(signal.ticker(), signal.low_interval(), signal.high_interval(), signal.version())

    def _create_merchant_id(self, ticker: str, low_interval: str, high_interval: str, version: str) -> str:
        return f"{ticker}-{low_interval}-{high_interval}-{version}"

    ## properties
    def row_key(self, row_key: str = None) -> str:
        if row_key is not None:
            new_row_key = row_key.strip()
            if len(new_row_key) == 0:
                raise ValueError("row_key must not be empty")
            self.state[M_STATE_KEY_ROWKEY()] = new_row_key
        return self.state.get(M_STATE_KEY_ROWKEY())

    def partition_key(self, partition_key: str = None) -> str:
        if partition_key is not None:
            new_partition_key = partition_key.strip()
            if len(new_partition_key) == 0:
                raise ValueError("partition_key must not be empty")
            self.state[M_STATE_KEY_PARTITIONKEY()] = new_partition_key
        return self.state.get(M_STATE_KEY_PARTITIONKEY())

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
    
    def rest_interval_minutes(self, rest_interval:int = None) -> int:
        ## this should come from merchant config
        if rest_interval is not None:
            if rest_interval < 0:
                raise ValueError("rest_interval must be >= 0")
            self.state[M_STATE_KEY_REST_INTERVAL()] = rest_interval
        return int(self.state.get(M_STATE_KEY_REST_INTERVAL()))

    def merchant_id(self, signal: MerchantSignal=None) -> str:
        if signal is not None:
            self._id = self._new_merchant_id_from_signal(signal=signal)
        return self._id
    
    def suggested_stoploss(self, stoploss: float = None) -> float:
        if stoploss is not None:
            if stoploss > 1.0:
                raise ValueError("stoploss must be <= 100.0")
            self.state[M_STATE_KEY_SUGGESTED_STOPLOSS()] = stoploss
        return float(self.state.get(M_STATE_KEY_SUGGESTED_STOPLOSS()))
    
    def takeprofit_percent(self, takeprofit: float = None) -> float:
        if takeprofit is not None:
            if takeprofit < 1.0:
                raise ValueError("takeprofit must be >= 100.0")
            self.state[M_STATE_KEY_TAKEPROFIT_PERCENT()] = takeprofit
        return float(self.state.get(M_STATE_KEY_TAKEPROFIT_PERCENT()))
    
    def version(self, version: int = None) -> str:
        if version is not None:
            if version < 1:
                raise ValueError("version must be >= 1")
            self.state[M_STATE_KEY_VERSION()] = version
        return self.state.get(M_STATE_KEY_VERSION())
    
    def ticker(self, ticker: str = None) -> str:
        if ticker is not None:
            ticker = ticker.strip()
            if len(ticker) == 0:
                raise ValueError("ticker must not be empty")
            self.state[M_STATE_KEY_TICKER()] = ticker
        return self.state.get(M_STATE_KEY_TICKER())
    
    def dry_run(self) -> bool:
        return self._cfg_is_dry_run()
    
    ## config

    def _cfg_is_dry_run(self) -> bool:
        return os.environ.get(env.DRY_RUN(), "false").lower() == "true"