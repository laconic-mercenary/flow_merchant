import copy
import json
import logging
import os
import eventkit
import uuid

from azure.data.tables import TableServiceClient

from bracket_strategy import BracketStrategy
from trailing_stop_strategy import TrailingStopStrategy
from live_capable import LiveCapable
from merchant_keys import keys, state, action
from merchant_order import Order
from merchant_signal import MerchantSignal
from order_capable import Broker, MarketOrderable, StopMarketOrderable, OrderCancelable, DryRunnable
from order_strategy import OrderStrategy
from order_strategies import OrderStrategies
from security import order_digest
from transactions import calculate_stop_loss, calculate_take_profit
from utils import unix_timestamp_secs, unix_timestamp_ms, roll_dice_10percent, null_or_empty, consts as util_consts

class cfg:
    @staticmethod
    def DRY_RUN_MODE() -> bool:
        return os.environ.get("MERCHANT_DRY_RUN", "false").lower() == "true"
    
    @staticmethod
    def MULTI_TRADE_MODE() -> bool:
        return os.environ.get("MERCHANT_MULTI_TRADE_MODE", "false").lower() == "true"

    @staticmethod
    def DRY_RUN_EXCEPTIONS() -> list[str]:
        raw_list = os.environ.get("MERCHANT_DRY_RUN_EXCEPTIONS", "-")
        raw_list = raw_list.split(",")
        return [ entry.strip() for entry in raw_list ]
    
    @staticmethod
    def TABLE_NAME():
        return "flowmerchant"
    
class SellResult:
    def __init__(self, order: Order, api_results:dict) -> None:
        self.order = order
        self.api_results = api_results

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
        self._order_strategy = None
        
        self.TABLE_NAME = cfg.TABLE_NAME()
        table_service.create_table_if_not_exists(table_name=self.TABLE_NAME)

        self.on_signal_received = eventkit.Event("on_signal_received")
        self.on_state_change = eventkit.Event("on_state_change")
        self.on_order_placed = eventkit.Event("on_order_placed")
        self.on_positions_check = eventkit.Event("on_positions_check")

    ### Selling

    def sell(self, identifier: str) -> SellResult:
        logging.debug(f"sell()")
        if null_or_empty(identifier):
            raise ValueError("identifier cannot be null or empty")
        current_positions = self._query_current_positions()
        for position in current_positions:
            position_order_list = json.loads(position.get(keys.BROKER_DATA()))
            for order_dict in position_order_list:
                order = Order.from_dict(order_dict)
                order_sell_id = order_digest(order)
                if order_sell_id == identifier:
                    logging.info(f"Selling {order}")
                    api_results = self._broker_sell_order(order=order)
                    self._remove_order_from_storage(position=position, removal_order=order)
                    return SellResult(order=order, api_results=api_results)
        logging.warning(f"Unable to find order with identifier {identifier}")
        return None

    def _broker_sell_order(self, order: Order) -> dict:
        logging.debug(f"_broker_sell_order()")
        if not isinstance(self.broker, MarketOrderable):
            raise ValueError("Broker must be MarketOrderable")
        if order.metadata.is_dry_run:
            if not isinstance(self.broker, DryRunnable):
                raise ValueError("Broker must be DryRunnable")
        results = {}
        if isinstance(self.broker, StopMarketOrderable):
            if not isinstance(self.broker, OrderCancelable):
                raise ValueError("Broker must be OrderCancelable")
            cancel_result = self.broker.cancel_order(
                ticker=order.ticker, 
                order_id=order.sub_orders.stop_loss.id
            )
            results.update({"cancel_result": cancel_result})
        
        execute_market_order = self.broker.place_market_order if not order.metadata.is_dry_run else self.broker.place_market_order_test
        sell_result_api = execute_market_order(
                                ticker=order.ticker,
                                action="SELL",
                                contracts=order.sub_orders.main_order.contracts
                            )
        sell_result = self.broker.standardize_market_order(market_order_result=sell_result_api)
        logging.info(f"Sold order {order}, with result {sell_result}")
        results.update({
            "_market_order_sell_result_broker_api": sell_result_api,
            "market_order_sell_result": sell_result
        })
        return results
        
    def _remove_order_from_storage(self, position:dict, removal_order:Order) -> None:
        position_order_list = json.loads(position.get(keys.BROKER_DATA()))
        removal_id = order_digest(removal_order)
        new_order_list = []
        for order_dict in position_order_list:
            if order_digest(Order.from_dict(order_dict)) != removal_id:
                new_order_list.append(order_dict)
        if len(new_order_list) != 0:
            position[keys.BROKER_DATA()] = json.dumps(new_order_list)
            self._sync_with_storage(state=position)
            logging.info(f"Removed order {removal_order} from storage, order list went from size {len(position_order_list)} to {len(new_order_list)}")
        else:
            table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)    
            table_client.delete_entity(
                partition_key=position[keys.PARTITIONKEY()], 
                row_key=position[keys.ROWKEY()]
            )
            logging.info(f"Deleted position {position} - no remaining orders to track")

    ### Positions

    def check_positions(self) -> dict:
        logging.debug(f"check_positions()")

        logging.info(f"checking current positions...")

        start_time_ms = unix_timestamp_ms()
        
        results = self._check_positions()

        if roll_dice_10percent():
            self._purge_old_positions()

        results.update({ "elapsed_ms": unix_timestamp_ms() - start_time_ms })

        self.on_positions_check.emit(results)

        return results

    def _check_positions(self) -> dict:
        if not self._check_broker():
            return { }
        current_positions = self._query_current_positions()
        database = { }
        tickers = [ position[keys.TICKER()] for position in current_positions if keys.TICKER() in position ]
        tickers.sort()
        
        current_prices = self.broker.get_current_prices(symbols=tickers)
        database.update({ "current_prices": current_prices })

        results = {
            "monitored_tickers": tickers,
            "positions": {
                "losers": [],
                "winners": [],
                "leaders": [],
                "laggards": [],
            },
            "current_prices": current_prices
        }
        
        for position in current_positions:
            ticker = position.get(keys.TICKER())
            if ticker not in current_prices:
                logging.warning(f"check_positions() - no price for {ticker} - {current_prices}")
            else:
                check_result = self._check_position(
                                    position=position, 
                                    database=database
                                )

                if check_result.get("updated", False):
                    self._sync_with_storage(state=position)
                
                results["positions"]["losers"].extend(check_result["orders"]["losers"])
                results["positions"]["winners"].extend(check_result["orders"]["winners"])
                results["positions"]["leaders"].extend(check_result["orders"]["leaders"])
                results["positions"]["laggards"].extend(check_result["orders"]["laggards"])
        return results


    def _check_position(self, position:dict, database:dict) -> dict:
        order_list = json.loads(position.get(keys.BROKER_DATA()))
        results = { 
            "updated": False,
            "orders": {
                "losers": [],
                "winners": [],
                "leaders": [],
                "laggards": []
            }
        }

        ticker = position.get(keys.TICKER())
        current_prices = database.get("current_prices")
        new_order_list = []

        for order_dict in order_list:
            order_dict:Order = Order.from_dict(order_dict)
            main_order = order_dict.sub_orders.main_order
            stop_loss_order = order_dict.sub_orders.stop_loss
            take_profit_order = order_dict.sub_orders.take_profit
            
            is_dry_run = order_dict.metadata.is_dry_run
            current_price = current_prices.get(order_dict.ticker)
            main_order_price = main_order.price
            take_profit_price = take_profit_order.price
            stop_loss_order_id = stop_loss_order.id
            stop_loss_price = stop_loss_order.price
            strategy = self._strategy_from_order(order=order_dict)

            logging.info(f"Checking position: {ticker}, strategy: {strategy.name()} order:{order_dict}")

            ### TODO
            ### the below is not reliable for determining if the stop loss triggered or not
            ### need to determine the correct status to query on the order
            # if not is_dry_run:
            #     stop_loss_order_info = self.broker.get_order(ticker=ticker, order_id=stop_loss_order_id)
            #     logging.info(f"(from broker) stop loss order info: {stop_loss_order_info}")
            #     if stop_loss_order_info.get("ready"):
            #         logging.warning(f"Stop loss was triggered for {ticker} -- according to broker")
            #         ## kinda hacky but reuses the logic below
            #         stop_loss_price = take_profit_price - 1.0
            #         current_price = stop_loss_price - 1.0

            if current_price <= stop_loss_price:
                logging.info(f"stop loss {stop_loss_price} hit for {ticker} at {current_price}")
                self._stop_loss_reached(
                    order=order_dict,
                    results=results,
                    strategy=strategy,
                    merchant_params={ 
                        "current_price": current_price,
                        "dry_run_order": is_dry_run
                    }
                )
            else:
                if current_price >= take_profit_price:
                    logging.info(f"take profit {take_profit_price} reached for {ticker} at {current_price}")
                    self._take_profit_reached(
                        order=order_dict, 
                        strategy=strategy, 
                        results=results,
                        new_order_list=new_order_list,
                        merchant_params={ 
                            "current_price": current_price,
                            "dry_run_order": is_dry_run
                        }
                    )
                    results.update({ "updated": True })
                else:
                    if current_price > main_order_price:
                        results["orders"]["leaders"].append(order_dict.__dict__)
                        new_order_list.append(order_dict.__dict__)
                    else:
                        results["orders"]["laggards"].append(order_dict.__dict__)
                        new_order_list.append(order_dict.__dict__)

        position.update({ keys.BROKER_DATA(): json.dumps(new_order_list) })

        return results
    
    def _take_profit_reached(self, order:Order, results:dict, strategy:OrderStrategy, new_order_list:list[dict], merchant_params:dict) -> None:
        handle_result = strategy.handle_take_profit(
                                broker=self.broker,
                                order=order,
                                merchant_params=merchant_params
                            )
        if handle_result.get("complete", False):
            results["orders"]["winners"].append(order.__dict__)
        else:
            results["orders"]["leaders"].append(order.__dict__)
            new_order_list.append(order.__dict__)

    def _stop_loss_reached(self, order:Order, results:dict, strategy:OrderStrategy, merchant_params:dict = {}) -> None:
        handle_result = strategy.handle_stop_loss(
                            broker=self.broker, 
                            order=order,
                            merchant_params=merchant_params
                        )
        results.update({ "updated": True })
        if order.projections.loss_without_fees > 0.0:
            results["orders"]["winners"].append(order.__dict__)
        else:
            results["orders"]["losers"].append(order.__dict__)

    def _check_broker(self) -> bool:
        result = True
        if not isinstance(self.broker, LiveCapable):
            logging.warning("Broker is not LiveCapable - will skip checking positions")
            result = False
        if not isinstance(self.broker, MarketOrderable):
            logging.warning("Broker is not MarketOrderable - will skip checking positions")
            result = False
        return result

    def _query_current_positions(self) -> list: 
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        return list(table_client.list_entities())

    def _purge_old_positions(self) -> dict:
        table_client =  self.table_service.get_table_client(table_name=self.TABLE_NAME)
        all_positions = list(table_client.list_entities())
        one_year_old_ts = unix_timestamp_secs() - util_consts.ONE_YEAR_IN_SECS()
        for position in all_positions:
            last_action_time = position.get(keys.LAST_ACTION_TIME())
            if one_year_old_ts > last_action_time:
                orders = position.get(keys.BROKER_DATA())
                orders = json.loads(orders)
                if len(orders) != 0:
                    logging.warning(f"position {position} has orders {orders} - not deleting!")
                else:
                    logging.info(f"deleting old position {position}")
                    table_client.delete_entity(
                        partition_key=position.get(keys.PARTITIONKEY()),
                        row_key=position.get(keys.ROWKEY())
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
            if self.status() == state.SHOPPING():
                self._handle_signal_when_shopping(signal)
            elif self.status() == state.BUYING():
                self._handle_signal_when_buying(signal)
            elif self.status() == state.SELLING():
                self._handle_signal_when_selling(signal)
            elif self.status() == state.RESTING():
                self._handle_signal_when_resting(signal)
            else:
                raise ValueError(f"Unknown state {self.status()}")
        finally:
            logging.info(f"finished handling signal - id={signal.id()}")
    
    def load_state_from_storage(self) -> None:
        logging.debug(f"load_state_from_storage()")
        query_filter = f"{keys.MERCHANT_ID()} eq '{self.merchant_id()}'"
        table_client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        rows = list(table_client.query_entities(query_filter))
        if len(rows) > 1:
            raise ValueError(f"Multiple open merchants found for {self.merchant_id()}")
        else:
            if len(rows) == 1:
                logging.info(f"found existing merchant - id={self.merchant_id()}")
                row = rows[0]
                self.status(row.get(keys.STATUS()))
                self.id(row.get(keys.ID()))
                self.partition_key(row.get(keys.PARTITIONKEY()))
                self.row_key(row.get(keys.ROWKEY()))
                self.last_action_time(row.get(keys.LAST_ACTION_TIME()))
                self.version(row.get(keys.VERSION()))
                self.broker_data(row.get(keys.BROKER_DATA()))
            else:
                logging.info(f"no open merchants found for {self.merchant_id()}, creating new...")
                self.status(state.SHOPPING())
                self.id(str(uuid.uuid4()))
                self.broker_data(json.dumps([ ]))
                table_client.create_entity(entity=self.state)

    def load_config_from_env(self) -> None:
        """ NOTE - env will OVERRIDE signal configs """
        logging.debug(f"load_config_from_env()")
        if self.dry_run():
            logging.warning(f"DRY RUN MODE - will not execute actual trades but will store state. The following are exempted: " + str(cfg.DRY_RUN_EXCEPTIONS()))

    def load_config_from_signal(self, signal: MerchantSignal) -> None:
        logging.debug(f"load_config_from_signal()")
        if self.state is None:
            self.state = {}
        self.order_strategy(self._strategy_from_signal(signal=signal))
        self.state[keys.MERCHANT_ID()] = self.merchant_id()
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
            return self._handle_signal_when_buying(signal)
        else:
            if signal.interval() == self.high_interval():
                if signal.action() == action.BUY():
                    self._start_buying()
                    return True
        return False

    def _handle_signal_when_buying(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_buying()")
        if signal.interval() == self.low_interval():
            if signal.action() == action.BUY():
                self._handle_orders(signal=signal)
                if signal.rest_after_buy():
                    self._start_resting()
                else:
                    self._start_selling()
                return True
        elif signal.interval() == self.high_interval():
            if signal.action() == action.SELL():
                self._start_shopping()
                return True
        return False
    
    def _handle_signal_when_selling(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_selling()")
        #### the selling phase means we have already placed an order and are waiting for 
        #### the results - which is handled by a separate worker ( see check_positions() )

        if signal.action() == action.BUY():    
            ## In multi-trade mode, we can have overlapping orders of the same asset for one merchant
            if self.multitrade_mode():
                self._start_buying()
                return self._handle_signal_when_buying(signal=signal)
            else:    
                ## We have a BUY Signal and we are not in multi-trade mode
                if self._has_open_orders(signal=signal):
                    ## remain in the selling phase until these orders are sold
                    logging.info(f"merchant {self.merchant_id()} has open orders, will skip this buy signal for {signal.ticker()}")
                    return False
                else:
                    ## we could go to the resting phase, but why waste this opportunity if it is indeed
                    ## a bullish signal?
                    self._start_buying()
                    return self._handle_signal_when_buying(signal=signal)
        
        self._start_resting()
        return True
        

    def _handle_signal_when_resting(self, signal: MerchantSignal) -> bool:
        logging.debug(f"_handle_signal_when_resting()")
        now_timestamp_seconds = unix_timestamp_secs()
        rest_interval_seconds = self.rest_interval_minutes() * 60
        if (now_timestamp_seconds > self.last_action_time() + rest_interval_seconds):
            if self.multitrade_mode():
                if signal.interval() == self.low_interval():
                    if signal.action() == action.SELL():
                        self._start_buying()
                        return True
                    elif signal.action() == action.BUY():
                        self._start_buying()
                        return self._handle_signal_when_buying(signal)
                elif signal.interval() == self.high_interval():
                    if signal.action() == action.BUY():
                        self._start_buying()
                        return True
            self._start_shopping()
        else:
            time_left_in_seconds = now_timestamp_seconds - (self.last_action_time() + rest_interval_seconds)
            logging.info(f"Resting for another {time_left_in_seconds} seconds")
        return True

    def _start_buying(self) -> None:
        logging.debug(f"_start_buying()")
        self.status(state.BUYING())
        self.last_action_time(unix_timestamp_secs())
        self._sync_with_storage()
        state_copy = copy.deepcopy(self.state)
        self.on_state_change.emit(self.merchant_id(), self.status(), state_copy)

    def _start_shopping(self) -> None:
        logging.debug(f"_start_shopping()")
        self.status(state.SHOPPING())
        self.last_action_time(unix_timestamp_secs())
        self._sync_with_storage()
        state_copy = copy.deepcopy(self.state)
        self.on_state_change.emit(self.merchant_id(), self.status(), state_copy)

    def _start_selling(self) -> None:
        logging.debug(f"_start_selling()")
        self.status(state.SELLING())
        self.last_action_time(unix_timestamp_secs())
        self._sync_with_storage()
        state_copy = copy.deepcopy(self.state)
        self.on_state_change.emit(self.merchant_id(), self.status(), state_copy)

    def _start_resting(self) -> None:
        logging.debug(f"_start_resting()")
        self.status(state.RESTING())
        self.last_action_time(unix_timestamp_secs())
        self._sync_with_storage()
        state_copy = copy.deepcopy(self.state)
        self.on_state_change.emit(self.merchant_id(), self.status(), state_copy)

    def _sync_with_storage(self, state:dict = None) -> None:
        logging.debug(f"_sync_with_storage()")
        if state is None:
            state = self.state
        client = self.table_service.get_table_client(table_name=self.TABLE_NAME)
        logging.info(f"persisting the following state to storage: {state}")
        client.update_entity(entity=state)

    def _handle_orders(self, signal: MerchantSignal) -> None:
        order_result = self._place_orders(signal)
        order_list = json.loads(self.broker_data())
        order_list.append(order_result.__dict__)
        new_order_list = json.dumps(order_list)
        self.broker_data(broker_data=new_order_list)
        self.on_order_placed.emit(self.merchant_id(), order_result)

    def _place_orders(self, signal: MerchantSignal) -> Order:
        merchant_params = {}
        if self.dry_run():
            if not signal.ticker() in cfg.DRY_RUN_EXCEPTIONS():
                merchant_params.update({ "dry_run": True })
        return self.order_strategy().place_orders(
            broker=self.broker,
            signal=signal,
            merchant_state=self.state,
            merchant_params=merchant_params
        )

    ## common

    def _strategy_from_signal(self, signal: MerchantSignal) -> OrderStrategy:
        if signal.strategy() == "bracket":
            return BracketStrategy()
        return TrailingStopStrategy()

    def _strategy_from_order(self, order: Order) -> OrderStrategy:
        if order.merchant_params.strategy == OrderStrategies.BRACKET:
            return BracketStrategy()
        return TrailingStopStrategy()
        
    def _new_merchant_id_from_signal(self, signal: MerchantSignal) -> str:
        return self._create_merchant_id(signal.ticker(), signal.low_interval(), signal.high_interval(), signal.version())

    def _create_merchant_id(self, ticker: str, low_interval: str, high_interval: str, version: str) -> str:
        return f"{ticker}-{low_interval}-{high_interval}-{version}"
    
    def _has_open_orders(self, signal:MerchantSignal) -> bool:
        orders_str = self.broker_data()
        orders_list = json.loads(orders_str)
        return len(orders_list) != 0

    ## properties

    def multitrade_mode(self) -> bool:
        return cfg.MULTI_TRADE_MODE()

    def order_strategy(self, strategy: OrderStrategy = None) -> OrderStrategy:
        if strategy is not None:
            self._order_strategy = strategy
        if self._order_strategy is None:
            raise ValueError("No order strategy has been set")
        return self._order_strategy
    
    def row_key(self, row_key: str = None) -> str:
        if row_key is not None:
            new_row_key = row_key.strip()
            if len(new_row_key) == 0:
                raise ValueError("row_key must not be empty")
            self.state[keys.ROWKEY()] = new_row_key
        return self.state.get(keys.ROWKEY())

    def partition_key(self, partition_key: str = None) -> str:
        if partition_key is not None:
            new_partition_key = partition_key.strip()
            if len(new_partition_key) == 0:
                raise ValueError("partition_key must not be empty")
            self.state[keys.PARTITIONKEY()] = new_partition_key
        return self.state.get(keys.PARTITIONKEY())

    def status(self, status: str = None) -> str:
        if status is not None:
            _allowed_states = [ state.BUYING(), state.SELLING(), state.RESTING(), state.SHOPPING() ] 
            if status not in _allowed_states:
                raise ValueError(f"status {status} is not supported, only {_allowed_states}")
            self.state[keys.STATUS()] = status
        return self.state.get(keys.STATUS())
    
    def id(self, id: str = None) -> str:
        if id is not None:
            new_id = id.strip()
            if len(new_id) == 0:
                raise ValueError("id must not be empty")
            self.state[keys.ID()] = new_id
        return self.state.get(keys.ID())
    
    def broker_data(self, broker_data: str = None) -> str:
        if broker_data is not None:
            json.loads(broker_data)
            self.state[keys.BROKER_DATA()] = broker_data
        return self.state.get(keys.BROKER_DATA())
    
    def high_interval(self, high_intv: str = None) -> str:
        ## this should come from merchant config
        if high_intv is not None:
            self.state[keys.HIGH_INTERVAL()] = high_intv
        return self.state.get(keys.HIGH_INTERVAL())
    
    def low_interval(self, low_intv: str = None) -> str:
        ## this should come from merchant config
        if low_intv is not None:
            self.state[keys.LOW_INTERVAL()] = low_intv
        return self.state.get(keys.LOW_INTERVAL())
        
    def last_action_time(self, latest: int = None) -> int:
        ## this should come from storage
        if latest is not None:
            self.state[keys.LAST_ACTION_TIME()] = latest
        return int(self.state.get(keys.LAST_ACTION_TIME()))
    
    def rest_interval_minutes(self, rest_interval:int = None) -> int:
        ## this should come from merchant config
        if rest_interval is not None:
            if rest_interval < 0:
                raise ValueError(f"rest_interval must be >= 0, but received {rest_interval}")
            self.state[keys.REST_INTERVAL()] = rest_interval
        return int(self.state.get(keys.REST_INTERVAL()))

    def merchant_id(self, signal: MerchantSignal=None) -> str:
        if signal is not None:
            self._id = self._new_merchant_id_from_signal(signal=signal)
        return self._id
    
    def suggested_stoploss(self, stoploss: float = None) -> float:
        if stoploss is not None:
            self.state[keys.STOPLOSS()] = stoploss
        return float(self.state.get(keys.STOPLOSS()))
    
    def takeprofit_percent(self, takeprofit: float = None) -> float:
        if takeprofit is not None:
            self.state[keys.TAKEPROFIT()] = takeprofit
        return float(self.state.get(keys.TAKEPROFIT()))
    
    def version(self, version: int = None) -> str:
        if version is not None:
            if version < 1:
                raise ValueError("version must be >= 1")
            self.state[keys.VERSION()] = version
        return self.state.get(keys.VERSION())
    
    def ticker(self, ticker: str = None) -> str:
        if ticker is not None:
            ticker = ticker.strip()
            if len(ticker) == 0:
                raise ValueError("ticker must not be empty")
            self.state[keys.TICKER()] = ticker
        return self.state.get(keys.TICKER())
    
    def dry_run(self) -> bool:
        return cfg.DRY_RUN_MODE()
    