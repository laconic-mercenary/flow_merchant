
from broker_exceptions import OversoldError, InvalidQuantityScale, ApiError
from order_strategy import OrderStrategy, HandleResult
from order_capable import Broker, MarketOrderable, OrderCancelable, DryRunnable, StopMarketOrderable
from live_capable import LiveCapable, BalancesResult, AssetBalance
from merchant_order import Order, MerchantParams, SubOrder, SubOrders, Metadata, Projections, Results
from merchant_signal import MerchantSignal
from merchant_keys import keys
from transactions import calculate_stop_loss, calculate_take_profit, calculate_pnl, Transaction, TransactionAction
from utils import unix_timestamp_ms, pause_thread, null_or_empty

import logging
import typing
import uuid

class BracketStrategy(OrderStrategy):

    def _current_price(self, broker:Broker, ticker:str) -> float:
        if not isinstance(broker, LiveCapable):
            raise ValueError("Broker is not LiveCapable")
        results = broker.get_current_prices([ticker])
        if ticker not in results:
            raise ValueError(f"Expected ticker {ticker} to be in current prices {results}")
        return results.get(ticker)
    
    def handle_price_change(self, broker:Broker, order:Order, merchant_params:dict = {}) -> HandleResult:
        return HandleResult(target_order=order, complete=False)

    def place_orders(self, broker:Broker, signal: MerchantSignal, merchant_state:dict, merchant_params:dict = {}) -> Order:
        if broker is None:
            raise ValueError("broker is required")
        if signal is None:
            raise ValueError("signal is required")
        if not isinstance(signal, MerchantSignal):
            raise TypeError(f"signal must be an instance of MerchantSignal, not {type(signal)}")
        
        ticker:str = signal.ticker()
        contracts:float = signal.contracts()
        take_profit_percent:float = signal.takeprofit_percent()
        stop_loss_percent:float = signal.suggested_stoploss()
        dry_run_mode:bool = merchant_params.get("dry_run", False)

        if null_or_empty(ticker):
            raise ValueError("Ticker is required")
        if contracts <= 0.0:
            raise ValueError("Contracts must be greater than 0")
        if take_profit_percent <= 0.0:
            raise ValueError("Take profit percent must be greater than 0")
        if stop_loss_percent <= 0.0:
            raise ValueError("Stop loss percent must be greater than 0")

        if dry_run_mode:
            if not isinstance(broker, DryRunnable):
                raise ValueError("Broker is set to dry run mode but is not a DryRunnable")
        else:
            if not isinstance(broker, MarketOrderable):
                raise ValueError("Broker is not a MarketOrderable")
            if not isinstance(broker, LiveCapable):
                raise ValueError("Broker is not a LiveCapable")

        execute_market_order:typing.Callable = broker.place_market_order_test if dry_run_mode else broker.place_market_order
        execute_stop_order:typing.Callable = None
        if isinstance(broker, StopMarketOrderable):
            execute_stop_order = broker.place_limit_order_test if dry_run_mode else broker.place_limit_order
        
        logging.info(f"placing market order for {contracts} contracts for {ticker}")
        market_order_rx:dict = execute_market_order(
            ticker=ticker,
            contracts=contracts,
            action="BUY"
        )
        
        market_order_info:dict = broker.standardize_market_order(market_order_rx)
        logging.info(f"broker - market order response: {market_order_rx}")

        ## TODO - consider selling here and abandoning the order
        if keys.bkrdata.order.suborders.props.ID() not in market_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.ID()} not found in market order data {market_order_info}")
        if keys.bkrdata.order.suborders.props.PRICE() not in market_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.PRICE()} not found in market order data {market_order_info}")
        if keys.bkrdata.order.suborders.props.CONTRACTS() not in market_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.CONTRACTS()} not found in market order data {market_order_info}")

        main_order_price:float = market_order_info.get(keys.bkrdata.order.suborders.props.PRICE())
        main_order_contracts:float = market_order_info.get(keys.bkrdata.order.suborders.props.CONTRACTS())

        stop_loss_price:float = calculate_stop_loss(
            close_price=main_order_price,
            stop_loss_percent=stop_loss_percent
        )
        take_profit_price:float = calculate_take_profit(
            close_price=main_order_price,
            take_profit_percent=take_profit_percent
        )

        suborder_main:SubOrder = SubOrder(
            id = market_order_info.get(keys.bkrdata.order.suborders.props.ID()),
            api_rx = market_order_rx,
            time = market_order_info.get("timestamp"),
            price = main_order_price,
            contracts = main_order_contracts
        )
        suborder_stop:SubOrder = SubOrder(
            id = f"{suborder_main.id}stop",
            api_rx = {},
            time = suborder_main.time,
            price = stop_loss_price,
            contracts = suborder_main.contracts
        )
        suborder_profit:SubOrder = SubOrder(
            id = f"{suborder_main.id}profit",
            api_rx = {},
            time = suborder_main.time,
            price = take_profit_price,
            contracts = suborder_main.contracts
        )

        if execute_stop_order is not None:
            logging.info(f"placing stop order for {contracts} contracts @ {stop_loss_price} for {ticker}")
            stop_loss_order_rx:dict = execute_stop_order(
                ticker=ticker,
                action="SELL",
                contracts=main_order_contracts,
                limit=stop_loss_price
            )
            stop_loss_order_info:dict = broker.standardize_limit_order(stop_loss_order_rx)
        
            if keys.bkrdata.order.suborders.props.ID() not in stop_loss_order_info:
                raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.ID()} not found in stop loss order data {stop_loss_order_info}")
            if keys.bkrdata.order.suborders.props.PRICE() not in stop_loss_order_info:
                raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.PRICE()} not found in stop loss order data {stop_loss_order_info}")

            stop_loss_order_price:float = stop_loss_order_info.get(keys.bkrdata.order.suborders.props.PRICE())
            
            suborder_stop:SubOrder = SubOrder(
                id = suborder_stop.id,
                api_rx = stop_loss_order_rx,
                time = stop_loss_order_info.get("timestamp"),
                price = stop_loss_order_price,
                contracts = main_order_contracts
            )

        new_order:Order = Order(
            results=None,
            projections=None,
            ticker = ticker,
            metadata = Metadata(
                id = str(uuid.uuid4()),
                time_created = unix_timestamp_ms(),
                is_dry_run = dry_run_mode,
                security_type = signal.security_type(),
                tags=signal.tags()
            ),
            merchant_params = MerchantParams(
                high_interval = signal.high_interval(),
                low_interval = signal.low_interval(),
                stoploss_percent = signal.suggested_stoploss(),
                takeprofit_percent = signal.takeprofit_percent(),
                notes = signal.notes(),
                version = signal.version(),
                strategy = signal.strategy()
            ),
            sub_orders = SubOrders(
                main_order = suborder_main,
                stop_loss = suborder_stop,
                take_profit = suborder_profit
            )
        )
        pnl = calculate_pnl(
            contracts=new_order.sub_orders.main_order.contracts,
            main_price=new_order.sub_orders.main_order.price,
            stop_price=new_order.sub_orders.stop_loss.price,
            profit_price=new_order.sub_orders.take_profit.price
        )
        new_order.projections = Projections(
            profit_without_fees = pnl.get("profit_without_fees"),
            loss_without_fees = pnl.get("loss_without_fees")
        )
        new_order.results = Results(
            transaction=None,
            complete=False,
            additional_data={}
        )
        return new_order
    
    def handle_take_profit(self, broker:Broker, order:Order, merchant_params:dict = {}) -> HandleResult:
        if broker is None:
            raise ValueError("broker is required")
        if not isinstance(broker, MarketOrderable):
            raise ValueError(f"Broker is not market orderable - broker {type(broker)} - {broker.get_name()} supports the following interfaces: {broker.__class__.__subclasses__()}")
        if order is None:
            raise ValueError("order is required")
        if not isinstance(order, Order):
            raise TypeError(f"order must be an instance of Order, not {type(order)}")
        if merchant_params is None:
            merchant_params = {}
        dry_run_mode = order.metadata.is_dry_run
        if dry_run_mode:
            if not isinstance(broker, DryRunnable):
                raise ValueError("Running in dry run mode but broker is NOT dry runnable")

        results = HandleResult(target_order=order, complete=False)
        
        if isinstance(broker, StopMarketOrderable) and isinstance(broker, OrderCancelable):
            if merchant_params.get("_skip_cancel", False):
                execute_cancel_order = broker.cancel_order_test if dry_run_mode else broker.cancel_order
                cancel_result = execute_cancel_order(
                    ticker=order.ticker, 
                    order_id=order.sub_orders.stop_loss.id
                )
                results.additional_data.update({ "cancel_result": cancel_result })

        if not merchant_params.get("_skip_market_sell", False):
            sell_result = self.execute_market_sell(
                                ticker=order.ticker,
                                contracts=order.sub_orders.main_order.contracts,
                                broker=broker,
                                dry_run_mode=order.metadata.is_dry_run
                            )
            results.complete = True
            results.transaction = sell_result
            results.additional_data.update({
                "sell_result": sell_result.__dict__
            })
            order.results = Results(
                transaction=sell_result,
                complete=True,
                additional_data=results.additional_data.copy()
            )
        return results

    def handle_stop_loss(self, broker:Broker, order:Order, merchant_params:dict = {}) -> HandleResult:
        if broker is None:
            raise ValueError("broker is required")
        if not isinstance(broker, Broker):
            raise TypeError(f"broker must be an instance of Broker, not {type(broker)}")
        if order is None:
            raise ValueError("order is required")
        if not isinstance(order, Order):
            raise TypeError(f"order must be an instance of Order, not {type(order)}")
        if merchant_params is None:
            merchant_params = {}
        merchant_params.update({"_skip_cancel": True})
        if isinstance(broker, StopMarketOrderable):
            ### No need to market order SELL because the stop-loss would handle it
            merchant_params.update({"_skip_market_sell": True})
        results = BracketStrategy.handle_take_profit(self=self, broker=broker, order=order, merchant_params=merchant_params)
        results.complete = True
        return results
    
    def execute_market_sell(self, ticker:str, contracts:float, broker:Broker, dry_run_mode:bool) -> Transaction:
        if not isinstance(broker, MarketOrderable):
            raise TypeError(f"Broker {broker} does not support market orders")
        if not isinstance(contracts, float):
            raise TypeError(f"Contracts must be a float, got {type(contracts)}")
        if not isinstance(ticker, str):
            raise TypeError(f"Ticker must be a string, got {type(ticker)}")
        execute_market_order = broker.place_market_order
        standardize_market_order = broker.standardize_market_order
        if dry_run_mode:
            if not isinstance(broker, DryRunnable):
                raise TypeError(f"Broker {broker} does not support dry run mode")
            execute_market_order = broker.place_market_order_test
        attempts = 4
        pause = 1.0
        results = {}
        try:
            results = self._execute_market_sell_with_backoff(
                        ticker=ticker,
                        contracts=contracts, 
                        execute_fn=execute_market_order,
                        standardize_fn=standardize_market_order,
                        attempts=attempts,
                        pause_in_secs=pause
                    )
        except OversoldError as oe:
            logging.warning(f"OversoldError for {ticker} - will reattempt with actual quantity")
            contracts = self._get_quantity_for_ticker(ticker=ticker, broker=broker)
            logging.warning(f"After oversold error, retrying for actual quantity: {contracts}, for ticker {ticker}")
            results = self._execute_market_sell_with_backoff(
                        ticker=ticker,
                        contracts=contracts, 
                        execute_fn=execute_market_order,
                        standardize_fn=standardize_market_order,
                        attempts=attempts,
                        pause_in_secs=pause
                    )
        if "price" not in results:
            raise ValueError(f"No price found in results: {results}")
        if "contracts" not in results:
            raise ValueError(f"No contracts found in results: {results}")
        return Transaction(
                action=TransactionAction.SELL, 
                quantity=results.get("contracts"), 
                price=results.get("price")
            )
    
    def _get_quantity_for_ticker(self, ticker:str, broker:Broker) -> float:
        if not isinstance(broker, LiveCapable):
            raise TypeError(f"Broker {broker} does not support live trading")
        result:BalancesResult = broker.get_balances()
        balances:dict[str, AssetBalance] = result.balances
        remaining_quantity = None
        for asset_name in balances.keys():
            if ticker.startswith(asset_name):
                remaining_quantity = balances.get(asset_name).available
                break
        if remaining_quantity is None:
            raise ValueError(f"Expected  ticker {ticker} to be in balances: {balances}")
        return remaining_quantity
    
    def _execute_market_sell_with_backoff(self, ticker:str, contracts:float, execute_fn:typing.Callable, standardize_fn:typing.Callable, attempts:int = 4, pause_in_secs:float = 1.0, tracking_id:str = None) -> dict:
        backoff_count = attempts
        contracts_to_sell = contracts
        last_exception:Exception = None
        while backoff_count > 0:
            try:
                return standardize_fn(
                    execute_fn(
                        ticker=ticker,
                        action="SELL",
                        contracts=contracts_to_sell,
                        tracking_id=tracking_id
                    )
                )
            except OversoldError as oe:
                ## some exceptions we want immediate action on
                raise oe
            except InvalidQuantityScale as iqs:
                raise iqs
            except Exception as e:
                logging.error(f"Error on SELLing order {ticker}, details: {e}. Attempts left: {backoff_count}")
                pause_thread(seconds=pause_in_secs)
                last_exception = e
            finally:
                backoff_count = backoff_count - 1
                
        raise ValueError(f"{ticker} failed to SELL, despite multiple attempts and a contract decrease to {contracts_to_sell}, originally {contracts}") from last_exception
