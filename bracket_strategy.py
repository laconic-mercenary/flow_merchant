
from order_strategy import OrderStrategy
from order_capable import Broker, MarketOrderable, LimitOrderable, OrderCancelable, DryRunnable
from live_capable import LiveCapable
from merchant_signal import MerchantSignal
from merchant_keys import keys
from transactions import calculate_stop_loss, calculate_take_profit, calculate_pnl_from_order
from utils import unix_timestamp_secs, unix_timestamp_ms

import logging
import uuid

class BracketStrategy(OrderStrategy):

    def place_orders(self, broker:Broker, signal: MerchantSignal, merchant_state:dict, merchant_params:dict = {}) -> dict:
        ticker = signal.ticker()
        contracts = signal.contracts()
        take_profit_percent = signal.takeprofit_percent()
        stop_loss_percent = signal.suggested_stoploss()
        dry_run_mode = merchant_params.get("dry_run", False)

        if dry_run_mode:
            if not isinstance(broker, DryRunnable):
                raise ValueError("Broker is set to dry run mode but is not a DryRunnable")
        else:
            if not isinstance(broker, MarketOrderable):
                raise ValueError("Broker is not a MarketOrderable")
            if not isinstance(broker, LimitOrderable):
                raise ValueError("Broker is not a LimitOrderable")
            if not isinstance(broker, LiveCapable):
                raise ValueError("Broker is not a LiveCapable")

        execute_market_order = broker.place_market_order_test if dry_run_mode else broker.place_market_order
        execute_limit_order = broker.place_limit_order_test if dry_run_mode else broker.place_limit_order
        
        logging.info(f"placing market order for {contracts} contracts for {ticker}")
        market_order_rx = execute_market_order(
            ticker=ticker,
            contracts=contracts,
            action="BUY"
        )
        market_order_info = broker.standardize_market_order(market_order_rx)
        logging.info(f"broker - market order response: {market_order_rx}")

        ## TODO - consider selling here and abandoning the order
        if keys.bkrdata.order.suborders.props.ID() not in market_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.ID()} not found in market order data {market_order_info}")

        if not dry_run_mode:
            market_order_info = broker.get_order(
                ticker=ticker, 
                order_id=market_order_info.get(keys.bkrdata.order.suborders.props.ID())
            )
        
        if keys.bkrdata.order.suborders.props.ID() not in market_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.ID()} not found in market order data {market_order_info}")
        if keys.bkrdata.order.suborders.props.PRICE() not in market_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.PRICE()} not found in market order data {market_order_info}")
        if keys.bkrdata.order.suborders.props.CONTRACTS() not in market_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.CONTRACTS()} not found in market order data {market_order_info}")

        main_order_price = market_order_info.get(keys.bkrdata.order.suborders.props.PRICE())
        main_order_contracts = market_order_info.get(keys.bkrdata.order.suborders.props.CONTRACTS())

        stop_loss_price = calculate_stop_loss(
            close_price=main_order_price,
            stop_loss_percent=stop_loss_percent
        )

        logging.info(f"placing limit order for {contracts} contracts @ {stop_loss_price} for {ticker}")
        stop_loss_order_rx = execute_limit_order(
            ticker=ticker,
            action="SELL",
            contracts=main_order_contracts,
            limit=stop_loss_price
        )
        stop_loss_order_info = broker.standardize_limit_order(stop_loss_order_rx)
    
        if keys.bkrdata.order.suborders.props.ID() not in stop_loss_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.ID()} not found in stop loss order data {stop_loss_order_info}")
        if keys.bkrdata.order.suborders.props.PRICE() not in stop_loss_order_info:
            raise ValueError(f"critical key {keys.bkrdata.order.suborders.props.PRICE()} not found in stop loss order data {stop_loss_order_info}")
        
        take_profit_price = calculate_take_profit(
            close_price=main_order_price,
            take_profit_percent=take_profit_percent
        )

        stop_loss_order_price = stop_loss_order_info.get(keys.bkrdata.order.suborders.props.PRICE())

        ## NOTE: this data is persisted in the merchant state
        new_order = {
            keys.bkrdata.order.METADATA(): {
                keys.bkrdata.order.metadata.ID(): str(uuid.uuid4()),
                "time_created": unix_timestamp_ms(),
                keys.bkrdata.order.metadata.DRY_RUN(): dry_run_mode,
                "high_interval": signal.high_interval(),
                "low_interval": signal.low_interval(),
                "version": signal.version()
            },
            keys.bkrdata.TICKER(): ticker,
            keys.bkrdata.order.SUBORDERS(): {
                keys.bkrdata.order.suborders.MAIN_ORDER(): {
                    keys.bkrdata.order.suborders.props.ID(): market_order_info.get(keys.bkrdata.order.suborders.props.ID()),
                    keys.bkrdata.order.suborders.props.API_RX(): market_order_rx,
                    keys.bkrdata.order.suborders.props.TIME(): market_order_info.get("timestamp"),
                    keys.bkrdata.order.suborders.props.CONTRACTS(): main_order_contracts,
                    keys.bkrdata.order.suborders.props.PRICE(): main_order_price,
                },
                keys.bkrdata.order.suborders.STOP_LOSS(): {
                    keys.bkrdata.order.suborders.props.ID(): stop_loss_order_info.get(keys.bkrdata.order.suborders.props.ID()),
                    keys.bkrdata.order.suborders.props.API_RX(): stop_loss_order_rx,
                    keys.bkrdata.order.suborders.props.TIME(): stop_loss_order_info.get("timestamp"),
                    keys.bkrdata.order.suborders.props.PRICE(): stop_loss_order_price
                },
                keys.bkrdata.order.suborders.TAKE_PROFIT(): {
                    keys.bkrdata.order.suborders.props.PRICE(): take_profit_price
                }
            }
        }
        pnl = calculate_pnl_from_order(order=new_order)
        new_order.update({
            keys.bkrdata.order.PROJECTIONS(): {
                "profit_without_fees": pnl.get("profit_without_fees"),
                "loss_without_fees" : pnl.get("loss_without_fees")
            }
        })
        return new_order
    
    def handle_take_profit(self, broker:Broker, order:dict, merchant_params:dict = {}) -> dict:
        if not isinstance(broker, MarketOrderable):
            raise ValueError("Broker is not market orderable")
        if not isinstance(broker, OrderCancelable):
            raise ValueError("Broker is not order cancelable")
        dry_run_mode = merchant_params.get("dry_run", False)
        if dry_run_mode:
            if not isinstance(broker, DryRunnable):
                raise ValueError("Running in dry run mode but broker is NOT dry runnable")
        
        execute_market_order = broker.place_market_order_test if dry_run_mode else broker.place_market_order
        execute_cancel_order = broker.cancel_order_test if dry_run_mode else broker.cancel_order

        ticker = order.get(keys.bkrdata.TICKER())
        suborders = order.get(keys.bkrdata.order.SUBORDERS())
        stoploss_order_id = suborders[keys.bkrdata.order.suborders.STOP_LOSS()].get(
            keys.bkrdata.order.suborders.props.ID()
        )
        contracts = suborders[keys.bkrdata.order.suborders.MAIN_ORDER()].get(
            keys.bkrdata.order.suborders.props.CONTRACTS()
        )

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
        
        logging.info(f"Results: cancel order - {cancel_result}, sell result - {sell_result}")
        return {
            "complete": True,
            "ticker": ticker,
            "order": order,
            "cancel_result": cancel_result,
            "sell_result": sell_result
        }
