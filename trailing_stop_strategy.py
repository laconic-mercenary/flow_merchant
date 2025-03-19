
from bracket_strategy import BracketStrategy
from order_capable import Broker, MarketOrderable, LimitOrderable, OrderCancelable, DryRunnable
from live_capable import LiveCapable
from merchant_keys import keys as mkeys
from merchant_order import Order, SubOrders, SubOrder, Projections
from merchant_signal import MerchantSignal
from transactions import calculate_pnl_from_order
from utils import unix_timestamp_secs, unix_timestamp_ms

import logging

class keys:
    @staticmethod
    def TRAILING():
        return "trailing"    

class consts:
    @staticmethod
    def ALLOWED_SELL_PERCENTS():
        return [0.0, 0.25, 0.5]

class TrailingStopStrategy(BracketStrategy):

    def place_orders(self, broker:Broker, signal:MerchantSignal, merchant_state:dict, merchant_params:dict = {}) -> Order:
        logging.debug("place_orders")
        return super().place_orders(broker, signal, merchant_state, merchant_params)

    def handle_take_profit(self, broker:Broker, order:Order, merchant_params:dict = {}) -> dict:
        logging.debug("handle_take_profit")
        ticker = order.ticker
        current_price = merchant_params.get("current_price")
        dry_run_mode = merchant_params.get("dry_run_order")

        new_stop_loss, new_take_profit = self._determine_new_levels(
                                            current_price=current_price,
                                            old_order_price=order.sub_orders.main_order.price,
                                            old_stop_loss=order.sub_orders.stop_loss.price,
                                            old_take_profit=order.sub_orders.take_profit.price
                                        )
        
        logging.info(f"creating new trailing stop: ticker={ticker}, old_stop_loss={order.sub_orders.stop_loss.price}, old_take_profit={order.sub_orders.take_profit.price} new_stop_loss={new_stop_loss}, new_take_profit={new_take_profit}")

        results = self._handle_orders(
                        broker=broker, 
                        ticker=ticker, 
                        sell_contracts=0.0, 
                        new_stop_loss=new_stop_loss, 
                        new_take_profit=new_take_profit, 
                        order=order,
                        dry_run_mode=dry_run_mode
                    )
        
        #order.sub_orders.stop_loss.time = unix_timestamp_ms()
        #order.sub_orders.stop_loss.price = new_stop_loss
        #order.sub_orders.stop_loss.api_rx = results.get("new_stop_loss_order_api")
        #order.sub_orders.take_profit.price = new_take_profit
        
        order.sub_orders = SubOrders(
            main_order=SubOrder(
                id=order.sub_orders.main_order.id,
                price=order.sub_orders.main_order.price,
                contracts=order.sub_orders.main_order.contracts,
                time=order.sub_orders.main_order.time,
                api_rx=order.sub_orders.main_order.api_rx
            ),
            stop_loss=SubOrder(
                id=order.sub_orders.stop_loss.id,
                price=new_stop_loss,
                contracts=order.sub_orders.stop_loss.contracts,
                time=unix_timestamp_ms(),
                api_rx=results.get("new_stop_loss_order_api")
            ),
            take_profit=SubOrder(
                id=order.sub_orders.take_profit.id,
                price=new_take_profit,
                contracts=order.sub_orders.take_profit.contracts,
                time=unix_timestamp_ms(),
                api_rx={}
            )
        )
        
        pnl = calculate_pnl_from_order(
                    order=order, 
                    sell_amount=None, 
                    current_price=current_price
                )
        
        order.projections = Projections(
            profit_without_fees=pnl.get("profit_without_fees"),
            loss_without_fees=pnl.get("loss_without_fees")
        )

        return results
        
    def _handle_orders(self, broker:Broker, ticker:str, sell_contracts:float, new_stop_loss:float, new_take_profit:float, order:Order, dry_run_mode:bool = False) -> dict:
        logging.debug("_handle_orders")
        if not isinstance(broker, MarketOrderable):
            raise ValueError(f"broker must implement MarketOrderable")
        if not isinstance(broker, OrderCancelable):
            raise ValueError(f"broker must implement OrderCancelable")
        if not isinstance(broker, LimitOrderable):
            raise ValueError(f"broker must implement LimitOrderable")
        if not isinstance(broker, DryRunnable):
            raise ValueError(f"broker must implement DryRunnable")
        
        execute_market_order = broker.place_market_order_test if dry_run_mode else broker.place_market_order
        execute_limit_order = broker.place_limit_order_test if dry_run_mode else broker.place_limit_order
        execute_cancel_order = broker.cancel_order_test if dry_run_mode else broker.cancel_order

        results = {}
        if sell_contracts != 0.0:
            partial_sell_result_raw = execute_market_order(
                                        ticker=ticker, 
                                        action="SELL", 
                                        contracts=sell_contracts, 
                                        broker_params={},
                                        tracking_id=f"{ticker}_{unix_timestamp_secs()}_partial_sell"
                                    )
            partial_sell_result = broker.standardize_market_order(market_order_result=partial_sell_result_raw)
            results.update({ 
                "main_order_sell": partial_sell_result,
                "main_order_sell_api": partial_sell_result_raw 
            })
        
        stop_loss_cancel_result = execute_cancel_order(
                                        ticker=ticker,
                                        order_id=order.sub_orders.stop_loss.id,
                                    )
        results.update({
            "stop_loss_cancel": stop_loss_cancel_result
        })
        
        remaining_contracts = order.sub_orders.main_order.contracts - sell_contracts
        new_stop_loss_result_raw = execute_limit_order(
                                    ticker=ticker, 
                                    action="SELL", 
                                    contracts=remaining_contracts, 
                                    limit=new_stop_loss, 
                                    broker_params={}
                                )
        new_stop_loss_result = broker.standardize_limit_order(limit_order_result=new_stop_loss_result_raw)
        results.update({
            "new_stop_loss_order": new_stop_loss_result,
            "new_stop_loss_order_api": new_stop_loss_result_raw
        })
        return results
    
    def _determine_new_levels(self, current_price:float, old_order_price:float, old_stop_loss:float, old_take_profit:float) -> tuple:
        grid_diff = old_take_profit - old_order_price
        new_stop_loss = current_price - grid_diff
        new_take_profit = current_price + grid_diff
        return (new_stop_loss, new_take_profit)
        