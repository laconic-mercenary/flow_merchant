
from bracket_strategy import BracketStrategy
from order_capable import Broker, MarketOrderable, LimitOrderable, OrderCancelable, DryRunnable
from live_capable import LiveCapable
from merchant_keys import keys as mkeys
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

    def place_orders(self, broker:Broker, signal:MerchantSignal, merchant_state:dict, merchant_params:dict = {}) -> dict:
        logging.debug("place_orders")
        order_results = super().place_orders(broker, signal, merchant_state, merchant_params)
        return order_results

    def handle_take_profit(self, broker:Broker, order:dict, merchant_params:dict = {}) -> dict:
        logging.debug("handle_take_profit")
        suborders = order.get(mkeys.bkrdata.order.SUBORDERS())
        take_profit_order = suborders.get(mkeys.bkrdata.order.suborders.TAKE_PROFIT())
        stop_loss_order = suborders.get(mkeys.bkrdata.order.suborders.STOP_LOSS())
        current_price = merchant_params.get("current_price")
        ticker = order.get(mkeys.bkrdata.order.TICKER())
        dry_run_mode = merchant_params.get("dry_run_order")

        take_profit_price = take_profit_order.get(mkeys.bkrdata.order.suborders.props.PRICE())
        stop_loss_price = stop_loss_order.get(mkeys.bkrdata.order.suborders.props.PRICE())
        new_stop_loss, new_take_profit = self._determine_new_levels(
                                            current_price=current_price, 
                                            old_stop_loss=stop_loss_price,
                                            old_take_profit=take_profit_price
                                        )
        
        logging.info(f"creating new trailing stop: ticker={ticker}, old_stop_loss={stop_loss_price}, old_take_profit={take_profit_price} new_stop_loss={new_stop_loss}, new_take_profit={new_take_profit}")

        results = self._handle_orders(
                        broker=broker, 
                        ticker=ticker, 
                        sell_contracts=0.0, 
                        new_stop_loss=new_stop_loss, 
                        new_take_profit=new_take_profit, 
                        suborders=suborders,
                        dry_run_mode=dry_run_mode
                    )
        
        stop_loss_order[mkeys.bkrdata.order.suborders.props.TIME()] = unix_timestamp_ms()
        stop_loss_order[mkeys.bkrdata.order.suborders.props.PRICE()] = new_stop_loss
        stop_loss_order[mkeys.bkrdata.order.suborders.props.API_RX()] = results.get("new_stop_loss_order_api")
        take_profit_order[mkeys.bkrdata.order.suborders.props.PRICE()] = new_take_profit

        pnl = calculate_pnl_from_order(
                    order=order, 
                    sell_amount=None, 
                    current_price=current_price
                )
        
        projections = order.get(mkeys.bkrdata.order.PROJECTIONS())
        projections[mkeys.bkrdata.order.projections.PROFIT_WITHOUT_FEES()] = pnl.get("profit_without_fees")
        projections[mkeys.bkrdata.order.projections.LOSS_WITHOUT_FEES()] = pnl.get("loss_without_fees")

        return results
        
    def _handle_orders(self, broker:Broker, ticker:str, sell_contracts:float, new_stop_loss:float, new_take_profit:float, suborders:dict, dry_run_mode:bool = False) -> dict:
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
        
        stop_loss_order = suborders.get(mkeys.bkrdata.order.suborders.STOP_LOSS())
        stop_loss_order_id = stop_loss_order.get(mkeys.bkrdata.order.suborders.props.ID())
        stop_loss_cancel_result = execute_cancel_order(
                                        ticker=ticker,
                                        order_id=stop_loss_order_id,
                                    )
        results.update({
            "stop_loss_cancel": stop_loss_cancel_result
        })
        
        main_order = suborders.get(mkeys.bkrdata.order.suborders.MAIN_ORDER())
        main_order_contracts = main_order.get(mkeys.bkrdata.order.suborders.props.CONTRACTS())
        remaining_contracts = main_order_contracts - sell_contracts
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
        
    def _determine_new_levels(self, current_price:float, old_stop_loss:float, old_take_profit:float) -> tuple:
        grid_diff = old_take_profit - old_stop_loss
        new_stop_loss = old_stop_loss
        new_take_profit = old_take_profit
        while current_price >= new_take_profit:
            new_stop_loss = new_stop_loss + grid_diff
            new_take_profit = new_stop_loss + grid_diff
        return (new_stop_loss, new_take_profit)
    