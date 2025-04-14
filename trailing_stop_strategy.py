
from bracket_strategy import BracketStrategy
from broker_exceptions import OrderAlreadyFilledError
from order_capable import Broker, MarketOrderable, LimitOrderable, OrderCancelable, DryRunnable, StopMarketOrderable
from order_strategy import HandleResult
from live_capable import LiveCapable
from merchant_keys import keys as mkeys
from merchant_order import Order, SubOrders, SubOrder, Projections, Results
from merchant_signal import MerchantSignal
from transactions import calculate_pnl
from utils import unix_timestamp_secs, unix_timestamp_ms

import logging

class TrailingStopStrategy(BracketStrategy):

    def place_orders(self, broker:Broker, signal:MerchantSignal, merchant_state:dict, merchant_params:dict = {}) -> Order:
        return super().place_orders(broker, signal, merchant_state, merchant_params)
    
    def handle_stop_loss(self, broker:Broker, order:Order, merchant_params:dict = {}) -> HandleResult:
        return super().handle_stop_loss(broker, order, merchant_params)

    def handle_take_profit(self, broker:Broker, order:Order, merchant_params:dict = {}) -> HandleResult:
        ticker = order.ticker
        current_price = merchant_params.get("current_price")
        dry_run_mode = merchant_params.get("dry_run_order")

        new_stop_loss, new_take_profit = self._determine_new_levels(
                                            current_price=current_price,
                                            order=order,
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
        
        if results.complete:
            return results
        
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
                api_rx=results.additional_data.get("new_stop_loss_order_api", {})
            ),
            take_profit=SubOrder(
                id=order.sub_orders.take_profit.id,
                price=new_take_profit,
                contracts=order.sub_orders.take_profit.contracts,
                time=unix_timestamp_ms(),
                api_rx={}
            )
        )
        
        pnl = calculate_pnl(
                    contracts=order.sub_orders.main_order.contracts,
                    main_price=order.sub_orders.main_order.price,
                    stop_price=order.sub_orders.stop_loss.price,
                    profit_price=order.sub_orders.take_profit.price,
                    current_price=current_price
                )
        
        order.projections = Projections(
            profit_without_fees=pnl.get("profit_without_fees"),
            loss_without_fees=pnl.get("loss_without_fees")
        )
        order.results = Results(
            transaction=None,
            complete=False
        )

        return results
        
    def _handle_orders(self, broker:Broker, ticker:str, sell_contracts:float, new_stop_loss:float, new_take_profit:float, order:Order, dry_run_mode:bool = False) -> HandleResult:
        if not isinstance(broker, MarketOrderable):
            raise ValueError(f"broker must implement MarketOrderable")
        execute_market_order = broker.place_market_order
        standardize_market_order = broker.standardize_market_order
        
        if dry_run_mode:
            if not isinstance(broker, DryRunnable):
                raise ValueError(f"broker must implement DryRunnable")
            execute_market_order = broker.place_market_order_test
        
        results = HandleResult(target_order=order, complete=False)
        if sell_contracts != 0.0:
            partial_sell_result_raw = self._execute_market_sell_with_backoff(
                                            ticker=ticker, 
                                            contracts=sell_contracts,
                                            execute_fn=execute_market_order,
                                            standardize_fn=standardize_market_order,
                                            tracking_id=f"{ticker}_{unix_timestamp_secs()}_partialsell"
                                        )
            partial_sell_result = broker.standardize_market_order(market_order_result=partial_sell_result_raw)
            results.additional_data.update({ 
                "main_order_sell": partial_sell_result,
                "main_order_sell_api": partial_sell_result_raw 
            })
        
        if isinstance(broker, StopMarketOrderable):
            execute_limit_order = broker.place_limit_order_test if dry_run_mode else broker.place_limit_order
            execute_cancel_order = broker.cancel_order_test if dry_run_mode else broker.cancel_order

            try:
                stop_loss_cancel_result = execute_cancel_order(
                                            ticker=ticker,
                                            order_id=order.sub_orders.stop_loss.id,
                                        )
                results.additional_data.update({ "stop_loss_cancel": stop_loss_cancel_result })
            except OrderAlreadyFilledError as e:
                ### cancelling the limit order (the stop loss) failed because apparently
                ### we were too late. has only happened once so far
                logging.warning(f"stop loss order already filled, skipping cancel: {e}")
                results.complete = True
                return results
            
            remaining_contracts = order.sub_orders.main_order.contracts - sell_contracts
            new_stop_loss_result_raw = execute_limit_order(
                                        ticker=ticker, 
                                        action="SELL", 
                                        contracts=remaining_contracts, 
                                        limit=new_stop_loss, 
                                        broker_params={}
                                    )
            new_stop_loss_result = broker.standardize_limit_order(limit_order_result=new_stop_loss_result_raw)
            results.additional_data.update({
                "new_stop_loss_order": new_stop_loss_result,
                "new_stop_loss_order_api": new_stop_loss_result_raw
            })

        return results
    
    def _determine_new_levels(self, current_price:float, order:Order) -> tuple:
        ### important: takeprofit_percent() is > 1.0 (for readability), thus divide by 100.0 first
        ### this was for readability when configuring the trading view alerts 
        pct_deff = order.merchant_params.takeprofit_percent / 100.0
        new_stop_loss = current_price - (current_price * pct_deff)
        new_take_profit = current_price + (current_price * pct_deff)
        return (new_stop_loss, new_take_profit)
        
if __name__ == "__main__":
    import unittest

    class Test(unittest.TestCase):
        def test_determine_new_levels(self):
            inst = TrailingStopStrategy()
            cur_price = 100.0
            
    unittest.main()
