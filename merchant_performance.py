from ledger import Ledger, Entry
from ledger_analytics import Analytics
from merchant_order import Order

import logging

class LedgerTransactionsResult:
    def __init__(self, metrics:dict):
        self.metrics = metrics

    def as_dict(self) -> dict:
        return self.metrics

class LedgerOrdersResult:
    def __init__(self):
        self.data = {
            "assets": {}
        }

    def add_ledger_entry(self, entry:Entry) -> None:
        ticker = entry.name
        if ticker not in self.data["assets"]:
            self.data["assets"].update({ticker: {"closed": {}, "open": {}}})
        
        asset_dict:dict = self.data["assets"].get(ticker)
        entry_order:Order = Order.from_dict(entry.data)
        asset_closed_orders:dict = asset_dict.get("closed")
        asset_open_orders:dict = asset_dict.get("open")
        
        order = {}
        if entry_order.results.complete:
            if entry_order.metadata.id in asset_open_orders:
                order = asset_open_orders.pop(entry_order.metadata.id)
            else:
                ### note: it is possible that the order never made it to the open list (as in, price
                ### plummetted immediately and order was closed immediately
                logging.warning(f"Order {entry_order.metadata.id} was closed immediately, no price action recorded")
                order = self._new_order_dict(entry_order)

            order.update({
                "status": "CLOSED",
                "result": {
                    "price": entry_order.results.transaction.price,
                    "contracts": entry_order.results.transaction.quantity,
                    "additional_data": entry_order.results.additional_data
                }
            })
            asset_closed_orders.update({entry_order.metadata.id: order})
            
        else:
            order = asset_open_orders.get(entry_order.metadata.id, {})
            if entry_order.metadata.id not in asset_open_orders:
                order = self._new_order_dict(order=entry_order)
                asset_open_orders.update({entry_order.metadata.id: order})

        order.get("price_action").append({
            "timestamp": entry.timestamp,
            "price": entry.amount
        })

    def convert_orders_to_lists(self) -> None:
        for asset in self.data["assets"].values():
            asset["open"] = list(asset["open"].values())
            asset["closed"] = list(asset["closed"].values())
        
    def _new_order_dict(self, order:Order) -> dict:
        return {
            "id": order.metadata.id,
            "dry_run": order.metadata.is_dry_run,
            "status": "CLOSED" if order.results.complete else "OPEN",
            "entry_order": {
                "price": order.sub_orders.main_order.price,
                "contracts": order.sub_orders.main_order.contracts,
                "timestamp": int(order.sub_orders.main_order.time / 1000.0),
                "spread": {
                    "take_profit": order.sub_orders.take_profit.price,
                    "stop_loss": order.sub_orders.stop_loss.price,
                },
                "interval": {
                    "high": order.merchant_params.high_interval,
                    "low": order.merchant_params.low_interval
                },
                "order_strategy": order.merchant_params.strategy.name
            },
            "price_action": []
        }
        
    def as_dict(self) -> dict:
        return self.data

class MerchantPerformance:
    
    def for_ledger_transactions(self, ledger:Ledger, from_timestamp:int, to_timestamp:int, filters:dict = {}) -> LedgerTransactionsResult:
        entries:list[Entry] = self._fetch_entries(ledger=ledger, from_timestamp=from_timestamp, to_timestamp=to_timestamp, filters=filters)
        metrics:dict = Analytics.all_performance_metrics(ledger_entries=entries)
        return LedgerTransactionsResult(metrics=metrics)

    def for_ledger_orders(self, ledger:Ledger, from_timestamp:int, to_timestamp:int, filters:dict = {}) -> LedgerOrdersResult:
        entries = self._fetch_entries(ledger=ledger, from_timestamp=from_timestamp, to_timestamp=to_timestamp, filters=filters)
        result = LedgerOrdersResult()
        for entry in entries:
            result.add_ledger_entry(entry=entry)
        result.convert_orders_to_lists()
        return result
    
    def _fetch_entries(self, ledger:Ledger, from_timestamp:int, to_timestamp:int, filters:dict = {}) -> list[Entry]:
        if ledger is None:
            raise ValueError("ledger is None")
        if not isinstance(ledger, Ledger):
            raise ValueError("ledger must be of type Ledger")
        if from_timestamp is None:
            raise ValueError("from_timestamp is required")
        if to_timestamp is None:
            raise ValueError("to_timestamp is required")
        if from_timestamp > to_timestamp:
            raise ValueError("from_timestamp must be less than to_timestamp")
        if filters is None:
            filters = {}
        name = None if "name" not in filters else filters.get("name")
        entries:list[Entry] = ledger.get_entries(
            name=name,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            filters=filters
        )
        entries.sort(key=lambda x: x.timestamp)
        return entries
