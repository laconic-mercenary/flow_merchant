from order_strategies import OrderStrategies
from utils import null_or_empty

import json

class SubOrder(dict):
    def __init__(self, id:str, api_rx:dict, time:int, price:float, contracts:float):
        super().__init__(
            id=id, 
            api_rx=api_rx, 
            time=time, 
            price=price,
            contracts=contracts
        )
        if null_or_empty(id):
            raise ValueError(f"SubOrder id is empty")
        if price is None:
            raise ValueError(f"SubOrder price is None")
        self.id = id
        self.api_rx = api_rx
        self.time = time
        self.price = price
        self.contracts = contracts

    def __eq__(self, value) -> bool:
        if not isinstance(value, SubOrder):
            return False
        equals = self.id == value.id
        equals = equals and self.api_rx == value.api_rx
        equals = equals and self.time == value.time
        equals = equals and self.price == value.price
        equals = equals and self.contracts == value.contracts
        return equals

class SubOrders(dict):
    def __init__(self, main_order:SubOrder, stop_loss:SubOrder, take_profit:SubOrder):
        super().__init__(
            main_order=main_order, 
            stop_loss=stop_loss, 
            take_profit=take_profit
        )
        if main_order is None:
            raise ValueError("Main order cannot be None")
        if stop_loss is None:
            raise ValueError("Stop loss cannot be None")
        if take_profit is None:
            raise ValueError("Take profit cannot be None")
        self.main_order = main_order
        self.stop_loss = stop_loss
        self.take_profit = take_profit

    def __eq__(self, value) -> bool:
        if not isinstance(value, SubOrders):
            return False
        equals = self.main_order == value.main_order
        equals = equals and self.stop_loss == value.stop_loss
        equals = equals and self.take_profit == value.take_profit
        return equals

class Metadata(dict):
    def __init__(self, id:str, time_created:int, is_dry_run:bool):
        super().__init__(
            id=id, 
            time_created=time_created, 
            is_dry_run=is_dry_run
        )
        if null_or_empty(id):
            raise ValueError(f"Metadata id is empty")
        if time_created is None:
            raise ValueError(f"Metadata time_created is None")
        if is_dry_run is None:
            is_dry_run = False
        self.id = id
        self.time_created = time_created
        self.is_dry_run = is_dry_run

    def __eq__(self, value) -> bool:
        if not isinstance(value, Metadata):
            return False
        equals = self.id == value.id
        equals = equals and self.time_created == value.time_created
        equals = equals and self.is_dry_run == value.is_dry_run
        return equals
    
class Projections(dict):
    def __init__(self, profit_without_fees:float, loss_without_fees:float):
        super().__init__(
            profit_without_fees=profit_without_fees,
            loss_without_fees=loss_without_fees
        )
        if profit_without_fees is None:
            raise ValueError(f"Projections profit_without_fees is None")
        if loss_without_fees is None:
            raise ValueError(f"Projections loss_without_fees is None")
        self.profit_without_fees = profit_without_fees
        self.loss_without_fees = loss_without_fees

    def __eq__(self, value) -> bool:
        if not isinstance(value, Projections):
            return False
        equals = self.profit_without_fees == value.profit_without_fees
        equals = equals and self.loss_without_fees == value.loss_without_fees
        return equals

class MerchantParams(dict):
    def __init__(self, high_interval:str, low_interval:str, stoploss_percent:float, takeprofit_percent:float, notes:str, version:int, strategy:OrderStrategies):
        super().__init__(
            high_interval=high_interval, 
            low_interval=low_interval, 
            stoploss_percent=stoploss_percent, 
            takeprofit_percent=takeprofit_percent, 
            notes=notes, 
            version=version,
            strategy=strategy
        )
        if null_or_empty(high_interval):
            raise ValueError(f"MerchantParams high_interval is empty")
        if not high_interval.isalnum():
            raise ValueError(f"MerchantParams high_interval is not alphanumeric")
        if null_or_empty(low_interval):
            raise ValueError(f"MerchantParams low_interval is empty")
        if not low_interval.isalnum():
            raise ValueError(f"MerchantParams low_interval is not alphanumeric")
        if stoploss_percent is None:
            raise ValueError(f"MerchantParams stoploss_percent is None")
        if takeprofit_percent is None:
            raise ValueError(f"MerchantParams takeprofit_percent is None")
        if not null_or_empty(notes):
            if len(notes) > 1024:
                raise ValueError(f"MerchantParams notes is too long")
        if version is None:
            raise ValueError(f"MerchantParams version is None")
        if strategy is None:
            raise ValueError(f"MerchantParams strategy is None")
        self.high_interval = high_interval
        self.low_interval = low_interval
        self.stoploss_percent = stoploss_percent
        self.takeprofit_percent = takeprofit_percent
        self.notes = notes
        self.version = version
        self.strategy = strategy

    def __eq__(self, value) -> bool:
        if not isinstance(value, MerchantParams):
            return False
        equals = self.high_interval == value.high_interval
        equals = equals and self.low_interval == value.low_interval
        equals = equals and self.stoploss_percent == value.stoploss_percent
        equals = equals and self.takeprofit_percent == value.takeprofit_percent
        equals = equals and self.notes == value.notes
        equals = equals and self.version == value.version
        equals = equals and self.strategy == value.strategy
        return equals
    
class Order(dict):
    def __init__(self, ticker:str, sub_orders:SubOrders, metadata:Metadata, merchant_params:MerchantParams, projections:Projections):
        super().__init__(
            ticker=ticker, 
            sub_orders=sub_orders, 
            metadata=metadata, 
            merchant_params=merchant_params,
            projections=projections
        )
        if null_or_empty(ticker):
            raise ValueError(f"Order ticker is empty")
        if ticker.count(" ") != 0:
            raise ValueError(f"Order ticker has spaces")
        if sub_orders is None:
            raise ValueError(f"Order sub_orders is None")
        if metadata is None:
            raise ValueError(f"Order metadata is None")
        if merchant_params is None:
            raise ValueError(f"Order merchant_params is None")
        #if projections is None:
        #    raise ValueError(f"Order projections is None")
        self.ticker = ticker
        self.sub_orders = sub_orders
        self.metadata = metadata
        self.merchant_params = merchant_params
        self.projections = projections

    def __eq__(self, value):
        if not isinstance(value, Order):
            return False
        equals = self.ticker == value.ticker
        equals = equals and self.sub_orders == value.sub_orders
        equals = equals and self.metadata == value.metadata
        equals = equals and self.merchant_params == value.merchant_params
        equals = equals and self.projections == value.projections
        return equals

    def to_json(self) -> str:
        return Order.to_json(self)
    
    def update(self, *args, **kwargs) -> None:
        super().update(*args, **kwargs)
        for key, val in self.items():
            setattr(self, key, val)
        
    @staticmethod
    def to_json(order) -> str:
        return json.dumps(order, default=lambda o: o.__dict__)

    @staticmethod
    def from_dict(order_dict:dict):
        if "sub_orders" not in order_dict:
            raise ValueError(f"Order dict does not contain orders: {order_dict}")
        if "metadata" not in order_dict:
            raise ValueError(f"Order dict does not contain metadata: {order_dict}")
        if "merchant_params" not in order_dict:
            raise ValueError(f"Order dict does not contain merchant_params: {order_dict}")
        if "strategy" not in order_dict.get("merchant_params"):
            raise ValueError(f"Order dict does not contain strategy: {order_dict}")
        if "projections" not in order_dict:
            raise ValueError(f"Order dict does not contain projections: {order_dict}")
        sub_orders = SubOrders(
            main_order=SubOrder(
                id=order_dict["sub_orders"]["main_order"]["id"],
                api_rx=order_dict["sub_orders"]["main_order"]["api_rx"],
                time=order_dict["sub_orders"]["main_order"]["time"],
                price=order_dict["sub_orders"]["main_order"]["price"],
                contracts=order_dict["sub_orders"]["main_order"]["contracts"]
            ),
            stop_loss=SubOrder(
                id=Order._key_or_none(order_dict["sub_orders"]["stop_loss"], "id"),
                api_rx=Order._key_or_none(order_dict["sub_orders"]["stop_loss"], "api_rx"),
                time=Order._key_or_none(order_dict["sub_orders"]["stop_loss"], "time"),
                price=Order._key_or_none(order_dict["sub_orders"]["stop_loss"], "price"),
                contracts=Order._key_or_none(order_dict["sub_orders"]["stop_loss"], "contracts")
            ),
            take_profit=SubOrder(
                id=Order._key_or_none(order_dict["sub_orders"]["take_profit"], "id"),
                api_rx=Order._key_or_none(order_dict["sub_orders"]["take_profit"], "api_rx"),
                time=Order._key_or_none(order_dict["sub_orders"]["take_profit"], "time"),
                price=Order._key_or_none(order_dict["sub_orders"]["take_profit"], "price"),
                contracts=Order._key_or_none(order_dict["sub_orders"]["take_profit"], "contracts")
            )
        )
        metadata = Metadata(
            id=order_dict["metadata"]["id"],
            time_created=order_dict["metadata"]["time_created"],
            is_dry_run=order_dict["metadata"]["is_dry_run"]
        )
        merchant_params = MerchantParams(
            high_interval=order_dict["merchant_params"]["high_interval"],
            low_interval=order_dict["merchant_params"]["low_interval"],
            stoploss_percent=order_dict["merchant_params"]["stoploss_percent"],
            takeprofit_percent=order_dict["merchant_params"]["takeprofit_percent"],
            notes=order_dict["merchant_params"]["notes"],
            version=order_dict["merchant_params"]["version"],
            strategy=OrderStrategies[order_dict["merchant_params"]["strategy"]]
        )
        projections = Projections(
            profit_without_fees=order_dict["projections"]["profit_without_fees"],
            loss_without_fees=order_dict["projections"]["loss_without_fees"],
        )
        return Order(ticker=order_dict["ticker"], projections=projections, sub_orders=sub_orders, metadata=metadata, merchant_params=merchant_params)

    @staticmethod
    def from_json(json_str:str):
        if null_or_empty(json_str):
            raise ValueError("json_str cannot be None")
        json_dict = json.loads(json_str)
        return Order.from_dict(json_dict)

    @staticmethod
    def _key_or_none(json_dict:dict, key:str):
        return json_dict[key] if key in json_dict else None

if __name__ == "__main__":
    import unittest

    class TestOrder(unittest.TestCase):
        def test_full_circle_fail(self):
            test_order = Order(
                ticker="example",
                projections=Projections(1, 2),
                sub_orders=SubOrders(
                    SubOrder("1", {"api_rx": "api_rx"}, 1, 2.0, 3.0),
                    SubOrder("2", {"api_rx": "api_rx"}, 4, 5.0, 6.0),
                    SubOrder("3", {"api_rx": "api_rx"}, 7, 8.0, 9.0)
                ),
                metadata=Metadata("1", 1, False),
                merchant_params=MerchantParams("1", "2", 3.0, 4.0, "5", 6, OrderStrategies.BRACKET)
            )

            json_str = Order.to_json(test_order)

            result = Order.from_json(json_str)

            test_order.sub_orders.main_order.api_rx = {"api_rx": "api_rx-different"}

            assert result != test_order
        
        def test_full_circle_ok(self):
            test_order = Order(
                ticker="test",
                projections=Projections(1, 2),
                sub_orders=SubOrders(
                    SubOrder("1", {"api_rx": "api_rx"}, 1, 2.0, 3.0),
                    SubOrder("2", {"api_rx": "api_rx"}, 4, 5.0, 6.0),
                    SubOrder("3", {"api_rx": "api_rx"}, 7, 8.0, 9.0)
                ),
                metadata=Metadata("1", 1, False),
                merchant_params=MerchantParams("1", "2", 3.0, 4.0, "5", 6, OrderStrategies.TRAILING_STOP)
            )

            json_str = Order.to_json(test_order)

            result = Order.from_json(json_str)

            assert result == test_order

        def test_update_override(self):
            test_order = Order(
                ticker="test",
                projections=Projections(1, 2),
                sub_orders=SubOrders(
                    SubOrder("1", {"api_rx": "api_rx"}, 1, 2.0, 3.0),
                    SubOrder("2", {"api_rx": "api_rx"}, 4, 5.0, 6.0),
                    SubOrder("3", {"api_rx": "api_rx"}, 7, 8.0, 9.0)
                ),
                metadata=Metadata("1", 1, False),
                merchant_params=MerchantParams("1", "2", 3.0, 4.0, "5", 6, OrderStrategies.BRACKET)
            )
            assert "remove" not in test_order
            test_order.update({"remove": True})
            assert test_order.get("remove") == True

        def test_serialization(self):
            test_order = Order(
                ticker="test",
                projections=Projections(1, 2),
                sub_orders=SubOrders(
                    SubOrder("1", {"api_rx": "api_rx"}, 1, 2.0, 3.0),
                    SubOrder("2", {"api_rx": "api_rx"}, 4, 5.0, 6.0),
                    SubOrder("3", {"api_rx": "api_rx"}, 7, 8.0, 9.0)
                ),
                metadata=Metadata("1", 1, False),
                merchant_params=MerchantParams("1", "2", 3.0, 4.0, "5", 6, OrderStrategies.TRAILING_STOP)
            )
            test_order.metadata.id = str(999)
            tmp = json.dumps(test_order)
            tmp = json.loads(tmp)
            self.assertEqual(tmp["metadata"]["id"], "1")

            ### GOTCHA!
            test_order.metadata = Metadata(id="999", time_created=1, is_dry_run=False)
            tmp2 = json.dumps(test_order)
            tmp2 = json.loads(tmp2)
            self.assertEqual(tmp2["metadata"]["id"], "1")

            ### however... (because the __init__ uses the dict.super())
            test_order.metadata = Metadata(id="999", time_created=1, is_dry_run=False)
            tmp3 = json.dumps(test_order.__dict__)
            tmp3 = json.loads(tmp3)
            self.assertEqual(tmp3["metadata"]["id"], "999")

            test_order = Order(
                metadata=Metadata(id="999", time_created=1, is_dry_run=False),
                ticker="test",
                projections=Projections(1, 2),
                sub_orders=SubOrders(
                    SubOrder("1", {"api_rx": "api_rx"}, 1, 2.0, 3.0),
                    SubOrder("2", {"api_rx": "api_rx"}, 4, 5.0, 6.0),
                    SubOrder("3", {"api_rx": "api_rx"}, 7, 8.0, 9.0)
                ),
                merchant_params=MerchantParams("1", "2", 3.0, 4.0, "5", 6, OrderStrategies.TRAILING_STOP)
            )
            tmp4 = json.dumps(test_order)
            tmp4 = json.loads(tmp4)
            self.assertEqual(tmp4["metadata"]["id"], "999")


    unittest.main()
