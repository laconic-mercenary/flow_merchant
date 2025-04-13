import enum

from merchant_signal import MerchantSignal
from merchant_keys import keys as mkeys
from merchant_order import Order

def multiply(a: float, b: float) -> float:
    if a is None or b is None:
        raise ValueError("Cannot multiply None values")
    return a * b

def _to_real_percent(friendly_percent:float) -> float:
    return (100.0 + friendly_percent) / 100.0

def calculate_stop_loss_from_signal(signal: MerchantSignal) -> float:
    return calculate_stop_loss(close_price=signal.close(), stop_loss_percent=signal.suggested_stoploss())

def calculate_stop_loss(close_price: float, stop_loss_percent: float) -> float:
    stop_loss_percent = -stop_loss_percent
    stop_loss_percent = _to_real_percent(stop_loss_percent)
    return multiply(close_price, stop_loss_percent)

def calculate_take_profit_from_signal(signal: MerchantSignal) -> float:
    return calculate_take_profit(close_price=signal.close(), take_profit_percent=signal.takeprofit_percent())

def calculate_take_profit(close_price: float, take_profit_percent: float) -> float:
    take_profit_percent = _to_real_percent(take_profit_percent)
    return multiply(close_price, take_profit_percent)

def calculate_percent_diff(a: float, b: float) -> float:
    if a is None or b is None:
        raise ValueError("Cannot calculate percent diff of None values")
    if a == b:
        return 0.0
    x = (a - b) / b
    return abs(x)

def calculate_pnl_from_order(order:Order, sell_amount:float=None, current_price:float=None) -> dict:
    net_value = multiply(order.sub_orders.main_order.contracts, order.sub_orders.main_order.price)
    stop_loss_value = multiply(order.sub_orders.main_order.contracts, order.sub_orders.stop_loss.price)
    take_profit_value = multiply(order.sub_orders.main_order.contracts, order.sub_orders.take_profit.price)
    profit_wout_fees = take_profit_value - net_value
    loss_wout_fees = stop_loss_value - net_value
    results = {
        "profit_without_fees": profit_wout_fees,
        "loss_without_fees": loss_wout_fees,
        "net_value": net_value,
        "stop_loss_value": stop_loss_value,
        "take_profit_value": take_profit_value
    }
    if current_price is not None:
        current_value = multiply(order.sub_orders.main_order.contracts, current_price)
        results["current_without_fees"] = current_value - net_value
    return results

def calculate_pnl_from_order2(order: dict, sell_amount:float=None, current_price:float=None) -> dict:
    suborders = order.get(mkeys.bkrdata.order.SUBORDERS())
    main_order = suborders.get(mkeys.bkrdata.order.suborders.MAIN_ORDER())
    stop_loss_order = suborders.get(mkeys.bkrdata.order.suborders.STOP_LOSS())
    take_profit_order = suborders.get(mkeys.bkrdata.order.suborders.TAKE_PROFIT())
    main_order_price = main_order.get(mkeys.bkrdata.order.suborders.props.PRICE())
    stop_loss_price = stop_loss_order.get(mkeys.bkrdata.order.suborders.props.PRICE())
    take_profit_price = take_profit_order.get(mkeys.bkrdata.order.suborders.props.PRICE())
    contracts = main_order.get(mkeys.bkrdata.order.suborders.props.CONTRACTS())
    if sell_amount is not None:
        contracts = sell_amount
    net_value = multiply(contracts, main_order_price)
    stop_loss_value = multiply(contracts, stop_loss_price)
    take_profit_value = multiply(contracts, take_profit_price)
    profit_wout_fees = take_profit_value - net_value
    loss_wout_fees = stop_loss_value - net_value
    results = {
        "profit_without_fees": profit_wout_fees,
        "loss_without_fees": loss_wout_fees,
        "net_value": net_value,
        "stop_loss_value": stop_loss_value,
        "take_profit_value": take_profit_value
    }
    if current_price is not None:
        current_value = multiply(contracts, current_price)
        results["current_without_fees"] = current_value - net_value
    return results

def safety_check(close_price: float, take_profit_price: float, stop_loss_price: float, quantity: float) -> None:
    if close_price < 0.0:
        raise ValueError(f"Close price {close_price} is less than 0.0")
    if quantity <= 0.0:
        raise ValueError(f"Quantity {quantity} is less than or eq 0")
    
class TransactionAction(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"

class Transaction:
    def __init__(self, action:TransactionAction, quantity:float, price:float):
        if not isinstance(action, TransactionAction):
            raise TypeError(f"Action must be a TransactionAction, got {type(action)}")
        if not isinstance(quantity, float):
            raise TypeError(f"Quantity must be a number, got {type(quantity)}")
        if not isinstance(price, float):
            raise TypeError(f"Price must be a number, got {type(price)}")
        self.action = action
        self.quantity = quantity
        self.price = price

    def __eq__(self, value):
        if not isinstance(value, Transaction):
            return False
        return self.action == value.action and self.quantity == value.quantity and self.price == value.price
