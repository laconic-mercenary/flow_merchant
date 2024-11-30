from merchant_signal import MerchantSignal

def multiply(a: float, b: float) -> float:
    if a is None or b is None:
        raise ValueError("Cannot multiply None values")
    return a * b

def calculate_stop_loss_from_signal(signal: MerchantSignal) -> float:
    return calculate_stop_loss(close_price=signal.close(), stop_loss_percent=signal.suggested_stoploss())

def calculate_stop_loss(close_price: float, stop_loss_percent: float) -> float:
    if stop_loss_percent > 1.0:
        raise ValueError(f"Stop loss percent {stop_loss_percent} must be less than 1.0 - as in, less than 100% of the close price")
    return multiply(close_price, stop_loss_percent)

def calculate_take_profit_from_signal(signal: MerchantSignal) -> float:
    return calculate_take_profit(close_price=signal.close(), take_profit_percent=signal.takeprofit_percent())

def calculate_take_profit(close_price: float, take_profit_percent: float) -> float:
    if take_profit_percent < 1.0:
        raise ValueError(f"Take profit percent {take_profit_percent} must be greater than 1.0 - as in, greater than 100% of the close price")
    return multiply(close_price, take_profit_percent)

def safety_check(close_price: float, take_profit_price: float, stop_loss_price: float, quantity: float) -> None:
    if close_price < 0.0:
        raise ValueError(f"Close price {close_price} is less than 0.0")
    if take_profit_price < 0.0:
        raise ValueError(f"Take profit {take_profit_price} is less than 0.0")
    if stop_loss_price < 0.0:
        raise ValueError(f"Stop loss {stop_loss_price} is less than 0.0")
    if quantity <= 0.0:
        raise ValueError(f"Quantity {quantity} is less than or eq 0")
    if close_price < stop_loss_price:
        raise ValueError(f"Close price {close_price} is less than suggested stoploss {stop_loss_price}")
    if close_price > take_profit_price:
        raise ValueError(f"Close price {close_price} is greater than take profit {take_profit_price}")
