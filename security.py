from merchant_order import Order

import hashlib
import os

class cfg:
    @staticmethod
    def DEFAULT_HASH_COUNT() -> int:
        return int(os.environ.get("SECURITY_DEFAULT_HASH_COUNT", "77"))

def hash(target: str, count: int = cfg.DEFAULT_HASH_COUNT()) -> str:
    if count < 0:
        raise ValueError("count must be >= 0")
    encoding = "utf-8"
    while count > 0:
        target = hashlib.sha256(target.encode(encoding)).hexdigest()
        count -= 1
    return target

def order_digest(order:Order) -> str:
    hash_count = 5 ## Keep it nice and quick
    metadata_chunk = f"{order.metadata.time_created}-{order.metadata.id}-{order.metadata.is_dry_run}"
    sub_orders_chunk = f"{order.sub_orders.main_order.id}-{order.sub_orders.stop_loss.id}-{order.sub_orders.take_profit.id}"
    merchant_params_chunk = f"{order.merchant_params.version}-{order.merchant_params.high_interval}-{order.merchant_params.low_interval}"
    payload = f"{metadata_chunk}+{sub_orders_chunk}+{merchant_params_chunk}"
    return hash(payload, count=hash_count)