from order_capable import OrderCapable
from ibkr import IBKRClient
from mexc import MEXC_API

class BrokerRepository:
    def __init__(self):
        self.__repository = {
            "stock": IBKRClient(),
            "crypto": MEXC_API(),
            "forex": None
        }

    def get_for_security(self, security_type: str) -> OrderCapable:
        if security_type not in self.__repository:
            raise ValueError(f"security type {security_type} not supported")
        return self.__repository[security_type]
