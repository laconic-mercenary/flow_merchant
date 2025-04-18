from order_capable import Broker
from ibkr import IBKRClient
from mexc import MEXC_API

class BrokerRepository:
    def __init__(self):
        self.__repository = {
            "crypto": MEXC_API()
        }

    def get_security_types(self) -> list:
        return list(self.__repository.keys())

    def is_supported_security(self, security_type: str) -> bool:
        if security_type is None or len(security_type.strip()) == 0:
            return False
        return security_type in self.__repository

    def get_for_security(self, security_type: str) -> Broker:
        if not self.is_supported_security(security_type):
            raise ValueError(f"security type {security_type} not supported")
        return self.__repository[security_type]
