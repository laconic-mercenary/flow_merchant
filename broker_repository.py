from order_capable import Broker, InvalidBroker
from mexc import MEXC_API
from security_types import SecurityTypes, security_type_from_str, valid_types
from utils import null_or_empty

class BrokerRepository:
    def __init__(self):
        self.__repository = {
            SecurityTypes.crypto: MEXC_API(),
            SecurityTypes.forex: InvalidBroker(),
            SecurityTypes.stocks: InvalidBroker(),
        }

    def invalid_broker(self) -> Broker:
        return InvalidBroker()

    def get_for_security(self, security_type: str) -> Broker:
        if null_or_empty(security_type):
            return False
        if security_type not in valid_types():
            raise ValueError(f"Invalid security type: {security_type}, valid types are {valid_types()}")
        security_type_enum = security_type_from_str(security_type_str=security_type)
        broker = self.__repository[security_type_enum]
        if isinstance(broker, InvalidBroker):
            raise ValueError(f"No broker implemented for security type: {security_type}")
        return broker
