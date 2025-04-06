
class BrokerException(Exception):
    pass

class OrderAlreadyFilledError(BrokerException):
    pass

class OversoldError(BrokerException):
    pass

class InvalidQuantityScale(BrokerException):
    pass

class ApiError(BrokerException):
    pass