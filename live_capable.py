from abc import ABC, abstractmethod

class LiveCapable(ABC):
    
    @abstractmethod
    def get_current_prices(self, symbols: list) -> dict:
        pass

    @abstractmethod
    def get_order(self, ticker: str, order_id: str) -> dict:
        pass