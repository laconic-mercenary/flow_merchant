from abc import ABC, abstractmethod

class AssetInfoResult:
    def __init__(self, base_scale: float):
        self.base_scale = base_scale

class LiveCapable(ABC):
    
    @abstractmethod
    def get_current_prices(self, symbols: list) -> dict:
        pass

    @abstractmethod
    def get_order(self, ticker: str, order_id: str) -> dict:
        pass

    @abstractmethod
    def get_asset_info(self, symbols:list) -> AssetInfoResult:
        pass