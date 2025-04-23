from abc import ABC, abstractmethod

class AssetInfoResult:
    def __init__(self, base_scale: float):
        self.base_scale = base_scale

class AssetBalance:
    def __init__(self, asset: str, available: float):
        self.asset = asset
        self.available = available

class BalancesResult:
    def __init__(self, balances:dict[AssetBalance]):
        self.balances = balances

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

    @abstractmethod
    def get_balances(self) -> BalancesResult:
        pass
