from azure.data.tables import TableClient

from abc import ABC, abstractmethod

class result:
    @staticmethod
    def LOSS():
        return "LOSS"

    @staticmethod
    def PROFIT():
        return "PROFIT"

class Ledger(ABC):
    @abstractmethod
    def log(self, ticker:str, amount:float, res:str = result.PROFIT(), addtional_data:dict = {}) -> None:
        pass