from azure.data.tables import TableClient

from abc import ABC, abstractmethod

class Entry(dict):
    def __init__(self, init_dict:dict):
        super().__init__(init_dict)
        self.name = init_dict.get("name")
        self.amount = init_dict.get("amount")
        self.hash = init_dict.get("hash")
        self.timestamp = init_dict.get("timestamp")
        self.data = init_dict.get("data")

    def __init__(self, name:str, amount:float, hash:str, timestamp:int, data:dict = {}, test:bool = False):
        super().__init__(name=name, amount=amount, hash=hash, timestamp=timestamp, data=data)
        if name is None:
            raise ValueError("name is required")
        if amount is None:
            raise ValueError("amount is required")
        if timestamp is None:
            raise ValueError("timestamp is required")
        self.name = name
        self.amount = amount
        self.hash = hash
        self.timestamp = timestamp
        self.test = test
        self.data = {} if data is None else data

class Signer:
    @abstractmethod
    def sign(self, new_entry:Entry, prev_entry:Entry) -> str:
        pass

class Ledger(ABC):
    @abstractmethod
    def log(self, entry:Entry) -> None:
        pass

    @abstractmethod
    def verify_integrity(self, signer:Signer) -> list[Entry]:
        pass

    @abstractmethod
    def purge_old_logs(self) -> list[Entry]:
        pass

    @abstractmethod
    def get_latest_entry(self) -> Entry:
        pass

    @abstractmethod
    def get_entries(self, name:str, from_timestamp:int, to_timestamp:int, include_tests:bool, filters:dict = {}) -> list[Entry]:
        pass

