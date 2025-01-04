from ledger import Ledger, result
from utils import unix_timestamp_secs

from azure.data.tables import TableClient

import uuid

class TableLedger(Ledger):
    def __init__(self, table_client: TableClient):
        if table_client is None:
            raise ValueError("table_client is required")
        self.table_client = table_client

    def log(self, ticker:str, amount:float, res:str = result.PROFIT(), addtional_data: dict = {}) -> None:
        if ticker is None or len(ticker) == 0:
            raise ValueError("ticker is required")
        if amount is None:
            raise ValueError("amount is required")
        if res not in [ result.PROFIT(), result.LOSS() ]:
            raise ValueError(f"res must be either {result.PROFIT()} or {result.LOSS()}")
        if addtional_data is None:
            addtional_data = {}
        entity = {
            "PartitionKey": ticker,
            "RowKey": str(uuid.uuid4()),
            "ticker": ticker,
            "amount": amount,
            "result": res,
            "log_ts": unix_timestamp_secs()
        }
        entity.update(addtional_data)
        self.table_client.create_entity(entity)

    def purge_old_logs(self) -> list:
        now = unix_timestamp_secs()
        one_year_old_ts = now - (365 * 24 * 60 * 60)
        query_filter = f"log_ts lt {one_year_old_ts}"
        deleted_entities = self.table_client.query_entities(query_filter)
        deleted_entities = list(deleted_entities)
        for entity in deleted_entities:
            self.table_client.delete_entity(
                partition_key=entity.get("PartitionKey"),
                row_key=entity.get("RowKey")
            )
        return deleted_entities