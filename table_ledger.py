from ledger import Ledger, Entry, Signer
from security import hash
from utils import unix_timestamp_secs, null_or_empty, unix_timestamp_secs_dec, consts as util_consts

from azure.data.tables import TableClient

import json
import logging
import uuid

class consts:
    @staticmethod
    def HASH_CT() -> int:
        return 21
    
class HashSigner(Signer):
    def sign(self, new_entry:Entry, prev_entry:Entry) -> str:
        if new_entry is None:
            raise ValueError("new_entry is required")
        if null_or_empty(new_entry.name):
            raise ValueError("new_entry.name is required")
        if new_entry.timestamp is None:
            raise ValueError("new_entry.timestamp is required")
        if new_entry.amount is None:
            raise ValueError("new_entry.amount is required")
        if new_entry.data is None:
            raise ValueError("new_entry.data is required")
        entry_blob = f"{new_entry.name}_{new_entry.timestamp}_{new_entry.amount}_{new_entry.data}"
        if prev_entry is not None:
            if prev_entry.hash is not None:
                entry_blob += f"_{prev_entry.hash}"
        entry_hash = hash(entry_blob, count=consts.HASH_CT())
        return entry_hash

class TableLedger(Ledger):
    def __init__(self, table_client: TableClient):
        if table_client is None:
            raise ValueError("table_client is required")
        self.table_client = table_client

    def verify_integrity(self, signer:Signer) -> list[Entry]:
        problem_entries = []
        now = unix_timestamp_secs()
        one_day_old_ts = now - util_consts.ONE_DAY_IN_SECS()
        query_filter = f"log_timestamp gt {one_day_old_ts}"
        entities = self.table_client.query_entities(query_filter)
        entities = list(entities)
        entity_ct = len(entities)
        if entity_ct == 0:
            logging.info("No entries found in the ledger - skipping integrity checks")
            return []
        entities.sort(key=lambda x: x["log_timestamp"], reverse=False)
        previous_entry = None
        for raw_entry in entities:
            current_entry = Entry(
                name=raw_entry["name"],
                amount=raw_entry["amount"],
                hash=raw_entry["hash"],
                timestamp=raw_entry["timestamp"],
                test=raw_entry["test"],
                data=json.loads(raw_entry["data"])
            )
            signature = signer.sign(current_entry, previous_entry)
            if signature != current_entry.hash:
                problem_entries.append(current_entry)
            previous_entry = current_entry
        logging.info(f"ledger integrity checks found {len(problem_entries)} problem entries out of {entity_ct}")
        return problem_entries

    def get_latest_entry(self) -> Entry:
        now = unix_timestamp_secs()
        one_mo_old_ts = now - util_consts.ONE_MONTH_IN_SECS()
        query_filter = f"log_timestamp gt {one_mo_old_ts}"
        entities = self.table_client.query_entities(query_filter)
        entities = list(entities)
        entity_ct = len(entities)
        if entity_ct == 0:
            logging.info("No entries found in the ledger - no latest entry exists")
            return None
        logging.info(f"Found {entity_ct} entries in the ledger")
        entities.sort(key=lambda x: x["log_timestamp"], reverse=True)
        if entities[0].get("log_timestamp") < entities[entity_ct - 1].get("log_timestamp"):
            raise ValueError("entities are not sorted by log_timestamp correctly")
        last_entry = entities[0]
        return Entry(
            name=last_entry["name"],
            amount=last_entry["amount"],
            hash=last_entry["hash"],
            timestamp=last_entry["timestamp"],
            test=last_entry["test"],
            data=json.loads(last_entry["data"])
        )

    def log(self, entry:Entry) -> None:
        if entry is None:
            raise ValueError("entry is required")
        if null_or_empty(entry.name):
            raise ValueError("entry.name is required")
        if entry.amount is None:
            raise ValueError("entry.amount is required")
        if null_or_empty(entry.hash):
            raise ValueError("entry.hash is required")
        if entry.timestamp is None:
            raise ValueError("entry.timestamp is required")
        if entry.data is None:
            raise ValueError("entry.data is required")
        entity = {
            "PartitionKey": entry.name,
            "RowKey": str(uuid.uuid4()),
            "name": entry.name,
            "amount": entry.amount,
            "timestamp": entry.timestamp,
            "log_timestamp": unix_timestamp_secs_dec(),
            "hash": entry.hash,
            "test": entry.test,
            "data": json.dumps(entry.data)
        }
        self.table_client.create_entity(entity)

    def purge_old_logs(self) -> list:
        now = unix_timestamp_secs()
        one_year_old_ts = now - util_consts.ONE_YEAR_IN_SECS()
        query_filter = f"log_timestamp lt {one_year_old_ts}"
        deleted_entities = self.table_client.query_entities(query_filter)
        deleted_entities = list(deleted_entities)
        for entity in deleted_entities:
            self.table_client.delete_entity(
                partition_key=entity.get("PartitionKey"),
                row_key=entity.get("RowKey")
            )
        return deleted_entities
    