import unittest.mock
from ledger import Ledger, Entry, Signer
from merchant_keys import keys as mkeys
from merchant_order import Order
from order_strategies import OrderStrategies
from security import hash
from utils import unix_timestamp_secs, unix_timestamp_ms, null_or_empty, unix_timestamp_secs_dec, consts as util_consts

from azure.data.tables import TableClient

import json
import logging
import uuid

class consts:
    @staticmethod
    def HASH_CT() -> int:
        return 21
    
    @staticmethod
    def AMOUNT_TRUNCATION() -> int:
        return 10
    
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
        if new_entry.test is None:
            raise ValueError("new_entry.test is required")
        amount = round(new_entry.amount, consts.AMOUNT_TRUNCATION())
        entry_blob = f"{new_entry.name}_{new_entry.timestamp}_{amount}_{new_entry.test}"
        if prev_entry is not None:
            if not null_or_empty(prev_entry.hash):
                entry_blob = f"{entry_blob}_{prev_entry.hash}"
        return hash(entry_blob, count=consts.HASH_CT())

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
        prev_entity = None
        for entity in entities:
            ## skip the first entry - we require the previous entry to compute the hash
            ## but we don't have it for the first entry
            if entity.get("hash") != entities[0].get("hash"):    
                current_entry = self._entry_from_entity(raw_entity=entity)
                previous_entry = self._entry_from_entity(raw_entity=prev_entity)
                signature = signer.sign(new_entry=current_entry, prev_entry=previous_entry)
                if signature != current_entry.hash:
                    logging.warning(f"entry hash mismatch for {current_entry.name} - expected {current_entry.hash} but got {signature}. Previous entry: {previous_entry}")
                    problem_entries.append(current_entry)
            prev_entity = entity
        logging.info(f"ledger integrity checks found {len(problem_entries)} problem entries out of {entity_ct}")
        return problem_entries
    
    def _entry_from_entity(self, raw_entity:dict) -> Entry:
        return Entry(
            name=raw_entity["name"],
            amount=raw_entity["amount"],
            hash=raw_entity["hash"],
            timestamp=raw_entity["timestamp"],
            test=raw_entity["test"],
            data=json.loads(raw_entity["data"])
        )

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
        last_entity = entities[0]
        if last_entity.get("log_timestamp") < entities[entity_ct - 1].get("log_timestamp"):
            raise ValueError("entities are not sorted by log_timestamp correctly (descending)")
        return self._entry_from_entity(raw_entity=last_entity)

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
            "PartitionKey": self._get_partition_key(entry.data),
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

    def _get_partition_key(self, data:dict) -> str:
        if mkeys.bkrdata.order.MERCHANT_PARAMETERS() not in data:
            raise ValueError("data must contain merchant parameters")
        merchant_params = data.get(mkeys.bkrdata.order.MERCHANT_PARAMETERS())
        if mkeys.bkrdata.order.merchant_params.HIGH_INTERVAL() not in merchant_params:
            raise ValueError("data must contain merchant parameters high interval")
        if mkeys.bkrdata.order.merchant_params.LOW_INTERVAL() not in merchant_params:
            raise ValueError("data must contain merchant parameters low interval")
        if mkeys.bkrdata.order.merchant_params.VERSION() not in merchant_params:
            raise ValueError("data must contain merchant parameters version")
        high_interval = merchant_params.get(mkeys.bkrdata.order.merchant_params.HIGH_INTERVAL())
        low_interval = merchant_params.get(mkeys.bkrdata.order.merchant_params.LOW_INTERVAL())
        version = merchant_params.get(mkeys.bkrdata.order.merchant_params.VERSION())
        partition_key = f"flowmerchant-{high_interval}-{low_interval}-{version}"
        return partition_key

    def purge_old_logs(self) -> list:
        now = unix_timestamp_secs()
        age = now - util_consts.ONE_MONTH_IN_SECS()
        query_filter = f"log_timestamp lt {age}"
        deleted_entities = self.table_client.query_entities(query_filter)
        deleted_entities = list(deleted_entities)
        for entity in deleted_entities:
            self.table_client.delete_entity(
                partition_key=entity.get("PartitionKey"),
                row_key=entity.get("RowKey")
            )
        return deleted_entities
    
    def get_entries(self, name:str, from_timestamp:int, to_timestamp:int = unix_timestamp_secs(), include_tests:bool=True, filters:dict = {}) -> list[Entry]:
        if from_timestamp is None:
            raise ValueError("from_timestamp is required")
        if to_timestamp is None:
            raise ValueError("to_timestamp is required")
        if include_tests is None:
            include_tests = True
        from_timestamp = abs(from_timestamp)
        to_timestamp = abs(to_timestamp)
        if from_timestamp > to_timestamp:
            raise ValueError("from_timestamp must be less than to_timestamp")
        query_include_tests = "" if include_tests else f"and test eq false"
        query_filter = f"timestamp ge {from_timestamp} and timestamp le {to_timestamp} {query_include_tests}"
        if not null_or_empty(name):
            if not name.isalnum():
                raise ValueError("name must be alphanumeric")
            query_filter = f"{query_filter} and name eq '{name}'"
        logging.info(f"ledger query: {query_filter}")
        entites = self.table_client.query_entities(query_filter)
        results = [self._entry_from_entity(raw_entity=entity) for entity in list(entites)]
        results = self._apply_filters(entries=results, filters=filters)
        results.sort(key=lambda x: x.timestamp, reverse=False)
        return results
    
    def _apply_filters(self, entries:list[Entry], filters:dict) -> list[Entry]:
        new_entries = []
        for entry in entries:
            if self._apply_filter(entry_data=entry.data, filters=filters):
                new_entries.append(entry)
        return new_entries
    
    def _apply_filter(self, entry_data:dict, filters:dict) -> bool:
        for filter_property, filter_value in filters.items():
            if filter_property in entry_data:
                if isinstance(filter_value, dict):
                    if not self._apply_filter(entry_data=entry_data[filter_property], filters=filter_value):
                        return False
                else:
                    if entry_data[filter_property] != filter_value:
                        return False
        return True
    
    def _patch_missing_strategy(self, entry_data:dict) -> bool:
        if not isinstance(entry_data["data"], str):
            logging.error("expected string")
            return False
        data_dict = json.loads(entry_data["data"])
        if not isinstance(data_dict, dict):
            logging.error("expected dict")
            return False
        if "strategy" not in data_dict["merchant_params"]:
            logging.warning(f"found missing strategy for entry: {data_dict}")
            data_dict["merchant_params"]["strategy"] = OrderStrategies.TRAILING_STOP.value
            entry_data["data"] = json.dumps(data_dict)
            return True
        return False
    
    def recompute_ledger(self) -> None:
        logging.warning("recomputing ledger - all entries will have hash signatures recalculated")
        start_time = unix_timestamp_ms()
        entities = self.table_client.list_entities()
        entities = list(entities)
        entities.sort(key=lambda x: x.get("log_timestamp"), reverse=False)
        signer = HashSigner()
        prev_entity = None
        for entity in entities:
            entry = self._entry_from_entity(raw_entity=entity)
            prev_entry = self._entry_from_entity(raw_entity=prev_entity) if prev_entity is not None else None
            signature = signer.sign(new_entry=entry, prev_entry=prev_entry)
            if entity.get("hash") != signature or self._patch_missing_strategy(entry_data=entity):
                logging.warning(f"entry {entry.name} hash mismatch, recomputing...")
                entity["hash"] = signature
                self.table_client.update_entity(entity=entity)
            prev_entity = entity
        logging.info(f"ledger recomputed in {unix_timestamp_ms() - start_time}ms")

if __name__ == "__main__":
    import unittest

    class TestLedger(unittest.TestCase):
        def test_verify_integity_no_enttries(self):
            mock_table_client = unittest.mock.Mock()
            mock_table_client.query_entities.return_value = []
            l = TableLedger(table_client=mock_table_client)
            result = l.verify_integrity(signer=HashSigner())
            assert len(result) == 0

        def test_verify_integity_with_entries(self):
            hash_signer = HashSigner()
            mock_table_client = unittest.mock.Mock()
            l = TableLedger(table_client=mock_table_client)
            
            result_1 = {
                "PartitionKey": "flowmerchant-1-1-1",
                "RowKey": "1",
                "name": "test",
                "amount": 100.50,
                "timestamp": unix_timestamp_secs(),
                "log_timestamp": unix_timestamp_secs(),
                "hash": "hash",
                "test": True,
                "data": "{}"
            }
            entry_1 = l._entry_from_entity(result_1)
            result_2 = {
                "PartitionKey": "flowmerchant-1-1-1",
                "RowKey": "2",
                "name": "test2",
                "amount": 150.10,
                "timestamp": unix_timestamp_secs(),
                "log_timestamp": unix_timestamp_secs(),
                "hash": "hash",
                "test": True,
                "data": "{}"
            }
            entry_2 = l._entry_from_entity(result_2)
            result_1["hash"] = hash_signer.sign(new_entry=entry_1, prev_entry=None)
            entry_1.hash = result_1["hash"]
            result_2["hash"] = hash_signer.sign(new_entry=entry_2, prev_entry=entry_1)
            mock_table_client.query_entities.return_value = [result_1, result_2]
            result = l.verify_integrity(signer=hash_signer)
            assert len(result) == 0

        def test_apply_filters(self):
            entries = [
                Entry(
                    name="name1",
                    amount=0.01,
                    hash="hash1",
                    timestamp=unix_timestamp_secs(),
                    data={
                        "merchant_params": {
                            "low_interval": "5",
                            "high_interval": "60"
                        },
                        "tag": "tag1",
                        "list": ["a", "b"]
                    }
                ),                
                Entry(
                    name="name2",
                    amount=0.02,
                    hash="hash2",
                    timestamp=unix_timestamp_secs(),
                    data={
                        "merchant_params": {
                            "low_interval": "5",
                            "high_interval": "15"
                        },
                        "tag": "tag2",
                        "list": ["a", "b"]
                    }
                )
            ]
            
            mock_table_client = unittest.mock.Mock()
            table_leder = TableLedger(table_client=mock_table_client)
            results = table_leder._apply_filters(entries=entries, filters={
                "merchant_params": {
                    "low_interval": "5",
                    "high_interval": "15"
                }
            })
            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(result.hash, entries[1].hash)

            results = table_leder._apply_filters(entries=entries, filters={
                "merchant_params": {
                    "low_interval": "5",
                    "high_interval": "60"
                }
            })
            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(result.hash, entries[0].hash)
            
            results = table_leder._apply_filters(entries=entries, filters={
                "merchant_params": {
                    "high_interval": "60"
                }
            })
            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(result.hash, entries[0].hash)

            results = table_leder._apply_filters(entries=entries, filters={
                "merchant_params": {
                    "low_interval": "5",
                    "high_interval": "30"
                }
            })
            self.assertEqual(len(results), 0)
            
            results = table_leder._apply_filters(entries=entries, filters={
                "merchant_params": {
                    "low_interval": "5",
                    "high_interval": "15"
                },
                "tag": "tag1"
            })
            self.assertEqual(len(results), 0)
            
            results = table_leder._apply_filters(entries=entries, filters={
                "merchant_params": {
                    "low_interval": "5",
                    "high_interval": "15"
                },
                "tag": "tag2"
            })
            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(result.hash, entries[1].hash)

            

    unittest.main()