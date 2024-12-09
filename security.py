import hashlib
import os

class cfg:
    @staticmethod
    def DEFAULT_HASH_COUNT() -> int:
        return int(os.environ.get("SECURITY_DEFAULT_HASH_COUNT", "77"))

def hash(target: str, count: int = cfg.DEFAULT_HASH_COUNT()) -> str:
    if count < 0:
        raise ValueError("count must be >= 0")
    encoding = "utf-8"
    while count > 0:
        target = hashlib.sha256(target.encode(encoding)).hexdigest()
        count -= 1
    return target

