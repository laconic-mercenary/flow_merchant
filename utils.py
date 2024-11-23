import time

__MS_IN_S = 1000

def unix_timestamp_secs() -> int:
    return int(time.time())

def unix_timestamp_ms() -> int:
    return int(time.time() * __MS_IN_S)
