import time

class consts:
    
    @staticmethod
    def MS_IN_S() -> int:
        return 1000

def unix_timestamp_secs() -> int:
    return int(time.time())

def unix_timestamp_ms() -> int:
    return int(time.time() * consts.MS_IN_S())
