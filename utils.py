import time

class consts:
    @staticmethod
    def MILLIS_IN_SECONDS() -> int:
        return 1000

    @staticmethod
    def ONE_HOUR_IN_SECS() -> int:
        return 60 * 60
    
    @staticmethod
    def ONE_DAY_IN_SECS() -> int:
        return 24 * consts.ONE_HOUR_IN_SECS()
    
    @staticmethod
    def ONE_WEEK_IN_SECS() -> int:
        return 7 * consts.ONE_DAY_IN_SECS()
    
    @staticmethod
    def ONE_MONTH_IN_SECS() -> int:
        return 30 * consts.ONE_DAY_IN_SECS()

    @staticmethod
    def ONE_YEAR_IN_SECS() -> int:
        return 365 * consts.ONE_DAY_IN_SECS()

def unix_timestamp_secs_dec() -> float:
    return float(unix_timestamp_ms()) / float(consts.MILLIS_IN_SECONDS())

def unix_timestamp_secs() -> int:
    return int(time.time())

def unix_timestamp_ms() -> int:
    return int(time.time() * consts.MILLIS_IN_SECONDS())

def null_or_empty(string:str) -> bool:
    return string is None or len(string.strip()) == 0