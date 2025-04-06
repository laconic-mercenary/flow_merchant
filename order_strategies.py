from enum import Enum

## inheriting str will enable serialization to JSON
class OrderStrategies(str, Enum):
    TRAILING_STOP = "TRAILING_STOP"
    BRACKET = "BRACKET"
