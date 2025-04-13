from enum import Enum

## inheriting str will enable serialization to JSON
class OrderStrategies(str, Enum):
    TRAILING_STOP = "TRAILING_STOP"
    BRACKET = "BRACKET"

def strategy_enum_from_str(strategy_str: str) -> OrderStrategies:
    if strategy_str not in OrderStrategies.__members__:
        raise ValueError(f"Invalid strategy: {strategy_str}")
    return OrderStrategies[strategy_str]
