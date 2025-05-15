# Flow Merchant
A flexible, broker-agnostic trading automation framework for implementing and executing trading strategies.

## Overview
Flow Merchant is a Python-based trading system designed to automate the execution of trading strategies across different brokers. It provides a robust abstraction layer that separates trading logic from broker-specific implementations, allowing traders to focus on strategy development while ensuring reliable order execution.

## Key Features
Broker-Agnostic Design: Interface-based architecture that works with any broker implementation
Multiple Strategy Support: Includes bracket orders and trailing stop strategies out of the box
Risk Management: Built-in profit and loss projections for each trade
Dry Run Capability: Test strategies without placing actual orders
Robust Error Handling: Retry mechanisms and comprehensive error management
Extensible Architecture: Easily add new strategies or broker integrations

### Architecture
Flow Merchant is built around several core components:

## Order Management
- Order: Represents a complete trading order with all associated metadata
- SubOrders: Manages the components of a bracket order (main order, stop loss, take profit)
- Results & Projections: Track expected and actual outcomes of trades

## Strategy Framework
- OrderStrategy: Base class for all trading strategies
- BracketStrategy: Implements bracket orders (entry + stop loss + take profit)
- TrailingStopStrategy: Extends bracket strategy with dynamic stop loss adjustment

## Broker Abstraction
Flow Merchant uses interface-based design to abstract broker interactions:
- Broker: Base interface for all broker implementations
- MarketOrderable: For placing market orders
- LimitOrderable: For placing limit orders
- StopMarketOrderable: For placing stop orders
- OrderCancelable: For canceling orders
- DryRunnable: For testing without actual execution
- LiveCapable: For accessing live market data

# Getting Started

## Prerequisites
- Python 3.7+
- Dependencies (list to be added)

## Installation

```bash
git clone https://github.com/laconic-mercenary/flow_merchant.git
cd flow_merchant
pip install -r requirements.txt
```

## Execute

## Basic Usage
from flow_merchant import BracketStrategy, MockBroker
from flow_merchant.merchant_signal import MerchantSignal

## Create a trading signal
```python
signal = MerchantSignal(
    ticker="AAPL",
    contracts=1.0,
    takeprofit_percent=2.0,
    stoploss_percent=1.0
)
```

# Place orders based on the signal
order = strategy.place_orders(broker, signal, {})

# Later, handle price changes or order execution
result = strategy.handle_price_change(broker, order, {"current_price": 150.25})

## Implementing Strategies
Flow Merchant makes it easy to implement custom trading strategies:

```python
from flow_merchant import OrderStrategy, HandleResult

class MyCustomStrategy(OrderStrategy):
    def place_orders(self, broker, signal, merchant_state, merchant_params={}):
        # Implement order placement logic
        pass
        
    def handle_price_change(self, broker, order, merchant_params={}):
        # Implement price change handling
        return HandleResult(target_order=order, complete=False)
        
    def handle_take_profit(self, broker, order, merchant_params={}):
        # Implement take profit handling
        pass
        
    def handle_stop_loss(self, broker, order, merchant_params={}):
        # Implement stop loss handling
        pass
```


## Implementing Broker Integrations
To add support for a new broker, implement the required interfaces:

```python
from flow_merchant import Broker, MarketOrderable, LiveCapable

class MyBrokerImplementation(Broker, MarketOrderable, LiveCapable):
    def get_name(self):
        return "MyBroker"
        
    def place_market_order(self, ticker, contracts, action, tracking_id=None):
        # Implement market order placement
        pass
        
    def standardize_market_order(self, market_order_result):
        # Convert broker-specific response to standard format
        pass
        
    def get_current_prices(self, tickers):
        # Implement price fetching
        pass
        
    # Implement other required methods...
```


## Built-in Strategies
### Bracket Strategy
The bracket strategy places three orders:
- A main entry order
- A stop loss order to limit downside
- A take profit order to secure gains

When either the stop loss or take profit is triggered, the other is automatically canceled.

### Trailing Stop Strategy
The trailing stop strategy extends the bracket strategy by dynamically adjusting the stop loss level as the price moves favorably, helping to lock in profits while allowing for continued upside.

# Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

# License
[License information to be added]

# Acknowledgements
[Acknowledgements to be added]