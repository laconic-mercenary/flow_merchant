
from azure.data.tables import TableServiceClient

import merchant
import security

from live_capable import LiveCapable

import copy
import json
import os

class CommandApp:

    def __init__(self, table_service: TableServiceClient, broker: LiveCapable) -> None:
        if table_service is None:
            raise ValueError("table_service is required")
        if broker is None:
            raise ValueError("broker is required")
        self._table_service = table_service
        self._broker = broker

    def html(self) -> str:
        positions = self._get_positions()
        return f"""
        <!doctype html>
        <html lang="en">>
            <head>
                <title>Flow Merchant</title>
                <meta charset="UTF-8" />
                {self._render_js()}
            </head>
            <body>
                {self._render_positions_table(positions)}
            </body>
        </html>
        """

    def _get_positions(self) -> list[dict]:
        table_client = self._table_service.get_table_client(table_name=merchant.TABLE_NAME())
        positions = list(table_client.list_entities())
        tickers = [ position.get("ticker") for position in positions ]
        current_prices = self._broker.get_current_prices(tickers)
        results = [ ]
        for position in positions:
            ticker = position.get("ticker")
            current_price = current_prices.get(ticker)
            position.update({"current_price": current_price})
            order_list = json.loads(position.get("broker_data"))
            for order in order_list:
                new_entry = copy.deepcopy(position)
                new_entry.update({"id": order["orders"]["main"].get("id")})
                new_entry.update({"entry_price": order["orders"]["main"].get("price")})
                new_entry.update({"entry_contracts": order["orders"]["main"].get("contracts")})
                new_entry.update({"stop_price": order["orders"]["stop_loss"].get("price")})
                new_entry.update({"take_profit_price": order["orders"]["take_profit"].get("price")})
                results.append(new_entry)
        return results
    
    def _render_js(self) -> str:
        return """
        <script>
            function sellPosition(id) {
                fetch('/api/command/' + id, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ "command": "SELL" }),
                })
                .then(response => {
                    if (!response.ok) {
                        throw new Error('Network response was not ok ' + response.statusText);
                    }
                    return response.text();
                })
                .then(data => {
                    alert('Sell command sent, reload the page to see the changes.');
                    location.reload();
                })
                .catch((error) => {
                    alert('An error occurred while sending the sell command:\n' + error.message);
                });
            }
        </script>
        """

    def _render_positions_table(self, positions: list[dict]) -> str:
        position_rows = [ self._render_positions_row(position) for position in positions ]
        return f"""
        <table>
            <thead>
                <tr>
                    <th>Ticker</th>
                    <th>Entry</th>
                    <th>Quantity</th>
                    <th>Stop Loss</th>
                    <th>Take Profit</th>
                    <th>Current Price</th>
                    <th>SELL</th>
                </tr>
            </thead>
            <tbody>
                {position_rows}
            </tbody>
        </table>
        """
    
    def _render_positions_row(self, position: dict) -> str:
        ticker = position.get("ticker")
        entry = position.get("entry_price")
        quantity = position.get("entry_contracts")
        stop_loss = position.get("stop_price")
        take_profit = position.get("take_profit_price")
        current_price = position.get("current_price")
        order_id = position.get("id")
        order_id = security.hash(order_id)
        return f"""
        <tr>
            <td>{ticker}</td>
            <td>{entry}</td>
            <td>{quantity}</td>
            <td>{stop_loss}</td>
            <td>{take_profit}</td>
            <td>{current_price}</td>
            <td><button onclick="sellPosition('{order_id}')">SELL</button></td>
        </tr>
        """
    