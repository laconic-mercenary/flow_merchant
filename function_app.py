import logging
import os

import azure.functions as func
from azure.data.tables import TableServiceClient, TableClient

from broker_repository import BrokerRepository
from discord import DiscordClient
from events import EventLoggable
from ledger import Ledger, result as ledger_result
from merchant_signal import MerchantSignal
from merchant import Merchant
from merchant import cfg as merchant_cfg
from merchant_keys import keys as mkeys
from server import *
from table_ledger import TableLedger

import command_app

app = func.FunctionApp()

@app.route(route="positions",
           methods=["GET", "POST"],
           auth_level=func.AuthLevel.ANONYMOUS)
def positions(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("positions() - invoked")
    try:
        return handle_for_positions(req)
    except Exception as e:
        logging.error(f"error handling positions - {e}", exc_info=True)
        default_event_logger().log_error("Error", f"error handling positions, {e}")
    return rx_not_found()
    
@app.route( route="signals", 
            methods=["GET", "POST"],
            auth_level=func.AuthLevel.ANONYMOUS )
def signals(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("signals() - invoked")
    try:
        if is_get(req):
            return handle_GET_for_signals(req)
        elif is_post(req):
            return handle_POST_for_signals(req)
    except Exception as e:
        logging.error(f"error handling market signal - {e}", exc_info=True)
        default_event_logger().log_error("Error", f"error handling market signal, {e}")
    return rx_not_found()

@app.route(route="command/{instruction}",
           methods=["GET", "POST"],
           auth_level=func.AuthLevel.ANONYMOUS)
def command(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("command() - invoked")
    instruction = req.route_params.get("instruction")
    try:
        if is_get(req):
            if instruction == "app":
                return handle_webapp_for_command()
            elif instruction == "get-positions":
                return handle_getpositions_for_command(req)
        elif is_post(req):
            return handle_instruction_for_command(req=req, command=instruction)
    except Exception as e:
        logging.error(f"error handling cmd - {e}", exc_info=True)
        default_event_logger().log_error("Error", f"error handling command, {e}")
    return rx_not_found()

def connect_table_service() -> TableServiceClient:
    return TableServiceClient.from_connection_string(os.environ["storageAccountConnectionString"])

def handle_webapp_for_command() -> func.HttpResponse:
    with connect_table_service() as table_service:
        broker_repo = BrokerRepository()
        broker = broker_repo.get_for_security(security_type="crypto")
        cmd_app = command_app.CommandApp(table_service=table_service, broker=broker)
        return func.HttpResponse(
            body=cmd_app.html(),
            mimetype="text/html",
            status_code=200
        )

def handle_getpositions_for_command(req: func.HttpRequest) -> func.HttpResponse:
    ## get positions from database, making sure to hash the IDs
    ## get current prices for the positions
    ## return as a json
    with connect_table_service() as table_service:    
        broker_repo = BrokerRepository()
        for sec_type in broker_repo.get_security_types():
            broker = broker_repo.get_for_security(sec_type)
            merchant = Merchant(table_service, broker)
            ## TODO
    
    return rx_not_found()

def handle_instruction_for_command(req: func.HttpRequest, command: str) -> func.HttpResponse:
    ## validate command (length and format)
    ## query all positions
    ## hash the IDs of each position
    ## check it against the command
    ## determine the command from the post payload (sell is the only one supported)
    return rx_bad_request()

def handle_for_positions(req: func.HttpRequest) -> func.HttpResponse:
    if is_health_check(req):
        return rx_ok()
    
    with connect_table_service() as table_service:
        security_type = req.params.get("securityType", "crypto")
        
        broker_repo = BrokerRepository()
        broker = broker_repo.get_for_security(security_type)
        
        merchant = Merchant(table_service, broker)
        subscribe_events(merchant=merchant)
        results = merchant.check_positions()
        
        return rx_json(results)
    
    return rx_not_found()

def handle_GET_for_signals(req: func.HttpRequest) -> func.HttpResponse:
    if is_health_check(req):
        return rx_ok()
    return rx_not_found()

def handle_POST_for_signals(req: func.HttpRequest) -> func.HttpResponse:
    headers = get_headers(req)
    logging.info(f"Trading View headers: {headers}")
    
    message_body = get_json_body(req)
    logging.info(f"received merchant signal: {message_body}")
    
    with connect_table_service() as table_service:
        signal = MerchantSignal.parse(message_body)
        if not is_authorized(signal.api_token()):
            return rx_unauthorized()
        
        broker_repo = BrokerRepository()
        broker = broker_repo.get_for_security(signal.security_type())
        
        merchant = Merchant(table_service, broker)
        subscribe_events(merchant=merchant)
        merchant.handle_market_signal(signal)

    return rx_ok()

def default_event_logger() -> EventLoggable:
    return DiscordClient()

def subscribe_events(merchant: Merchant) -> None:
    merchant.on_order_placed += merchant_order_placed
    merchant.on_positions_check += merchant_positions_checked
    merchant.on_signal_received += merchant_signal_received
    merchant.on_state_change += merchant_state_changed

def merchant_state_changed(merchant_id: str, status: str, state: dict) -> None:
    evt_logger = default_event_logger()
    ustatus = status.upper()
    title = f"{merchant_id} says:"
    msg = f"I am now {ustatus}"
    if ustatus == "RESTING":
        rest_interval_minutes = int(state[mkeys.REST_INTERVAL()])
        msg = f"I will be resting for {rest_interval_minutes} minute(s)"
    evt_logger.log_notice(title=title, message=msg)

def merchant_signal_received(merchant_id: str, signal: MerchantSignal) -> None:
    pass
    
def merchant_order_placed(merchant_id: str, order_data: dict) -> None:
    evt_logger = default_event_logger()
    title = f"{merchant_id} says:"
    msg = f"Good news - I have placed an order: {order_data}"
    evt_logger.log_success(title=title, message=msg)

def merchant_positions_checked(results: dict) -> None:
    if "monitored_tickers" not in results:
        logging.warning(f"merchant_positions_checked() - no monitored tickers - {results}")
        return
    if "positions" not in results:
        logging.warning(f"merchant_positions_checked() - no current positions - {results}")
        return
    if "elapsed_ms" not in results:
        logging.warning(f"merchant_positions_checked() - no elapsed ms - {results}")
        return
    
    elapsed_ms = results.get("elapsed_ms")
    current_positions = results.get("positions")
    monitored_tickers = results.get("monitored_tickers")
    current_prices = results.get("current_prices")

    title = "POSITIONS CHECK REPORT"
    msg = ""

    robot_speak = default_event_logger().log_notice

    if len(monitored_tickers) == 0:
        msg = "I am not monitoring any assets. Please check the correctness of trading view alerts."
    else:
        winners = current_positions.get("winners")
        laggards = current_positions.get("laggards")
        leaders = current_positions.get("leaders")
        losers = current_positions.get("losers")

        winner_ct = len(winners)
        laggard_ct = len(laggards)
        leader_ct = len(leaders)
        loser_ct = len(losers)

        def make_friendly(positions: list, prices:dict) -> list:
            results = []
            for pos in positions:
                if "orders" not in pos: 
                    raise ValueError(f"expected key orders in position {pos}")
                if "projections" not in pos:
                    raise ValueError(f"expected key projections in order {pos}")
                if "ticker" not in pos:
                    raise ValueError(f"expected key ticker in order {pos}")
                
                ticker = pos.get("ticker")
                sub_orders = pos.get("orders")
                projections = pos.get("projections")
                current_price = prices.get(ticker)

                potential_profit = projections.get("profit_without_fees")
                potential_loss = projections.get("loss_without_fees")

                main_order = sub_orders.get("main")
                stop_loss_order = sub_orders.get("stop_loss")
                take_profit_order = sub_orders.get("take_profit")
                
                main_price = main_order.get("price")
                main_contracts = main_order.get("contracts")

                stop_price = stop_loss_order.get("price")

                take_profit_price = take_profit_order.get("price")

                main_total = main_price * main_contracts

                main_price = round(main_price, 5)
                main_contracts = round(main_contracts, 5)
                main_total = round(main_total, 5)
                stop_price = round(stop_price, 5)
                potential_loss = round(potential_loss, 5)
                take_profit_price = round(take_profit_price, 5)
                potential_profit = round(potential_profit, 5)
                current_price = round(current_price, 5)

                sell_now_profit_loss = round((current_price * main_contracts) - main_total, 5)
                sell_now_msg = f"LOSS of {sell_now_profit_loss}" if sell_now_profit_loss < 0 else f"PROFIT of {sell_now_profit_loss}"

                results.append(f"{ticker} bought @ {main_price} x {main_contracts} = {main_total} -- STOP LOSS @ {stop_price} for {potential_loss} -- TAKE PROFIT @ {take_profit_price} for {potential_profit} -- CURRENT PRICE @ {current_price}, if you sold now it would be a {sell_now_msg} (values are NOT exact)")
            return results

        if winner_ct != 0:
            clown_face = "\U0001F921"
            msg += f"\n[{clown_face}] WINNERS: {make_friendly(winners, current_prices)}"
            robot_speak = default_event_logger().log_success
            write_ledger(positions=winners, result=ledger_result.PROFIT())

        if loser_ct != 0:
            barf_face = "\U0001F92E"
            msg += f"\n[{barf_face}] LOSERS: {make_friendly(losers, current_prices)}"
            write_ledger(positions=losers, result=ledger_result.LOSS())

        if leader_ct != 0:
            smiley_face = "\U0001f600"
            msg += f"\n[{smiley_face}] Leaders: {make_friendly(leaders, current_prices)}"

        if laggard_ct != 0:
            sickly_face = "\U0001F912"
            msg += f"\n[{sickly_face}] Laggards: {make_friendly(laggards, current_prices)}"

        if elapsed_ms > 1000:
            msg += f"\nBeware that the positions check took {elapsed_ms} ms to complete."

    if len(msg) != 0:
        robot_speak(title=title, message=msg)

def write_ledger(positions:list, result:str) -> None:
    with connect_table_service() as table_service:
        table_name = "fm_orderledger"
        table_client = table_service.create_table_if_not_exists(table_name=table_name)
        table_ledr = TableLedger(table_client=table_client)
        ledgers = [ table_ledr ]
        for pos in positions:
            if "order" not in pos:
                raise ValueError(f"expected key 'order' in position {pos}")
            order = pos.get("order")
            if "projections" not in order:
                raise ValueError(f"expected key 'projections' in order {order}")
            if "ticker" not in order:
                raise ValueError(f"expected key 'ticker' in order {order}")
            ticker = order.get("ticker")
            projections = order.get("projections")
            amount = projections.get("profit_without_fees") if result == ledger_result.PROFIT() else projections.get("loss_without_fees")
            for ledger in ledgers:
                ledger.log(ticker=ticker, amount=amount, res=result)
        deleted_logs = table_ledr.purge_old_logs()
        if len(deleted_logs) != 0:
            logging.info(f"removed the following from the ledger (expired): {deleted_logs}")

#####################################
#####################################
### Tests
#####################################
#####################################

import unittest
from unittest.mock import Mock, patch

class TestFlowMerchant(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
