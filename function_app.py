import logging
import os

import azure.functions as func
from azure.data.tables import TableServiceClient, TableClient

from broker_repository import BrokerRepository
from discord import DiscordClient
from events import EventLoggable
from merchant_signal import MerchantSignal
from merchant import Merchant, TABLE_NAME, M_STATE_KEY_REST_INTERVAL
from server import *

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

# @app.route(route="command/{instruction}",
#            methods=["GET", "POST"],
#            auth_level=func.AuthLevel.ANONYMOUS)
# def command(req: func.HttpRequest, instruction: str) -> func.HttpResponse:
#     logging.info("command() - invoked")
#     try:
#         if is_get(req):
#             if instruction == "app":
#                 return handle_webapp_for_command()
#             elif instruction == "get-positions":
#                 return handle_getpositions_for_command(req)
#         elif is_post(req):
#             return handle_instruction_for_command(req=req, command=instruction)
#     except Exception as e:
#         logging.error(f"error handling cmd - {e}", exc_info=True)
#         default_event_logger().log_error("Error", f"error handling command, {e}")
#     return rx_not_found()

def connect_table_service() -> TableServiceClient:
    return TableServiceClient.from_connection_string(os.environ["storageAccountConnectionString"])

def get_table_client(table_service: TableServiceClient) -> TableClient:
    return table_service.get_table_client(table_name=TABLE_NAME())

def handle_webapp_for_command() -> func.HttpResponse:
    html = "<html><body><h1>Command</h1></body></html>"
    return func.HttpResponse(html, mimetype="text/html", status_code=200)

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
        rest_interval_minutes = int(state[M_STATE_KEY_REST_INTERVAL()])
        msg = f"I will be resting for {rest_interval_minutes} minute(s)"
    evt_logger.log_notice(title=title, message=msg)

def merchant_signal_received(merchant_id: str, signal: MerchantSignal) -> None:
    evt_logger = default_event_logger()
    title = f"{merchant_id} says:"
    msg = f"New signal received: {signal.info()}"
    evt_logger.log_notice(title=title, message=msg)

def merchant_order_placed(merchant_id: str, order_data: dict) -> None:
    evt_logger = default_event_logger()
    title = f"{merchant_id} says:"
    msg = f"Good news - I have placed an order: {order_data}"
    evt_logger.log_success(title=title, message=msg)

def merchant_positions_checked(results: dict) -> None:
    """
    {
        'monitored_tickers': [
            'PEPEUSDT', 
            'TNSRUSDT'
        ], 
        'current_positions': {
            'winners': [], 
            'laggards': [], 
            'leaders': [], 
            'losers': []
        }, 
        'elapsed_ms': 246
    }
    """
    if "monitored_tickers" not in results:
        logging.warning("merchant_positions_checked() - no monitored tickers")
        return
    if "current_positions" not in results:
        logging.warning("merchant_positions_checked() - no current positions")
        return
    if "elapsed_ms" not in results:
        logging.warning("merchant_positions_checked() - no elapsed ms")
        return
    
    elapsed_ms = results.get("elapsed_ms")
    current_positions = results.get("current_positions")
    monitored_tickers = results.get("monitored_tickers")

    title = "POSITIONS CHECK REPORT"
    msg = ""

    if len(monitored_tickers) == 0:
        msg = "I am not monitoring any assets. Please check the correctness of trading view alerts."
    else:
        winner_ct = len(current_positions.get("winners"))
        laggard_ct = len(current_positions.get("laggards"))
        leader_ct = len(current_positions.get("leaders"))
        loser_ct = len(current_positions.get("losers"))

        total_ct = winner_ct + laggard_ct + leader_ct + loser_ct
        
        if total_ct != 0:
            msg = f"=== Results ==="
            msg += f"\nI am monitoring these tickers: {monitored_tickers}"

        if winner_ct > 0:
            msg += f"\nWinners: {current_positions.get('winners')}"

        if laggard_ct > 0:
            msg += f"\nLaggards: {current_positions.get('laggards')}"

        if leader_ct > 0:
            msg += f"\nLeaders: {current_positions.get('leaders')}"

        if loser_ct > 0:
            msg += f"\nLosers: {current_positions.get('losers')}"

        if elapsed_ms > 500:
            msg += f"\nBeware that the elapsed time took {elapsed_ms} ms."

    if len(msg) > 0:
        default_event_logger().log_notice(title=title, message=msg)


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
