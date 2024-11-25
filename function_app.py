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
           methods=["GET"],
           auth_level=func.AuthLevel.ANONYMOUS)
def positions(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("positions() - invoked")
    if not is_get(req):
        return rx_invalid_method()
    ## TODO domain check
    try:
        return handle_GET_for_positions(req)
    except Exception as e:
        logging.error(f"error handling positions - {e}", exc_info=True)
        default_event_logger().log_error("Error", f"error handling positions, {e}")
    return rx_not_found()
    
@app.route( route="signals", 
            methods=["GET", "POST"],
            auth_level=func.AuthLevel.ANONYMOUS )
def signals(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("signals() - invoked")
    if not is_post_or_get(req):
        return rx_invalid_method()
    ## TODO domain check
    try:
        if is_get(req):
            return handle_GET_for_signals(req)
        elif is_post(req):
            return handle_POST_for_signals(req)
    except Exception as e:
        logging.error(f"error handling market signal - {e}", exc_info=True)
        default_event_logger().log_error("Error", f"error handling market signal, {e}")
    return rx_not_found()

def connect_table_service() -> TableServiceClient:
    return TableServiceClient.from_connection_string(os.environ["storageAccountConnectionString"])

def get_table_client(table_service: TableServiceClient) -> TableClient:
    return table_service.get_table_client(table_name=TABLE_NAME())

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
    evt_logger = default_event_logger()
    title = f"[[ POSITIONS CHECK REPORT ]]"
    msg = f"{results}"
    evt_logger.log_notice(title=title, message=msg)

def handle_GET_for_positions(req: func.HttpRequest) -> func.HttpResponse:
    if is_health_check(req):
        return rx_ok()
    
    if "securityType" not in req.params:
        return rx_bad_request("securityType is required")
    
    with connect_table_service() as table_service:
        event_logger = default_event_logger()
        event_logger.log_notice("Notice", f"Received positions check request")

        security_type = req.params.get("securityType")
        
        broker_repo = BrokerRepository()
        broker = broker_repo.get_for_security(security_type)
        
        merchant = Merchant(table_service, broker, event_logger)
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
        
        event_logger = default_event_logger()
        event_logger.log_notice("Notice",f"received market signal: {message_body} - which is {signal.info()}")
        
        broker_repo = BrokerRepository()
        broker = broker_repo.get_for_security(signal.security_type())
        
        merchant = Merchant(table_service, broker, event_logger)
        subscribe_events(merchant=merchant)
        merchant.handle_market_signal(signal)

    return rx_ok()

def default_event_logger() -> EventLoggable:
    return DiscordClient()
    

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
