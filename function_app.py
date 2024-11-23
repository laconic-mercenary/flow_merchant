import logging
import os

import azure.functions as func
from azure.data.tables import TableServiceClient, TableClient

from broker_repository import BrokerRepository
from discord import DiscordClient
from events import EventLoggable
from live_capable import LiveCapable
from merchant_signal import MerchantSignal
from merchant import Merchant
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
    return table_service.get_table_client(table_name="flowmerchant")

def handle_GET_for_positions(req: func.HttpRequest) -> func.HttpResponse:
    if is_health_check(req):
        return rx_ok()
    if "securityType" not in req.params:
        return rx_bad_request("securityType is required")
    with connect_table_service() as table_service:
        event_logger = default_event_logger()
        event_logger.log_notice("Notice", f"Received positions check request")
        
        broker_repo = BrokerRepository()
        broker = broker_repo.get_for_security(req.params.get("securityType"))
        
        merchant = Merchant(table_service, broker, event_logger)
        results = merchant.check_positions()
        return rx_json(results)
    return rx_not_found()

def handle_GET_for_signals(req: func.HttpRequest) -> func.HttpResponse:
    if is_health_check(req):
        return rx_ok()
    return rx_not_found()

def handle_POST_for_signals(req: func.HttpRequest) -> func.HttpResponse:
    headers = get_headers(req)
    logging.debug(f"headers: {headers}")
    
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
