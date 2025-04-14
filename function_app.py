import logging
import os

import azure.functions as func
from azure.data.tables import TableServiceClient, TableClient

from broker_repository import BrokerRepository
from merchant_signal import MerchantSignal
from merchant_order import Order
from merchant import Merchant, PositionsCheckResult
from merchant_reporting import MerchantReporting
from server import *
from signal_enhancements import apply_all
from table_ledger import TableLedger, HashSigner
from utils import time_utc_as_str, roll_dice_10percent as roll_dice

import command_app

app = func.FunctionApp()

@app.route(route="positions",
           methods=["GET"],
           auth_level=func.AuthLevel.ANONYMOUS)
def positions(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("positions() - invoked")
    try:
        return handle_for_positions(req)
    except Exception as e:
        logging.error(f"error handling positions - {e}", exc_info=True)
        report_problem(msg=f"error handling positions", exc=e)
    return rx_not_found()
    
@app.route( route="signals", 
            methods=["POST"],
            auth_level=func.AuthLevel.ANONYMOUS )
def signals(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("signals() - invoked")
    return handle_for_signals(req)
    
@app.route(route="command/{instruction}/{identifier}",
           methods=["GET"],
           auth_level=func.AuthLevel.ANONYMOUS)
def command(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("command() - invoked")
    instruction = req.route_params.get("instruction")
    identifier = req.route_params.get("identifier")
    if instruction is None or identifier is None:
        ### consider logging more info if this endpoint is getting attacked
        logging.warning("command() - missing instruction or identifier")
        return rx_bad_request()
    try:
        return handle_instruction_for_command(
            req=req, 
            command=instruction, 
            identifier=identifier
        )
    except Exception as e:
        logging.error(f"error handling cmd - {e}", exc_info=True)
        report_problem(msg=f"error handling cmd", exc=e)
    return rx_bad_request()

def connect_table_service() -> TableServiceClient:
    return TableServiceClient.from_connection_string(os.environ["storageAccountConnectionString"])

def handle_instruction_for_command(req: func.HttpRequest, command:str, identifier:str) -> func.HttpResponse:
    if command == "sell":
        security_type = req.params.get("securityType", "crypto")        
        broker = BrokerRepository().get_for_security(security_type)
        with connect_table_service() as table_service:        
            merchant = Merchant(table_service, broker)
            subscribe_events(merchant=merchant)
            result = merchant.sell(identifier)
            if result is None:
                return rx_not_found("Unable to sell - order not found or no longer exists")
            return rx_json({
                "ticker": result.order.ticker,
                "id": result.order.metadata.id,
                "dry_run": result.order.metadata.is_dry_run,
                "action": "SELL", 
                "result": "OK",
                "timestamp": time_utc_as_str()
            })
    return rx_bad_request()

def handle_for_positions(req: func.HttpRequest) -> func.HttpResponse:
    if is_health_check(req):
        return rx_ok()
    
    security_type = req.params.get("securityType", "crypto")        
    broker = BrokerRepository().get_for_security(security_type)

    with connect_table_service() as table_service:        
        merchant = Merchant(table_service, broker)
        subscribe_events(merchant=merchant)
        results = merchant.check_positions()
        return rx_json(results.__dict__)
    
    return rx_not_found()

def handle_for_signals(req: func.HttpRequest) -> func.HttpResponse:
    headers = None
    message_body = None
    try:
        headers = get_headers(req)
        logging.info(f"Trading View headers: {headers}")
        message_body = get_json_body(req)
        logging.info(f"received merchant signal: {message_body}")

        signal = MerchantSignal.parse(message_body)
        
        if not is_authorized(signal.api_token()):
            return rx_unauthorized()
        
        broker = BrokerRepository().get_for_security(signal.security_type())
    
        with connect_table_service() as table_service:    
            merchant = Merchant(table_service, broker)
            subscribe_events(merchant=merchant)
            signal = enhance_signal(signal)
            merchant.handle_market_signal(signal)
        return rx_ok()
    
    except Exception as e:
        logging.error(f"error handling market signal - {e} - {message_body}", exc_info=True)
        report_problem(
            msg=f"error handling market signal", 
            exc=e, 
            additional_data={
                "message_body": message_body,
                "headers": headers
            }
        )
    return rx_not_found()

def enhance_signal(signal: MerchantSignal) -> MerchantSignal:
    return apply_all(signal)

def subscribe_events(merchant: Merchant) -> None:
    merchant.on_order_placed += merchant_order_placed
    merchant.on_positions_check += merchant_positions_checked
    merchant.on_signal_received += merchant_signal_received
    merchant.on_state_change += merchant_state_changed

def report_problem(msg:str, exc:Exception, additional_data:dict = {}) -> None:
    try:
        msg = f"Message: {msg} -- Data: {additional_data}"
        MerchantReporting().report_problem(msg=msg, exc=exc)
    except Exception as e:
        logging.error(f"error reporting problem - {e} -- NOTE the original error was {exc}", exc_info=True)
        MerchantReporting().report_problem(msg=f"Error in reporting problem. Original error was {exc}")

def merchant_state_changed(merchant_id: str, status: str, state: dict) -> None:
    try:
        MerchantReporting().report_state_changed(merchant_id, status, state)
    except Exception as e:
        logging.error(f"error reporting state change - {e}", exc_info=True)
        report_problem(msg=f"error reporting state change", exc=e)

def merchant_signal_received(merchant_id: str, signal: MerchantSignal) -> None:
    try:
        MerchantReporting().report_signal_received(signal)
    except Exception as e:
        logging.error(f"error reporting signal received - {e}", exc_info=True)
        report_problem(msg=f"error reporting signal received", exc=e)

def merchant_order_placed(merchant_id: str, order_data: Order) -> None:
    try:
        MerchantReporting().report_order_placed(order_data)
    except Exception as e:
        logging.error(f"error reporting order placed - {e}", exc_info=True)
        report_problem(msg=f"error reporting order placed", exc=e)

def merchant_positions_checked(results: PositionsCheckResult) -> None:
    reporting = MerchantReporting()
    try:
        reporting.report_check_results(results=results)
        closed_positions = results.winners + results.losers
        logging.info(f"reporting to ledger, the following closed positions: {closed_positions}")
        with connect_table_service() as table_service:
            table_name = "fmorderledger"
            table_client = table_service.create_table_if_not_exists(table_name=table_name)
            table_ledger = TableLedger(table_client=table_client)
            table_signer = HashSigner()
            
            reporting.report_to_ledger(positions=closed_positions, ledger=table_ledger, signer=table_signer)
            
            ## only trigger a performance report occassionally due to it's processurally expensive nature
            if roll_dice():
                reporting.report_ledger_performance(ledger=table_ledger, signer=table_signer)
    except Exception as e:
        logging.error(f"error writing ledger - {e}", exc_info=True)
        report_problem(msg=f"error writing ledger", exc=e)
