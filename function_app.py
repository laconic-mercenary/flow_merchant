import json
import logging
import os

import azure.functions as func
from azure.data.tables import TableServiceClient

from broker_repository import BrokerRepository
from merchant_signal import MerchantSignal
from merchant_order import Order
from merchant import Merchant, PositionsCheckResult
from merchant_reporting import MerchantReporting
from server import *
from table_ledger import TableLedger, HashSigner
from utils import null_or_empty, time_utc_as_str, unix_timestamp_secs, roll_dice_10percent as roll_dice, consts as util_consts

app = func.FunctionApp()

###
# /positions
###

@app.route(route="positions",
           methods=["GET"],
           auth_level=func.AuthLevel.ANONYMOUS)
def positions(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("positions() - invoked")
    try:
        if is_health_check(req):
            return rx_ok()
    
        security_type = req.params.get("securityType", "crypto")        
        with connect_table_service() as table_service:        
            return handle_for_positions(
                        security_type=security_type, 
                        table_service=table_service
                    )
    except Exception as e:
        logging.error(f"error handling positions - {e}", exc_info=True)
        report_problem(msg=f"error handling positions", exc=e)
    return rx_not_found()
    
###
# /signals
###

@app.route( route="signals", 
            methods=["POST"],
            auth_level=func.AuthLevel.ANONYMOUS )
def signals(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("signals() - invoked")
    try:
        if req.get_body() is None:
            return rx_bad_request()
        if null_or_empty(req.get_body().decode("utf-8")):
            return rx_bad_request()
        
        headers = get_headers(req=req)
        logging.info(f"request headers: {headers}")
        message_body = get_json_body(req=req)
        logging.info(f"received merchant signal: {message_body}")

        return handle_for_signals(message_body=message_body)
    except json.decoder.JSONDecodeError as jde:
        body = req.get_body().decode("utf-8")
        logging.error(f"error handling signals - {jde}, request body - {body}", exc_info=True)
        report_problem(msg=f"Invalid JSON received - double check your signal", exc=jde, additional_data={"request_body": body})
    except Exception as e:
        logging.error(f"error handling signals - {e}, request body - {req.get_body().decode('utf-8')}", exc_info=True)
        report_problem(msg=f"error handling signals", exc=e)
    return rx_bad_request()

###
# /command/{instruction}/{identifier}
###
    
@app.route(route="command/{instruction}/{identifier}",
           methods=["GET"],
           auth_level=func.AuthLevel.ANONYMOUS)
def command(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("command() - invoked")
    if "instruction" not in req.route_params:
        return rx_bad_request()
    if "identifier" not in req.route_params:
        return rx_bad_request()
    if "securityType" not in req.params:
        return rx_bad_request()
    
    instruction = req.route_params.get("instruction")
    identifier = req.route_params.get("identifier")
    security_type = req.params.get("securityType")

    if null_or_empty(instruction) or null_or_empty(identifier) or null_or_empty(security_type):
        logging.warning(f"command() - missing instruction({instruction}) or identifier({identifier}) or security type({security_type})")  
        return rx_bad_request()
    try:
        return handle_instruction_for_command(
            command=instruction, 
            identifier=identifier,
            security_type=security_type
        )
    except Exception as e:
        logging.error(f"error handling cmd - {e}", exc_info=True)
        report_problem(msg=f"error handling cmd", exc=e)
    return rx_bad_request()

###
# support
###

def connect_table_service() -> TableServiceClient:
    return TableServiceClient.from_connection_string(os.environ["storageAccountConnectionString"])

def handle_instruction_for_command(command:str, identifier:str, security_type:str) -> func.HttpResponse:
    logging.info(f"received command: command={command}, identifier={identifier}, security_type={security_type}")
    if command == "sell":
        return handle_command_for_sell(identifier=identifier, security_type=security_type)
    elif command == "report_performance":
        return handle_command_for_report_performance(identifer=identifier)
    logging.warning(f"unknown command {command} - ignoring")
    return rx_bad_request()

def handle_command_for_sell(identifier:str, security_type:str) -> func.HttpResponse:
    broker = BrokerRepository().get_for_security(security_type=security_type)
    with connect_table_service() as table_service:        
        merchant = Merchant(table_service=table_service, broker=broker)
        subscribe_events(merchant=merchant)
        result = merchant.sell(identifier=identifier)
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
    
def handle_command_for_report_performance(identifer:str) -> func.HttpResponse:
    identifer = identifer.strip()
    if len(identifer) > 50:
        logging.warning(f"identifer too long: {identifer}")
        return rx_bad_request()
    if not identifer.isalnum():
        logging.warning(f"identifer should be alphanumeric: {identifer}")
        return rx_bad_request()
    if identifer.upper() != identifer:
        logging.warning(f"identifer should be upper case: {identifer}")
        return rx_bad_request()
    with connect_table_service() as table_service:
        table_name = "fmorderledger"
        table_client = table_service.create_table_if_not_exists(table_name=table_name)
        table_ledger = TableLedger(table_client=table_client)
        report_hours = 24
        entries = table_ledger.get_entries(
                        name=identifer,
                        from_timestamp=util_consts.ONE_HOUR_IN_SECS(hours=report_hours),
                        to_timestamp=unix_timestamp_secs()
                    )
        MerchantReporting().report_performance_for_entries(
                                ledger_entries=entries,
                                title=f"{identifer} - {report_hours} hours"
                            )
        return rx_json({
            "identifer": identifer,
            "report_hours": report_hours,
            "ledger_entries_processed": len(entries),
            "status": "ok"
        })

def handle_for_positions(security_type:str, table_service:TableServiceClient) -> func.HttpResponse:
    broker = BrokerRepository().get_for_security(security_type=security_type)
    merchant = Merchant(table_service=table_service, broker=broker)
    subscribe_events(merchant=merchant)
    results = merchant.check_positions()
    return rx_json(data=results.__dict__)

def handle_for_signals(message_body:dict) -> func.HttpResponse:
    signal = MerchantSignal.parse(msg_body=message_body)
    
    if not is_authorized(client_token=signal.api_token()):
        return rx_unauthorized()
    
    broker = BrokerRepository().get_for_security(security_type=signal.security_type())

    with connect_table_service() as table_service:    
        merchant = Merchant(table_service=table_service, broker=broker)
        subscribe_events(merchant=merchant)
        merchant.handle_market_signal(signal=signal)
    return rx_ok()

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
        MerchantReporting().report_state_changed(merchant_id=merchant_id, status=status, state=state)
    except Exception as e:
        logging.error(f"error reporting state change - {e}", exc_info=True)
        report_problem(msg=f"error reporting state change", exc=e)

def merchant_signal_received(merchant_id: str, signal: MerchantSignal) -> None:
    try:
        MerchantReporting().report_signal_received(signal=signal)
    except Exception as e:
        logging.error(f"error reporting signal received - {e}", exc_info=True)
        report_problem(msg=f"error reporting signal received", exc=e)

def merchant_order_placed(merchant_id: str, order_data: Order) -> None:
    try:
        MerchantReporting().report_order_placed(order=order_data)
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
