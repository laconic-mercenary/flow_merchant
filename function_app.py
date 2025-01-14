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
from merchant_reporting import MerchantReporting
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
        report_problem(msg=f"error handling positions", exc=e)
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
        report_problem(msg=f"error handling market signal", exc=e)
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
        report_problem(msg=f"error handling cmd", exc=e)
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

def subscribe_events(merchant: Merchant) -> None:
    merchant.on_order_placed += merchant_order_placed
    merchant.on_positions_check += merchant_positions_checked
    merchant.on_signal_received += merchant_signal_received
    merchant.on_state_change += merchant_state_changed

def report_problem(msg:str, exc:Exception) -> None:
    report = MerchantReporting()
    report.report_problem(msg=msg, exc=exc)

def merchant_state_changed(merchant_id: str, status: str, state: dict) -> None:
    report = MerchantReporting()
    report.report_state_changed(merchant_id, status, state)

def merchant_signal_received(merchant_id: str, signal: MerchantSignal) -> None:
    report = MerchantReporting()
    report.report_signal_received(signal)
    
def merchant_order_placed(merchant_id: str, order_data: dict) -> None:
    report = MerchantReporting()
    report.report_order_placed(order_data)

def merchant_positions_checked(results: dict) -> None:
    report = MerchantReporting()
    report.report_check_results(results)

def write_ledger(positions:list, result:str) -> None:
    with connect_table_service() as table_service:
        table_name = "fmorderledger"
        table_client = table_service.create_table_if_not_exists(table_name=table_name)
        table_ledr = TableLedger(table_client=table_client)
        ledgers = [ table_ledr ]
        for pos in positions:
            if "orders" not in pos:
                raise ValueError(f"expected key 'orders' in position {pos}")
            if "projections" not in pos:
                raise ValueError(f"expected key 'projections' in pos {pos}")
            if "ticker" not in pos:
                raise ValueError(f"expected key 'ticker' in order {pos}")
            ticker = pos.get("ticker")
            projections = pos.get("projections")
            amount = projections.get("profit_without_fees") if result == ledger_result.PROFIT() else projections.get("loss_without_fees")
            for ledger in ledgers:
                ledger.log(ticker=ticker, amount=amount, res=result)
        deleted_logs = table_ledr.purge_old_logs()
        if len(deleted_logs) != 0:
            logging.info(f"removed the following from the ledger (expired): {deleted_logs}")
