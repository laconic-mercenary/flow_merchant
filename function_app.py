import logging
import os

import azure.functions as func
from azure.data.tables import TableServiceClient, TableClient

from broker_repository import BrokerRepository
from discord import DiscordClient
from events import EventLoggable
from ledger import Ledger, Entry, Signer
from merchant_signal import MerchantSignal
from merchant import Merchant
from merchant import cfg as merchant_cfg
from merchant_keys import keys as mkeys
from merchant_reporting import MerchantReporting
from server import *
from table_ledger import TableLedger, HashSigner
from utils import unix_timestamp_secs

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
    
    security_type = req.params.get("securityType", "crypto")        
    broker = BrokerRepository().get_for_security(security_type)

    with connect_table_service() as table_service:        
        merchant = Merchant(table_service, broker)
        subscribe_events(merchant=merchant)
        results = merchant.check_positions()
        return rx_json(results)
    
    return rx_not_found()

def handle_for_signals(req: func.HttpRequest) -> func.HttpResponse:
    headers = get_headers(req)
    logging.info(f"Trading View headers: {headers}")
    message_body = get_json_body(req)
    logging.info(f"received merchant signal: {message_body}")

    try:
        signal = MerchantSignal.parse(message_body)
        
        if not is_authorized(signal.api_token()):
            return rx_unauthorized()
        
        broker = BrokerRepository().get_for_security(signal.security_type())
    
        with connect_table_service() as table_service:    
            merchant = Merchant(table_service, broker)
            subscribe_events(merchant=merchant)
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

def subscribe_events(merchant: Merchant) -> None:
    merchant.on_order_placed += merchant_order_placed
    merchant.on_positions_check += merchant_positions_checked
    merchant.on_signal_received += merchant_signal_received
    merchant.on_state_change += merchant_state_changed

def report_problem(msg:str, exc:Exception, additional_data:dict = {}) -> None:
    msg = f"Message: {msg} -- Data: {additional_data}"
    MerchantReporting().report_problem(msg=msg, exc=exc)

def merchant_state_changed(merchant_id: str, status: str, state: dict) -> None:
    MerchantReporting().report_state_changed(merchant_id, status, state)

def merchant_signal_received(merchant_id: str, signal: MerchantSignal) -> None:
    MerchantReporting().report_signal_received(signal)
    
def merchant_order_placed(merchant_id: str, order_data: dict) -> None:
    MerchantReporting().report_order_placed(order_data)

def merchant_positions_checked(results: dict) -> None:
    MerchantReporting().report_check_results(results)
    current_positions = results.get("positions")
    winners = current_positions.get("winners")
    losers = current_positions.get("losers")
    try:
        write_ledger(positions=winners + losers)
    except Exception as e:
        logging.error(f"error writing ledger - {e}", exc_info=True)
        report_problem(msg=f"error writing ledger", exc=e)

def write_ledger(positions:list) -> None:
    with connect_table_service() as table_service:
        table_name = "fmorderledger"
        table_client = table_service.create_table_if_not_exists(table_name=table_name)
        table_ledger = TableLedger(table_client=table_client)
        table_signer = HashSigner()
        for position in positions:
            if mkeys.bkrdata.order.PROJECTIONS() not in position:
                raise ValueError(f"expected key '{mkeys.bkrdata.order.PROJECTIONS()}' in position {position}")
            if mkeys.bkrdata.order.TICKER() not in position:
                raise ValueError(f"expected key '{mkeys.bkrdata.order.TICKER()}' in position {position}")
            if mkeys.bkrdata.order.METADATA() not in position:
                raise ValueError(f"expected key '{mkeys.bkrdata.order.METADATA()}' in position {position}")

            metadata = position.get(mkeys.bkrdata.order.METADATA())
            ticker = position.get(mkeys.bkrdata.order.TICKER())
            projections = position.get(mkeys.bkrdata.order.PROJECTIONS())

            ## ! TODO - only accounts for trailing stop strategy !
            amount = projections.get("loss_without_fees")
            dry_run = metadata.get(mkeys.bkrdata.order.metadata.DRY_RUN())

            last_entry = table_ledger.get_latest_entry()
            new_entry = Entry(
                name=ticker,
                amount=amount,
                hash=None,
                test=dry_run,
                timestamp=unix_timestamp_secs()
            )
            new_entry.hash = table_signer.sign(new_entry=new_entry, prev_entry=last_entry)
            table_ledger.log(entry=new_entry)

        deleted_logs = table_ledger.purge_old_logs()
        if len(deleted_logs) != 0:
            logging.info(f"removed the following from the ledger (expired): {deleted_logs}")
        
        bad_entries = table_ledger.verify_integrity(signer=table_signer)
        if len(bad_entries) != 0:
            logging.error(f"bad entries found in ledger: {bad_entries}")
