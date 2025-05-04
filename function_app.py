import json
import logging
import os

import azure.functions as func
from azure.data.tables import TableServiceClient

from broker_repository import BrokerRepository
from ledger_analytics import Analytics
from merchant_signal import MerchantSignal
from merchant_order import Order
from merchant import Merchant, PositionsCheckResult
from merchant_reporting import MerchantReporting
from server import *
from table_ledger import TableLedger, HashSigner
from utils import null_or_empty, time_utc_as_str, unix_timestamp_secs, roll_dice_5percent as roll_dice, consts as util_consts

app = func.FunctionApp()        

###
# /performance
###

@app.route(route="performance/{hours}/{query}/{identifier}",
           methods=["GET"],
           auth_level=func.AuthLevel.ANONYMOUS)
def performance(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("test() - invoked")
    if "identifier" not in req.route_params:
        return rx_bad_request("identifier is required")
    if "hours" not in req.route_params:
        return rx_bad_request("hours is required")
    
    identifier:str = req.route_params.get("identifier")
    query:str = req.route_params.get("query")
    hours:int = int(req.route_params.get("hours"))

    query = query.strip().upper()
    identifier = identifier.strip().upper()
    try:
        return handle_performance_metrics(
                    hours=hours, 
                    query=query, 
                    identifier=identifier
                )
    except Exception as e:
        logging.error("error in handling performance request", exc_info=True)
        report_problem(msg="error in handling performance request", exc=e)
        return rx_not_found()

def handle_performance_metrics(hours:int, query:str, identifier:str) -> func.HttpResponse:
    if query == "TICKER": 
        if not identifier.isalpha():
            logging.warning("identifier not alphabetic")
            return rx_bad_request()
    elif query == "SPREAD":
        if "-" not in identifier:
            logging.warning("identifier does not contain '-'")
            return rx_bad_request()
    elif query == "INTERVAL":
        if not identifier.isnumeric():
            logging.warning("identifier not numeric")
            return rx_bad_request()
    elif query == "ALL":
        ### ignore the identifier if querying for all
        identifier = "ALL"
    else:
        logging.warning(f"invalid query: {query}")
        return rx_bad_request()
    
    if len(identifier) > 25:
        logging.warning("identifier too long")
        return rx_bad_request()
    if identifier == "ALL":
        logging.warning(f"identifier is 'all' - will query all assets for hours {hours}")
        identifier = None
    if hours <= 0:
        logging.warning("hours <= 0")
        return rx_bad_request()
    if util_consts.ONE_HOUR_IN_SECS(hours=hours) > util_consts.ONE_WEEK_IN_SECS(weeks=1):
        logging.warning(f"hours too high: {hours}")
        return rx_bad_request()
    
    with connect_table_service() as table_service:        
        table_name = "fmorderledger"
        table_client = table_service.create_table_if_not_exists(table_name=table_name)
        table_ledger = TableLedger(table_client=table_client)
        now_ts = unix_timestamp_secs()
        from_ts = now_ts - util_consts.ONE_HOUR_IN_SECS(hours=hours)

        filters = {}
        if query == "INTERVAL":
            interval = int(identifier)
            filters.update({
                "merchant_params": {
                    "high_interval": str(interval)
                }
            })
            identifier = None
        elif query == "SPREAD":
            take_profit, stop_loss = parse_spread(identifier=identifier)
            filters.update({
                "merchant_params": {
                    "stoploss_percent": float(stop_loss),
                    "takeprofit_percent": float(take_profit)
                }
            })
            identifier = None

        entries = table_ledger.get_entries(
                        name=identifier,
                        from_timestamp=from_ts,
                        to_timestamp=now_ts,
                        filters=filters
                    )
        entries.sort(key=lambda x: x.timestamp)
        metrics = Analytics.all_performance_metrics(ledger_entries=entries)
        return rx_json(metrics)
    
def parse_spread(identifier:str) -> tuple[str, str]:
    ### format is {high}-{low}
    split_results = identifier.split("-")
    if len(split_results) != 2:
        raise ValueError(f"invalid spread identifier: {identifier}")
    high = split_results[0]
    low = split_results[1]
    return high, low

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
    request_body = req.get_body()
    try:
        if request_body is None:
            logging.warning(f"signals() - empty request body")
            return rx_bad_request()
        if null_or_empty(request_body.decode("utf-8")):
            logging.warning(f"signals() - empty request body")
            return rx_bad_request()
        
        headers = get_headers(req=req)
        logging.info(f"request headers: {headers}")
        signal_dict = get_json_body(req=req)
        logging.info(f"received merchant signal: {signal_dict}")

        return handle_for_signals(message_body=signal_dict)
    except json.decoder.JSONDecodeError as jde:
        if request_body is not None:
            request_body = request_body.decode("utf-8")
        logging.error(f"error handling signals - {jde}, request body - {request_body}", exc_info=True)
        report_problem(
            msg=f"Invalid JSON received - double check your signal", 
            exc=jde, 
            additional_data={"request_body": request_body}
        )
    except Exception as e:
        if request_body is not None:
            request_body = request_body.decode("utf-8")
        logging.error(f"error handling signals - {e}, request body - {request_body}", exc_info=True)
        report_problem(
            msg=f"Invalid JSON received - double check your signal", 
            exc=jde
        )
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
        logging.warning(f"command() - missing instruction")
        return rx_bad_request()
    if "identifier" not in req.route_params:
        logging.warning(f"command() - missing identifier")
        return rx_bad_request()
    
    instruction = req.route_params.get("instruction")
    identifier = req.route_params.get("identifier")

    if null_or_empty(instruction) or null_or_empty(identifier):
        logging.warning(f"command() - missing instruction({instruction}) or identifier({identifier})")  
        return rx_bad_request()
    try:
        logging.info(f"command() - handling instruction: {instruction}, identifier: {identifier}")
        return handle_instruction_for_command(
            command=instruction, 
            identifier=identifier
        )
    except Exception as e:
        logging.error(f"error handling cmd - {e}", exc_info=True)
        report_problem(msg=f"error handling cmd", exc=e)
    return rx_bad_request()

def handle_instruction_for_command(command:str, identifier:str) -> func.HttpResponse:
    logging.info(f"received command: command={command}, identifier={identifier}")
    if command == "sell":
        return handle_command_for_sell(identifier=identifier)
    elif command == "report_performance":
        return handle_command_for_report_performance(identifer=identifier)
    logging.warning(f"unknown command {command} - ignoring")
    return rx_bad_request()


###
# support
###

def connect_table_service() -> TableServiceClient:
    return TableServiceClient.from_connection_string(os.environ["storageAccountConnectionString"])

def handle_command_for_sell(identifier:str) -> func.HttpResponse:
    broker_repo = BrokerRepository()
    with connect_table_service() as table_service:        
        merchant = Merchant(table_service=table_service, broker=broker_repo.invalid_broker())
        subscribe_events(merchant=merchant)

        order, position = merchant.find_order_by_identifier(identifier=identifier)
        if order is None or position is None:
            return rx_not_found("Unable to sell - order not found or no longer exists")
        
        broker = broker_repo.get_for_security(order.metadata.security_type.value)
        merchant.main_broker(main_broker=broker)
        result = merchant.sell(order=order, position=position)

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
        now_ts = unix_timestamp_secs()
        from_ts = now_ts - util_consts.ONE_HOUR_IN_SECS(hours=report_hours)
        entries = table_ledger.get_entries(
                        name=identifer,
                        from_timestamp=from_ts,
                        to_timestamp=now_ts
                    )
        MerchantReporting().report_performance_for_entries(
                                ledger_entries=entries,
                                title=f"{identifer} - {report_hours} hours"
                            )
        return rx_json({
            "identifer": identifer,
            "report_hours": report_hours,
            "ledger_entries_processed": len(entries),
            "status": "ok",
            "from_timestamp": from_ts,
            "to_timestamp": now_ts
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
