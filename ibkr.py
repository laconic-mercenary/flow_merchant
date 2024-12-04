import logging
import requests
import os

from order_capable import Broker

def IBKR_ENV_GATEWAY_ENDPOINT():
    return "IBKR_GATEWAY_ENDPOINT" 

def IBKR_ENV_GATEWAY_PASSWD():
    return "IBKR_GATEWAY_PASSWORD"

class IBKRClient(Broker):

    def get_name(self):
        return "IBKR"

    def _cfg_gateway_endpoint(self) -> str:
        gateway_endpoint = os.environ[IBKR_ENV_GATEWAY_ENDPOINT()]
        if gateway_endpoint is None or len(gateway_endpoint) == 0:
            raise ValueError(f"{IBKR_ENV_GATEWAY_ENDPOINT()} cannot be None")
        return gateway_endpoint
    
    def _cfg_gateway_passwd(self) -> str:
        gateway_passwd = os.environ[IBKR_ENV_GATEWAY_PASSWD()]
        if gateway_passwd is None or len(gateway_passwd) == 0:
            raise ValueError(f"{IBKR_ENV_GATEWAY_PASSWD()} cannot be None")
        return gateway_passwd

