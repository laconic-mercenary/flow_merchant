
import uuid
import logging

class MerchantSignal:

    def __init__(self, msg_body):
        if not msg_body:
            raise ValueError("Message body cannot be null")
        self.msg = msg_body
        self.metadata = msg_body.get("metadata", {})
        self.security = msg_body.get("security", {})
        self.flowmerchant = msg_body.get("flowmerchant", {})
        self._notes = msg_body.get("notes", "")
        self.TABLE_NAME = "flowmerchant"
        self.__id = str(uuid.uuid4())

    @staticmethod
    def parse(msg_body):
        if not msg_body:
            raise ValueError("Message body cannot be null")

        # Validate metadata
        metadata = msg_body.get("metadata")
        if not metadata:
            logging.error("Metadata is missing")
            raise ValueError("Metadata is required")
        if "key" not in metadata:
            logging.error("API key is missing in metadata")
            raise ValueError("API key is required in metadata")

        # Validate security
        security = msg_body.get("security")
        if not security:
            logging.error("Security information is missing")
            raise ValueError("Security information is required")
        required_security_keys = ["ticker", "contracts", "interval", "price"]
        for key in required_security_keys:
            if key not in security:
                logging.error(f"Missing required security key: {key}")
                raise ValueError(f"Missing required security key: {key}")

        # Validate price
        price = security.get("price")
        if not price:
            logging.error("Price information is missing in security")
            raise ValueError("Price information is required in security")
        required_price_keys = ["close"]
        for key in required_price_keys:
            if key not in price:
                logging.error(f"Missing required price key: {key}")
                raise ValueError(f"Missing required price key: {key}")

        # Validate flowmerchant
        flowmerchant = msg_body.get("flowmerchant")
        if not flowmerchant:
            logging.error("Flowmerchant information is missing")
            raise ValueError("Flowmerchant information is required")
        required_flowmerchant_keys = ["low_interval", "high_interval", "action"]
        for key in required_flowmerchant_keys:
            if key not in flowmerchant:
                logging.error(f"Missing required flowmerchant key: {key}")
                raise ValueError(f"Missing required flowmerchant key: {key}")
        if "suggested_stoploss" in flowmerchant or "takeprofit_percent" in flowmerchant:
            if not "suggested_stoploss" in flowmerchant:
                raise ValueError("suggested_stoploss is required when takeprofit_percent is present")
            if not "takeprofit_percent" in flowmerchant:
                raise ValueError("takeprofit_percent is required when suggested_stoploss is present")
            float(flowmerchant["suggested_stoploss"])
            float(flowmerchant["takeprofit_percent"])

        # Validate action
        if flowmerchant.get("action") not in ["buy", "sell"]:
            logging.error(f"Invalid action: {flowmerchant['action']}")
            raise ValueError("Invalid action")

        # Validate data types
        float(security["price"].get("high", 0.0))
        float(security["price"].get("low", 0.0))
        float(security["price"].get("open", 0.0))
        float(security["price"].get("close"))
        
        if not isinstance(security.get("contracts"), float):
            if not isinstance(security.get("contracts"), int):
                logging.error(f"Contracts must be numeric: {security['contracts']}")
                raise ValueError("Contracts must be numeric")
        
        if security.get("contracts") <= 0:
            logging.error(f"Contracts must be greater than 0: {security['contracts']}")
            raise ValueError("Contracts must be greater than 0")

        if not isinstance(flowmerchant.get("version", 1), int):
            logging.error(f"Version must be an integer: {flowmerchant['version']}")
            raise ValueError("Version must be an integer")
        
        try:
            float(flowmerchant.get("suggested_stoploss", 0.0))
            float(flowmerchant.get("takeprofit_percent", 0.0))
        except ValueError as e:
            logging.error(f"Flowmerchant values must be numbers: {e}")
            raise ValueError("Flowmerchant values must be numbers")

        return MerchantSignal(msg_body)

    # Accessor methods for metadata
    def api_token(self) -> str:
        return self.metadata.get("key")

    # Accessor methods for security
    def ticker(self) -> str:
        return self.security.get("ticker")

    def exchange(self) -> str:
        return self.security.get("exchange", "none")

    def security_type(self) -> str:
        return self.security.get("type", "crypto")

    def contracts(self) -> int:
        return self.security.get("contracts")

    def interval(self) -> str:
        return self.security.get("interval")

    def high(self) -> float:
        return float(self.security["price"].get("high", 0.0))

    def low(self) -> float:
        return float(self.security["price"].get("low", 0.0))

    def open(self) -> float:
        return float(self.security["price"].get("open", 0.0))

    def close(self) -> float:
        return float(self.security["price"].get("close"))

    # Accessor methods for flowmerchant
    def suggested_stoploss(self) -> float:
         stop_loss = float(self.flowmerchant.get("suggested_stoploss", 0.0))
         if stop_loss < 0.0 or stop_loss > 100.0:
             raise ValueError("stop_loss must be between 0.0 and 100.0")
         return stop_loss    

    def takeprofit_percent(self) -> float:
        take_profit = float(self.flowmerchant.get("takeprofit_percent", 0.0))
        if take_profit < 0.0 or take_profit > 100.0:
            raise ValueError("take_profit must be between 0.0 and 100.0")
        return take_profit

    def rest_interval(self) -> int:
        return int(self.flowmerchant.get("rest_interval_minutes", "15"))

    def version(self) -> int:
        return int(self.flowmerchant.get("version", 1))

    def action(self) -> str:
        return self.flowmerchant.get("action")
    
    def low_interval(self) -> str:
        return self.flowmerchant.get("low_interval")
    
    def high_interval(self) -> str:
        return self.flowmerchant.get("high_interval")

    def rest_after_buy(self) -> bool:
        return self.flowmerchant.get("rest_after_buy", False)
        
    def dry_run(self) -> bool:
        return bool(self.flowmerchant.get("dry_run", False))
        
    def notes(self) -> str:
        return self._notes
    
    def id(self) -> str:
        return self.__id

    def broker_params(self) -> dict:
        return self.flowmerchant.get("broker_params", {})

    def __str__(self) -> str:
        return (
            f"MerchantSignal(id={self.id()}, "
            f"action={self.action()}, "
            f"ticker={self.ticker()}, "
            f"close={self.close()}, "
            f"interval={self.interval()}, "
            f"low_interval={self.low_interval()}, "
            f"high_interval={self.high_interval()}, "
            f"suggested_stoploss={self.suggested_stoploss()}, "
            f"high={self.high()}, "
            f"low={self.low()}, "
            f"takeprofit_percent={self.takeprofit_percent()}, "
            f"contracts={self.contracts()}, "
            f"version={self.version()}, "
            f"rest_interval={self.rest_interval()}, "
            f"rest_after_buy={self.rest_after_buy()}, "
            f"notes={self.notes()}"
            f")"
        )

    def info(self) -> str:
        return str(self)
