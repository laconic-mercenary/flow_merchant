
class action:
    @staticmethod
    def BUY():
        return "buy"
    
    @staticmethod
    def SELL():
        return "sell"

##
# Keys that are stored in the merchant state (Azure Storage)

class state:
    @staticmethod
    def SHOPPING():
        return "shopping"
    
    @staticmethod
    def BUYING():
        return "buying"
    
    @staticmethod
    def SELLING():
        return "selling"
    
    @staticmethod
    def RESTING():
        return "resting"

class keys:
    @staticmethod
    def PARTITIONKEY():
        return "PartitionKey"

    @staticmethod
    def ROWKEY():
        return "RowKey"
    
    @staticmethod
    def STATUS():
        return "status"
    
    @staticmethod
    def LAST_ACTION_TIME():
        return "last_action_time"
    
    @staticmethod
    def TICKER():
        return "ticker"
    
    @staticmethod
    def INTERVAL():
        return "interval"
    
    @staticmethod
    def REST_INTERVAL():
        return "rest_interval_minutes"

    @staticmethod
    def HIGH_INTERVAL():
        return "high_interval"

    @staticmethod
    def LOW_INTERVAL():
        return "low_interval"
    
    @staticmethod
    def ID():
        return "id"

    @staticmethod
    def VERSION():
        return "version"
    
    @staticmethod
    def ACTION():
        return "action"
    
    @staticmethod
    def STOPLOSS():
        return "stoploss"

    @staticmethod
    def TAKEPROFIT():
        return "takeprofit"

    @staticmethod
    def MERCHANT_ID():
        return "merchant_id"

    @staticmethod
    def BROKER_DATA():
        return "broker_data"

    @staticmethod
    def DRY_RUN():
        return "dry_run"

    class bkrdata:
        @staticmethod
        def ORDER():
            return "order"

        @staticmethod
        def TICKER():
            return "ticker"
        
        class order:
            @staticmethod
            def TICKER():
                return "ticker"

            @staticmethod
            def METADATA():
                return "metadata"
            
            class metadata:
                @staticmethod
                def DRY_RUN():
                    return "is_dry_run"
                
                @staticmethod
                def ID():
                    return "id"
                
                @staticmethod
                def TIME_CREATED():
                    return "time_created"
                
            @staticmethod
            def PROJECTIONS():
                return "projections"
            
            @staticmethod
            def SUBORDERS():
                return "orders"
            
            @staticmethod
            def MERCHANT_PARAMETERS():
                return "merchant_params"
            
            class merchant_params:
                @staticmethod
                def LOW_INTERVAL():
                    return "low_interval"
                
                @staticmethod
                def HIGH_INTERVAL():
                    return "high_interval"
                
                @staticmethod
                def VERSION():
                    return "version"
                
                @staticmethod
                def NOTES():
                    return "notes"
                
                @staticmethod
                def STOPLOSS_PERCENT():
                    return "stoploss_percent"
                
                @staticmethod
                def TAKEPROFIT_PERCENT():
                    return "takeprofit_percent"
            
            class projections:
                @staticmethod
                def PROFIT_WITHOUT_FEES():
                    return "profit_without_fees"
                
                @staticmethod
                def LOSS_WITHOUT_FEES():
                    return "loss_without_fees"
            
            class suborders:
                @staticmethod
                def MAIN_ORDER():
                    return "main"
                
                @staticmethod
                def STOP_LOSS():
                    return "stop_loss"
                
                @staticmethod
                def TAKE_PROFIT():
                    return "take_profit"
                
                class props:
                    @staticmethod
                    def ID():
                        return "id"
                    
                    @staticmethod
                    def TIME():
                        return "time"
                    
                    @staticmethod
                    def PRICE():
                        return "price"
                    
                    @staticmethod
                    def API_RX():
                        return "api_response"
                    
                    @staticmethod
                    def CONTRACTS():
                        return "contracts"
