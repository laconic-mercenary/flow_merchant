from discord import DiscordClient, WebhookMessage, Thumbnail, Author, Footer, Field, Embed, colors
from ledger import Ledger, Entry, Signer
from ledger_analytics import Analytics
from merchant_keys import keys as mkeys
from merchant_order import Order
from merchant_signal import MerchantSignal
from merchant import PositionsCheckResult
from persona import Persona
from personas import database, main_author, next_laggard_persona, next_leader_persona, next_loser_persona, next_winner_persona
from security import order_digest
from transactions import multiply, calculate_percent_diff, calculate_pnl
from utils import unix_timestamp_secs, time_from_timestamp, time_utc_as_str, roll_dice_33percent, null_or_empty, consts as utils_consts

import logging
import io
import os
import traceback

class cfg:
    def REPORTING_SIGNAL_RECEIVED() -> bool:
        enabled = os.environ.get("MERCHANT_REPORTING_SIGNAL_RECEIVED", "false")
        return enabled.lower() == "true"
    
    def REPORTING_STATE_CHANGED() -> bool:
        enabled = os.environ.get("MERCHANT_REPORTING_STATE_CHANGED", "false")
        return enabled.lower() == "true"
    
    def REPORTING_NEW_ORDERS() -> bool:
        enabled = os.environ.get("MERCHANT_REPORTING_NEW_ORDERS", "true")
        return enabled.lower() == "true"
    
    def REPORTING_CHECK_RESULTS() -> bool:
        enabled = os.environ.get("MERCHANT_REPORTING_CHECK_RESULTS", "true")
        return enabled.lower() == "true"

    def REPORTING_LEDGER_PERFORMANCE() -> bool:
        enabled = os.environ.get("MERCHANT_REPORTING_LEDGER_PERFORMANCE", "true")
        return enabled.lower() == "true"

    def REPORTING_IGNORE_DRY_RUN() -> bool:
        enabled = os.environ.get("MERCHANT_REPORTING_IGNORE_DRY_RUN", "false")
        return enabled.lower() == "true"


class MerchantReporting:

    def report_problem(self, msg:str, exc:Exception = None) -> None:
        if null_or_empty(msg):
            raise ValueError("msg cannot be empty")
        author = main_author(db=database())
        fields=[]
        if exc is not None:
            fields.append(Field(
                name="Message",
                value=str(exc)
            ))
            fields.append(Field(
                name="Traceback",
                value=self._traceback_as_str(exc)
            ))
        DiscordClient().send_webhook_message(msg=WebhookMessage(
            username=author.name,
            avatar_url=author.avatar_url,
            content="Got a problem here.",
            embeds=[
                Embed(
                    author=Author(
                        name=author.name,
                        icon_url=author.avatar_url
                    ),
                    title="An error occurred, here are the details.",
                    description=msg,
                    color=colors.RED(),
                    footer=Footer(
                        text="Beware that we may be out of sync with the broker if the error happend during a trade.",
                        icon_url=author.avatar_url
                    ),
                    thumbnail=Thumbnail(
                        url=author.avatar_url
                    ),
                    fields=fields
                )
            ]
        ))

    def report_signal_received(self, signal:MerchantSignal) -> None:
        ### nothing for now, keep the traffic lean
        if not cfg.REPORTING_SIGNAL_RECEIVED():
            logging.debug("signal received reporting is disabled")

    def report_state_changed(self, merchant_id: str, status: str, state: dict) -> None:
        if not cfg.REPORTING_STATE_CHANGED():
            logging.warning("merchant state changed reporting (buying, selling, shopping etc) is disabled")
            return
        if null_or_empty(merchant_id):
            raise ValueError("merchant_id cannot be empty")
        if null_or_empty(status):
            raise ValueError("status cannot be empty")
        if state is None:
            raise ValueError("state cannot be None")
        if not isinstance(state, dict):
            raise TypeError("state must be a dict")
        reportable_states = ["buying", "resting"] ## don't report states for now to avoid rate limiting on discord
        status_lower = status.lower()
        if status_lower not in reportable_states:
            return
        msg = f"Merchant {merchant_id} is now {status}"
        if status_lower == "resting":
            rest_interval_minutes = int(state[mkeys.REST_INTERVAL()])
            msg += f": for {rest_interval_minutes} minute(s)"
        author = main_author(db=database())
        DiscordClient().send_webhook_message(msg=WebhookMessage(
            username=author.name,
            avatar_url=author.avatar_url,
            content=msg,
            embeds=[]
        ))

    def report_order_placed(self, order:Order) -> None:
        if not cfg.REPORTING_NEW_ORDERS():
            logging.warning("reporting for new order placements is currently disabled")
            return
        if order is None:
            raise ValueError("order cannot be None")
        if not isinstance(order, Order):
            raise TypeError("order must be an instance of Order")
        ticker = order.ticker
        order_dry_run = order.metadata.is_dry_run
        main_order = order.sub_orders.main_order
        stop_loss_order = order.sub_orders.stop_loss
        take_profit_order = order.sub_orders.take_profit

        main_price = main_order.price
        main_contracts = main_order.contracts
        main_id = main_order.id
        stop_price = stop_loss_order.price
        take_profit_price = take_profit_order.price

        main_total = multiply(main_price, main_contracts)
        stop_price_per = calculate_percent_diff(main_price, stop_price)
        take_profit_price_per = calculate_percent_diff(main_price, take_profit_price)

        main_price = round(main_price, 8)
        main_total = round(main_total, 8)
        stop_price = round(stop_price, 8)
        take_profit_price = round(take_profit_price, 8)
        stop_price_per = round(stop_price_per, 3)
        take_profit_price_per = round(take_profit_price_per, 3)

        description = "[DRY RUN MODE] This is not a real order." if order_dry_run else "I have placed an order with the broker."
        
        persona_db = database()
        author = main_author(db=persona_db)

        DiscordClient().send_webhook_message(msg=WebhookMessage(
            username=author.name,
            avatar_url=author.avatar_url,
            content=f"New Order - {ticker}",
            embeds=[
                Embed(
                    author=Author(
                        name=author.name,
                        icon_url=author.avatar_url
                    ),
                    title="New order placed",
                    description=description,
                    color=colors.BLUE(),
                    footer=Footer(
                        text="To check the progress of the order, run the check_positions function.",
                        icon_url=author.avatar_url
                    ),
                    thumbnail=Thumbnail(
                        url=author.avatar_url
                    ),
                    fields=[
                        Field(
                            name="Ticker",
                            value=ticker
                        ),
                        Field(
                            name="Time(UTC)",
                            value=time_utc_as_str()
                        ),
                        Field(
                            name="Main price (total)",
                            value=f"{main_price} ({main_price * main_contracts})"
                        ),
                        Field(
                            name="Main contracts",
                            value=str(main_contracts)
                        ),
                        Field(
                            name="Stop price",
                            value=f"{stop_price} ({stop_price_per * 100}%)"
                        ),
                        Field(
                            name="Take profit price",
                            value=f"{take_profit_price} ({take_profit_price_per * 100}%)"
                        ),
                        Field(
                            name="Broker order ID",
                            value=main_id
                        )
                    ]
                )
            ]
        ))

    def report_check_results(self, results:PositionsCheckResult) -> None:
        if not cfg.REPORTING_CHECK_RESULTS():
            logging.warning("IMPORTANT - check results reporting (winners, leaders, laggards, losers) is currently disabled!")
            return
        if results is None:
            logging.warning(f"merchant_positions_checked() - no results")
            return
        if not isinstance(results, PositionsCheckResult):
            raise TypeError(f"results must be an instance of PositionsCheckResult, not {type(results)}")
        if results.current_prices is None:
            logging.warning(f"merchant_positions_checked() - no current price - {results.__dict__}")
            return
        if results.elapsed_ms is None:
            logging.warning(f"merchant_positions_checked() - no elapsed ms - {results.__dict__}")
            return
        
        logging.info(f"merchant_positions_checked() - with the following results: {results.__dict__}")
        
        if len(results.winners) == 0 and len(results.laggards) == 0 and len(results.leaders) == 0 and len(results.losers) == 0:
            logging.info(f"merchant_positions_checked() - no positions to report")
        else:
            self._send_check_result(
                winners=results.winners, 
                losers=results.losers, 
                leaders=results.leaders, 
                laggards=results.laggards, 
                current_prices=results.current_prices,
                elapsed_time_ms=results.elapsed_ms
            )

    def report_ledger_performance(self, ledger:Ledger, signer:Signer) -> None:
        if not cfg.REPORTING_LEDGER_PERFORMANCE():
            logging.warning("reporting ledger performance is disabled")
            return
        if ledger is None:
            raise ValueError("ledger cannot be None")
        if signer is None:
            raise ValueError("signer cannot be None")
        if not isinstance(ledger, Ledger):
            raise TypeError("ledger must be an instance of Ledger")
        if not isinstance(signer, Signer):
            raise TypeError("signer must be an instance of Signer")
        if roll_dice_33percent():
            deleted_logs = ledger.purge_old_logs()
            if len(deleted_logs) != 0:
                logging.info(f"removed {len(deleted_logs)} expired logs from the ledger: {deleted_logs}")
            
            bad_entries = ledger.verify_integrity(signer=signer)
            if len(bad_entries) != 0:
                msg = f"bad entries found in ledger: {bad_entries}"
                logging.critical(msg)
                self.report_problem(msg=msg, exc=Exception(msg))

        author = main_author(db=database())

        now = unix_timestamp_secs()
        report_timeframes = [ 
            # {
            #     "title": "Month To Date",
            #     "timeframe": now - utils_consts.ONE_MONTH_IN_SECS()
            # },
            {
                "title": "12 Hours",
                "timeframe": now - utils_consts.ONE_HOUR_IN_SECS(hours=12)
            } 
        ]
        embeds = []

        for report_timeframe in report_timeframes:
            ledger_entries = ledger.get_entries(
                                    name=None, 
                                    from_timestamp=report_timeframe.get("timeframe"),
                                    to_timestamp=now
                                )
            entries_by_tag = self._categorize_entries_by_tag(entries=ledger_entries)

            for tag in entries_by_tag:
                tagged_entries = entries_by_tag.get(tag)
                embed = self._embed_from_ledger_entries(
                            entries=tagged_entries, 
                            title=f"{report_timeframe.get('title')} - {tag}"
                        )
                embeds.append(embed)

        embed_limit = 10
        if len(embeds) > embed_limit:
            logging.warning(f"reached discord embed limit of {embed_limit} - truncating report...")
            embeds = embeds[:embed_limit]

        msg = WebhookMessage(
            username=author.name,
            avatar_url=author.avatar_url,
            content=f"Performance Report",
            embeds=embeds
        )

        logging.info(f"ledger performance report results: {msg}")
        DiscordClient().send_webhook_message(msg=msg)

    def _categorize_entries_by_tag(self, entries:list[Entry]) -> dict:
        results = {
            "All": []
        }
        for entry in entries:
            results["All"].append(entry)
            for tag in Order.from_dict(entry.data).metadata.tags:
                if tag not in results:
                    results[tag] = []
                results[tag].append(entry)
        breakdown = [ f"{tag}: {len(results.get(tag))}" for tag in results ]
        logging.info(f"categorizing entries by tag, total {len(entries)}, breakdown {breakdown}")
        return results

    def _embed_from_ledger_entries(self, entries:list[Entry], title:str) -> Embed:
        author = main_author(db=database())

        if len(entries) == 0:
            logging.info(f"No entries found in ledger - will skip analytics and performance reporting")
            return Embed(
                author=Author(
                    name=author.name,
                    icon_url=author.avatar_url
                ),
                title=title,
                description="No entries in ledger to analyze",
                color=colors.BLUE(),
                footer=Footer(
                    text="End Report",
                    icon_url=author.avatar_url
                ),
                thumbnail=Thumbnail(
                    url=author.avatar_url
                ),
                fields=[]
            )
        else:
            min_trades = 1
            max_analytics_entries = 4

            fields = []

            interval_analytics = Analytics.Intervals()
            spread_analytics = Analytics.Spreads()
            ticker_analytics = Analytics.Tickers()
            overall = Analytics.Overall()

            for ledger_entry in entries:
                interval_analytics.add(ledger_entry=ledger_entry)
                spread_analytics.add(ledger_entry=ledger_entry)
                ticker_analytics.add(ledger_entry=ledger_entry)
                overall.add(ledger_entry=ledger_entry)
            
            overall_results = overall.results()
            overall_results = [_overall for _overall in overall_results if _overall.total_trades >= min_trades]
            overall_results = overall_results[:min(len(overall_results), max_analytics_entries)]
            overall_payload = ""

            interval_results = interval_analytics.results()
            interval_results = [_interval for _interval in interval_results if _interval.total_trades >= min_trades]
            interval_results = interval_results[:min(len(interval_results), max_analytics_entries)]
            interval_payload = ""

            spread_results = spread_analytics.results()
            spread_results = [_spread for _spread in spread_results if _spread.total_trades >= min_trades]
            spread_results = spread_results[:min(len(spread_results), max_analytics_entries)]
            spread_payload = ""

            ticker_results = ticker_analytics.results()
            ticker_results = [_ticker for _ticker in ticker_results if _ticker.total_trades >= min_trades]
            ticker_results = ticker_results[:min(len(ticker_results), max_analytics_entries)]
            ticker_payload = ""

            overall_icon = "\U00002211"
            interval_icon = "\u23F0"
            spread_icon = "\U0001F503"
            ticker_icon = "\U0001F4CA"
            
            for overall in overall_results:
                overall_payload = overall_payload + f"{overall_icon} **Overall** ({len(overall_results)}): {round(overall.win_pct, 2) * 100.0}% ({overall.winning_trades}/{overall.total_trades} trades) - {round(overall.total_pnl, 4)}\n"

            for interval in interval_results:
                interval_payload = interval_payload + f"{interval_icon} *{interval.high_interval}/{interval.low_interval}* ({len(interval_results)}): {round(interval.win_pct, 2) * 100.0}% ({interval.winning_trades}/{interval.total_trades} trades) - {round(interval.total_pnl, 4)}\n"
            
            for spread in spread_results:
                spread_payload = spread_payload + f"{spread_icon} *{spread.take_profit}/{spread.stop_loss}* ({len(spread_results)}): {round(spread.win_pct, 2) * 100.0}% ({spread.winning_trades}/{spread.total_trades} trades) - {round(spread.total_pnl, 4)}\n"

            for ticker in ticker_results:
                ticker_payload = ticker_payload + f"{ticker_icon} *{ticker.ticker}* ({len(ticker_results)}): {round(ticker.win_pct, 2) * 100.0}% ({ticker.winning_trades}/{ticker.total_trades} trades) - {round(ticker.total_pnl, 4)}\n"

            fields.append(Field(
                name=f"Summary",
                value=f"{overall_payload}"
            ))
            fields.append(Field(
                name=f"By Interval",
                value=f"{interval_payload}"
            ))
            fields.append(Field(
                name=f"By Spread",
                value=f"{spread_payload}"
            ))
            fields.append(Field(
                name=f"By Ticker",
                value=f"{ticker_payload}"
            ))

            author = main_author(db=database())

            return Embed(
                author=Author(
                    name=author.name,
                    icon_url=author.avatar_url
                ),
                title=title,
                description="Overall, By Interval, By Spread, and By Ticker",
                color=colors.BLUE(),
                footer=Footer(
                    text="End Report",
                    icon_url=author.avatar_url
                ),
                thumbnail=Thumbnail(
                    url=author.avatar_url
                ),
                fields=fields
            )

    def _embed_from_ledger_performance(self, ledger:Ledger, signer:Signer, from_timestamp:int, title:str) -> Embed:
        entries = ledger.get_entries(
                        name=None, 
                        from_timestamp=from_timestamp, 
                        to_timestamp=unix_timestamp_secs(), 
                        include_tests=True
                    )
        return self._embed_from_ledger_entries(entries=entries, title=title)

    def report_to_ledger(self, positions:list[dict], ledger:Ledger, signer:Signer) -> None:
        if ledger is None:
            raise ValueError("ledger is None")
        if signer is None:
            raise ValueError("signer is None")
        if positions is None:
            raise ValueError("positions is None")
        for position in positions:
            order = Order.from_dict(position)
            
            pnl_dict = calculate_pnl(
                contracts=order.results.transaction.quantity,
                main_price=order.sub_orders.main_order.price,
                current_price=order.results.transaction.price,
                stop_price=0.0,
                profit_price=0.0
            )
            amount = pnl_dict.get("current_without_fees")

            last_entry = ledger.get_latest_entry()
            new_entry = Entry(
                name=order.ticker,
                amount=amount,
                hash=None,
                test=order.metadata.is_dry_run,
                timestamp=unix_timestamp_secs(),
                data=order.__dict__
            )
            new_entry.hash = signer.sign(new_entry=new_entry, prev_entry=last_entry)
            ledger.log(entry=new_entry)

    def _embed(self, author:str, author_icon:str, title:str, desc:str, color:int, footer:str, footer_icon:str, thumbnail:str, positions:list, prices:dict) -> Embed:
        icon_timestamp = "\U0001F55B"
        icon_stop_loss = "\U0001F6D1"
        icon_take_profit = "\U0001F3C6"
        icon_entry = "\U0001F4B5"
        icon_sell = "\U0001F58A"
        icon_real_order = "\U0001F4B0"

        def _current_profit(order:Order, current_price:float) -> float:
            main_price = order.sub_orders.main_order.price
            main_contracts = order.sub_orders.main_order.contracts
            return (current_price - main_price) * main_contracts
        
        def _sort_current_profit(order_dict:dict, prices:dict) -> float:
            order = Order.from_dict(order_dict)
            return _current_profit(order, prices.get(order.ticker))
        
        positions.sort(key=lambda x: _sort_current_profit(order_dict=x, prices=prices), reverse=True)

        fields = []
        discord_max_fields = 5
        max_iters = min(len(positions), discord_max_fields)
        for position in positions[:max_iters]:
            order = Order.from_dict(position)
            current_price = prices.get(order.ticker)
            dry_run_order = order.metadata.is_dry_run
            
            potential_profit = order.projections.profit_without_fees
            potential_loss = order.projections.loss_without_fees

            main_order = order.sub_orders.main_order
            stop_loss_order = order.sub_orders.stop_loss
            take_profit_order = order.sub_orders.take_profit
            
            main_price = main_order.price
            main_contracts = main_order.contracts

            stop_price = stop_loss_order.price
            take_profit_price = take_profit_order.price

            main_total = multiply(main_price, main_contracts)

            main_price = round(main_price, 7)
            main_contracts = round(main_contracts, 7)
            main_total = round(main_total, 7)
            stop_price = round(stop_price, 7)
            potential_loss = round(potential_loss, 7)
            take_profit_price = round(take_profit_price, 7)
            potential_profit = round(potential_profit, 7)
            current_price = round(current_price, 7)

            stop_price_per_diff = calculate_percent_diff(main_price, stop_price) * 100.0
            stop_price_per_diff = round(stop_price_per_diff, 3)
            take_profit_per_diff = calculate_percent_diff(main_price, take_profit_price) * 100.0
            take_profit_per_diff = round(take_profit_per_diff, 3)
            sell_now_per_diff = calculate_percent_diff(current_price, main_price) * 100.0
            sell_now_per_diff = round(sell_now_per_diff, 3)

            sell_now_profit_loss = round(_current_profit(order=order, current_price=current_price), 7)
            sell_now_msg = f"LOSS of {sell_now_profit_loss}" if sell_now_profit_loss < 0 else f"PROFIT of {sell_now_profit_loss}"
            
            order_sell_id = order_digest(order=order)
            order_friendly_timestamp = time_from_timestamp(order.metadata.time_created / 1000.0)
            ### TODO - hardcoded url and also securityType
            sell_now_link = f"[~SELL NOW~](https://stockton-jpe01-flow-merchant.azurewebsites.net/api/command/sell/{order_sell_id}?securityType=crypto)"

            real_world_order_icon = f"{icon_real_order}" if not dry_run_order else ""

            payload = f"PRICE @ {current_price} ({sell_now_per_diff}%) - {sell_now_msg}"
            payload += f"\n{icon_timestamp} _*{order_friendly_timestamp}*_ UTC"
            payload += f"\n{icon_entry} @ {main_price} x {main_contracts} = {main_total}"
            payload += f"\n{icon_stop_loss} @ {stop_price} ({stop_price_per_diff}%) for {potential_loss}"
            payload += f"\n{icon_take_profit} @ {take_profit_price} ({take_profit_per_diff}%) for {potential_profit}"
            payload += f"\n{icon_sell} {sell_now_link}"

            fields.append(Field(
                name=f"{order.ticker} - ({order.merchant_params.high_interval}/{order.merchant_params.low_interval}) {real_world_order_icon}",
                value=payload
            ))

        return Embed(
            author=Author(
                name=author,
                icon_url=author_icon
            ),
            title=title,
            description=desc,
            color=color,
            footer=Footer(
                text=footer,
                icon_url=footer_icon
            ),
            thumbnail=Thumbnail(url=thumbnail),
            fields=fields
        )
    
    def _send_check_result(self, winners:list, losers:list, leaders:list, laggards:list, current_prices:dict, elapsed_time_ms:int):
        embeds = []

        winners = self._apply_filters(orders=winners)
        losers = self._apply_filters(orders=losers)
        leaders = self._apply_filters(orders=leaders)
        laggards = self._apply_filters(orders=laggards)

        winners_ct = len(winners)
        losers_ct = len(losers)
        leaders_ct = len(leaders)
        laggards_ct = len(laggards)

        persona_db = database()
        if winners_ct != 0:
            winner_persona = next_winner_persona(db=persona_db)
            entry = self._embed(
                author=winner_persona.name,
                author_icon=winner_persona.avatar_url,
                title=f"{winner_persona.name} says: {winner_persona.quote}",
                desc="Presenting, your WINNERS:",
                color=colors.GREEN(),
                footer=winner_persona.advice,
                footer_icon=winner_persona.avatar_url,
                thumbnail=winner_persona.portrait_url,
                positions=winners,
                prices=current_prices
            )
            embeds.append(entry)
        
        if leaders_ct != 0:
            leader_persona = next_leader_persona(db=persona_db)
            entry = self._embed(
                author=leader_persona.name,
                author_icon=leader_persona.avatar_url,
                title=f"{leader_persona.name} says: {leader_persona.quote}",
                desc="Here are the LEADERS:",
                color=colors.LIGHT_BLUE(),
                footer=leader_persona.advice,
                footer_icon=leader_persona.avatar_url,
                thumbnail=leader_persona.portrait_url,
                positions=leaders,
                prices=current_prices
            )
            embeds.append(entry)

        if laggards_ct != 0:
            laggard_persona = next_laggard_persona(db=persona_db)
            entry = self._embed(
                author=laggard_persona.name,
                author_icon=laggard_persona.avatar_url,
                title=f"{laggard_persona.name} says: {laggard_persona.quote}",
                desc="These are currently negative:",
                color=colors.YELLOW(),
                footer=laggard_persona.advice,
                footer_icon=laggard_persona.avatar_url,
                thumbnail=laggard_persona.portrait_url,
                positions=laggards,
                prices=current_prices
            )
            embeds.append(entry)

        if losers_ct != 0:
            loser_persona = next_loser_persona(db=persona_db)
            entry = self._embed(
                author=loser_persona.name,
                author_icon=loser_persona.avatar_url,
                title=f"{loser_persona.name} says: {loser_persona.quote}",
                desc="These go on the Wall of Shame:",
                color=colors.RED(),
                footer=loser_persona.advice,
                footer_icon=loser_persona.avatar_url,
                thumbnail=loser_persona.portrait_url,
                positions=losers,
                prices=current_prices
            )
            embeds.append(entry)

        icon_report = "\U0001F4CA"
        icon_timestamp = "\U0001F552"
        icon_positions = "\U0001F4C8"
        icon_elapsed =  "\U000023F3"

        current_time = time_utc_as_str()
        main_message = f"{icon_report} **REPORT RESULTS**\n {icon_timestamp} *Time (UTC)*: __{current_time}__ \n {icon_positions} *Positions*: "
        if winners_ct != 0:
            main_message += f"{winners_ct} winner(s) "
        if leaders_ct != 0:
            main_message += f"{leaders_ct} leader(s) "
        if laggards_ct != 0:
            main_message += f"{laggards_ct} laggard(s) "
        if losers_ct != 0:
            main_message += f"{losers_ct} loser(s) "
        main_message += f"\n {icon_elapsed} *Elapsed*: {elapsed_time_ms}ms"
        
        author = main_author(db=persona_db)
        msg = WebhookMessage(
            username=author.name,
            avatar_url=author.avatar_url,
            content=main_message,
            embeds=embeds
        )
        logging.info(f"Sending Discord payload: {msg}")
        DiscordClient().send_webhook_message(msg=msg)

    def _apply_filters(self, orders:list[dict]) -> list[dict]:
        if cfg.REPORTING_IGNORE_DRY_RUN():
            orders = self._remove_dry_run_orders(orders)
        return orders

    def _remove_dry_run_orders(self, orders:list[dict]) -> list[dict]:
        return [order for order in orders if not Order.from_dict(order).metadata.is_dry_run]

    def _traceback_as_str(self, ex:Exception) -> str:
        out = io.StringIO()
        traceback.print_exception(ex, file=out)
        return out.getvalue().strip()
    
if __name__ == "__main__":
    import unittest

    class Test(unittest.TestCase):
        def test_send_check_result(self):
            instance = MerchantReporting()

    unittest.main()