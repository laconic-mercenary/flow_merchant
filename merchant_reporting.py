from discord import DiscordClient, WebhookMessage, Thumbnail, Author, Footer, Field, Embed, colors
from ledger import Ledger, Entry, Signer
from ledger_analytics import LedgerAnalytics, LedgerAnalysis, PerformanceView, SpreadData, IntervalData, TickerData
from merchant_keys import keys as mkeys
from merchant_order import Order
from merchant_signal import MerchantSignal
from persona import Persona
from personas import database, main_author, next_laggard_persona, next_leader_persona, next_loser_persona, next_winner_persona
from transactions import multiply, calculate_percent_diff
from utils import unix_timestamp_secs, unix_timestamp_ms, roll_dice_10percent, consts as utils_consts

import datetime
import logging
import io
import traceback

class MerchantReporting:

    def report_problem(self, msg:str, exc:Exception) -> None:
        author = main_author(db=database())
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
                    fields=[
                        Field(
                            name="Message",
                            value=str(exc)
                        ),
                        Field(
                            name="Traceback",
                            value=self._traceback_as_str(exc)
                        )
                    ]
                )
            ]
        ))

    def report_signal_received(self, signal:MerchantSignal) -> None:
        pass

    def report_state_changed(self, merchant_id: str, status: str, state: dict) -> None:
        reportable_states = ["buying", "resting"]
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
                            name="Main price",
                            value=str(main_price)
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

    def report_check_results(self, results:dict) -> None:
        if "positions" not in results:
            logging.warning(f"merchant_positions_checked() - no current positions - {results}")
            return
        if "elapsed_ms" not in results:
            logging.warning(f"merchant_positions_checked() - no elapsed ms - {results}")
            return
        if "current_prices" not in results:
            logging.warning(f"merchant_positions_checked() - no current prices - {results}")
            return

        logging.info(f"merchant_positions_checked() - with the following results: {results}")
        
        elapsed_ms = results.get("elapsed_ms")
        current_positions = results.get("positions")
        current_prices = results.get("current_prices")

        winners = current_positions.get("winners")
        laggards = current_positions.get("laggards")
        leaders = current_positions.get("leaders")
        losers = current_positions.get("losers")

        ## winners.sort(key=lambda x: x.profit_loss, reverse=True)

        if len(winners) == 0 and len(laggards) == 0 and len(leaders) == 0 and len(losers) == 0:
            logging.info(f"merchant_positions_checked() - no positions to report")
        else:
            self._send_check_result(
                winners=winners, 
                losers=losers, 
                leaders=leaders, 
                laggards=laggards, 
                current_prices=current_prices,
                elapsed_time_ms=elapsed_ms
            )

    def report_ledger_performance(self, ledger:Ledger, signer:Signer, analytics:LedgerAnalytics) -> None:
        deleted_logs = ledger.purge_old_logs()
        if len(deleted_logs) != 0:
            logging.info(f"removed {len(deleted_logs)} expired logs from the ledger: {deleted_logs}")
        
        if roll_dice_10percent():
            bad_entries = ledger.verify_integrity(signer=signer)
            if len(bad_entries) != 0:
                msg = f"bad entries found in ledger: {bad_entries}"
                logging.critical(msg)
                self.report_problem(msg=msg, exc=Exception(msg))

        now_ts = unix_timestamp_secs()
        one_week_ago_ts = now_ts - utils_consts.ONE_WEEK_IN_SECS()
        entries = ledger.get_entries(name=None, from_timestamp=one_week_ago_ts, to_timestamp=now_ts)
        
        if len(entries) == 0:
            logging.info(f"No entries found in ledger - will skip analytics and performance reporting")
        else:
            analysis = analytics.performance_by_interval(entries=entries)
            persona = main_author(db=database())
            DiscordClient().send_webhook_message(msg=WebhookMessage(
                username=persona.name,
                avatar_url=persona.avatar_url,
                content="Performance Report",
                embeds=self._embeds_from_analysis(analysis=analysis)
            ))

    def _field_from_analysis(self, interval_performance:PerformanceView.IntervalPerformance) -> Field:
        new_line = "\n"
        with io.StringIO(initial_value="", newline=new_line) as field_report:
            field_report.write(f"Top Spreads, Top Tickers in Spread{new_line}")
            current_interval = interval_performance.interval
            count = 1
            for spread_performance in interval_performance.spreads[:min(len(interval_performance.spreads), 5)]:
                current_spread:SpreadData = spread_performance.spread
                tickers = spread_performance.tickers
                top_tickers = ""
                for ticker in tickers[:min(len(tickers), 5)]:
                    top_tickers += f"{ticker.ticker()} ({round(ticker.performance.win_pct * 100.0, 3)}% - {round(ticker.performance.total_pnl, 3)}), "
                row = f"{count}. {current_spread.take_profit}/{current_spread.stop_loss} {top_tickers} {new_line}" 
                field_report.write(row)
                count += 1

            return Field(
                name=f"{current_interval.high_interval}/{current_interval.low_interval} (high/low)",
                value=field_report.getvalue()
            )

    def _embeds_from_analysis(self, analysis:LedgerAnalysis) -> list[Embed]:
        embeds = [ ]
        performance = PerformanceView(ledger_analysis=analysis)
        profits = performance.profits_by_category()
        author = main_author(db=database())
        fields = [ ]

        for interval_profit in profits[:min(len(profits), 5)]:
            field = self._field_from_analysis(interval_performance=interval_profit)
            fields.append(field)

        embeds.append(Embed(
            author=Author(
                name=author.name,
                icon_url=author.avatar_url
            ),
            title=f"By Interval",
            description=f"Profits by the leading 5 intervals (high / low)",
            color=colors.BLUE(),
            fields=fields,
            footer=Footer(
                text=f"End",
                icon_url=author.avatar_url
            ),
            thumbnail=Thumbnail(
                url=author.avatar_url
            )
        ))

        return embeds

    def report_to_ledger(self, positions:list[dict], ledger:Ledger, signer:Signer) -> None:
        if ledger is None:
            raise ValueError("ledger is None")
        if signer is None:
            raise ValueError("signer is None")
        if positions is None:
            raise ValueError("positions is None")
        for position in positions:
            order = Order.from_dict(position)
            
            ## ! TODO - only accounts for trailing stop strategy !
            amount = order.projections.loss_without_fees

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
        icon_stop_loss = "\U0001F6D1"
        icon_take_profit = "\U0001F3C6"
        icon_entry = "\U0001F4B5"
        
        fields = []
        for position in positions:
            order = Order.from_dict(position)
            current_price = prices.get(order.ticker)
            
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

            sell_now_profit_loss = round(multiply(current_price, main_contracts) - main_total, 7)
            sell_now_msg = f"LOSS of {sell_now_profit_loss}" if sell_now_profit_loss < 0 else f"PROFIT of {sell_now_profit_loss}"
            
            fields.append(Field(
                name=f"{order.ticker} - ({order.merchant_params.high_interval}/{order.merchant_params.low_interval})",
                value=f"PRICE @ {current_price} ({sell_now_per_diff}%) - {sell_now_msg}\n{icon_entry} @ {main_price} x {main_contracts} = {main_total}\n{icon_stop_loss} @ {stop_price} ({stop_price_per_diff}%) for {potential_loss}\n{icon_take_profit} @ {take_profit_price} ({take_profit_per_diff}%) for {potential_profit}"
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

        current_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
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