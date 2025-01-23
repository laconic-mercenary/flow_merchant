from discord import DiscordClient, WebhookMessage, Thumbnail, Author, Footer, Field, Embed, colors
from merchant_keys import keys as mkeys
from merchant_signal import MerchantSignal
from persona import Persona
from personas import database, main_author, next_laggard_persona, next_leader_persona, next_loser_persona, next_winner_persona
from transactions import multiply, calculate_percent_diff

import datetime
import logging
import io
import traceback

class MerchantReporting:

    def report_problem(self, msg:str, exc:Exception) -> None:
        persona_db = database()
        author = main_author(db=persona_db)
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
        persona_db = database()
        author = main_author(db=persona_db)
        DiscordClient().send_webhook_message(msg=WebhookMessage(
            username=author.name,
            avatar_url=author.avatar_url,
            content=msg,
            embeds=[]
        ))

    def report_order_placed(self, order:dict) -> None:
        if not mkeys.bkrdata.order.METADATA() in order:
            raise ValueError("order is missing metadata")
        if not mkeys.bkrdata.order.SUBORDERS() in order:
            raise ValueError("order is missing suborders")
        if not mkeys.bkrdata.order.PROJECTIONS() in order:
            raise ValueError("order is missing projections")
        
        ticker = order.get(mkeys.bkrdata.order.TICKER())
        metadata = order.get(mkeys.bkrdata.order.METADATA())
        suborders = order.get(mkeys.bkrdata.order.SUBORDERS())
        projections = order.get(mkeys.bkrdata.order.PROJECTIONS())

        order_dry_run = metadata.get(mkeys.bkrdata.order.metadata.DRY_RUN())
        main_order = suborders.get(mkeys.bkrdata.order.suborders.MAIN_ORDER())
        stop_loss_order = suborders.get(mkeys.bkrdata.order.suborders.STOP_LOSS())
        take_profit_order = suborders.get(mkeys.bkrdata.order.suborders.TAKE_PROFIT())

        main_price = main_order.get(mkeys.bkrdata.order.suborders.props.PRICE())
        main_contracts = main_order.get(mkeys.bkrdata.order.suborders.props.CONTRACTS())
        main_id = main_order.get(mkeys.bkrdata.order.suborders.props.ID())
        stop_price = stop_loss_order.get(mkeys.bkrdata.order.suborders.props.PRICE())
        take_profit_price = take_profit_order.get(mkeys.bkrdata.order.suborders.props.PRICE())

        main_total = multiply(main_price, main_contracts)

        main_price = round(main_price, 8)
        main_total = round(main_total, 8)
        stop_price = round(stop_price, 8)
        take_profit_price = round(take_profit_price, 8)

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
                            value=f"{stop_price} ({calculate_percent_diff(main_price, stop_price) * 100}%)"
                        ),
                        Field(
                            name="Take profit price",
                            value=f"{take_profit_price} ({calculate_percent_diff(take_profit_price, main_price) * 100}%)"
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
    
    def _embed(self, author:str, author_icon:str, title:str, desc:str, color:int, footer:str, footer_icon:str, thumbnail:str, positions:list, prices:dict) -> Embed:
        icon_stop_loss = "\U0001F6D1"
        icon_take_profit = "\U0001F3C6"
        icon_entry = "\U0001F4B5"
        
        fields = []
        for position in positions:
            if "orders" not in position: 
                raise ValueError(f"expected key orders in position {position}")
            if "projections" not in position:
                raise ValueError(f"expected key projections in order {position}")
            if "ticker" not in position:
                raise ValueError(f"expected key ticker in order {position}")

            ticker = position.get("ticker")
            sub_orders = position.get("orders")
            projections = position.get("projections")
            current_price = prices.get(ticker)

            potential_profit = projections.get("profit_without_fees")
            potential_loss = projections.get("loss_without_fees")

            main_order = sub_orders.get("main")
            stop_loss_order = sub_orders.get("stop_loss")
            take_profit_order = sub_orders.get("take_profit")
            
            main_price = main_order.get("price")
            main_contracts = main_order.get("contracts")

            stop_price = stop_loss_order.get("price")

            take_profit_price = take_profit_order.get("price")

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
                name=f"{ticker}",
                value=f"{icon_entry} @ {main_price} x {main_contracts} = {main_total}\n{icon_stop_loss} @ {stop_price} ({stop_price_per_diff}%) for {potential_loss}\n{icon_take_profit} @ {take_profit_price} ({take_profit_per_diff}%) for {potential_profit}\nCURRENT PRICE @ {current_price} ({sell_now_per_diff}%), selling now would be a {sell_now_msg}"
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
        
        db = database()
        author = main_author(db=db)
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