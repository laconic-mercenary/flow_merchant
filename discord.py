import requests
import logging
import os
import datetime

from events import EventLoggable

def DISCORD_ENV_WEBHOOK_URL():
    return "DISCORD_WEBHOOK_URL"

def DISCORD_COLOR_GREEN():
    return 3066993

def DISCORD_COLOR_RED():
    return 15158332

def DISCORD_COLOR_BLUE():
    return 3447003

class DiscordClient(EventLoggable):
    def __init__(self, disabled=False):
        self.base_url = os.environ[DISCORD_ENV_WEBHOOK_URL()]
        self.__disabled = disabled

    def log_notice(self, title, message):
        self.send_message(title, message, DISCORD_COLOR_BLUE())

    def log_error(self, title, message):
        self.send_message(title, message, DISCORD_COLOR_RED())

    def log_success(self, title, message):
        self.send_message(title, message, DISCORD_COLOR_GREEN())

    def send_message(self, title, message, color=DISCORD_COLOR_BLUE()):
        if self.__disabled:
            return
        url = f"{self.base_url}"
        if not color in [DISCORD_COLOR_GREEN(), DISCORD_COLOR_RED(), DISCORD_COLOR_BLUE()]:
            color = DISCORD_COLOR_BLUE()
        if title is None or len(title) == 0:
            raise ValueError("title cannot be None")
        if message is None or len(message) == 0:
            raise ValueError("message cannot be None")
        payload = {
            "embeds": [
                {
                    "title": title,
                    "description": message,
                    "color": color,
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                }
            ]
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, json=payload, timeout=7)
        if response.status_code > 302:
            logging.error(f"Failed to send message: {response.text}")
