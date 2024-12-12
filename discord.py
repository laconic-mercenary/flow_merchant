import requests
import logging
import os
import datetime

from events import EventLoggable

class cfg:
    
    @staticmethod
    def WEBHOOK_URL():
        return os.environ["DISCORD_WEBHOOK_URL"]

class colors:

    @staticmethod
    def BLUE():
        return 3447003
    
    @staticmethod
    def RED():
        return 15158332

    @staticmethod
    def GREEN():
        return 3066993

class DiscordClient(EventLoggable):
    def __init__(self):
        self.base_url = os.environ[cfg.WEBHOOK_URL()]

    def log_notice(self, title:str, message:str):
        self.send_message(title, message, colors.BLUE())

    def log_error(self, title:str, message:str):
        self.send_message(title, message, colors.RED())

    def log_success(self, title:str, message:str):
        self.send_message(title, message, colors.GREEN())

    def send_message(self, title:str, message:str, avatar:str = None, color:int=colors.BLUE()):
        url = f"{self.base_url}"
        if not color in [colors.GREEN(), colors.RED(), colors.BLUE()]:
            color = colors.BLUE()
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
        if avatar is not None:
            payload["embeds"]["avatar_url"] = avatar

        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url=url, headers=headers, json=payload, timeout=7)
        if response.status_code > 302:
            raise ValueError(f"Discord returned status code {response.status_code} - {response.text}")
