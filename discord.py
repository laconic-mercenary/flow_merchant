import requests
import logging
import os
import datetime

from events import EventLoggable

class cfg:
    @staticmethod
    def WEBHOOK_URL():
        url = os.environ["DISCORD_WEBHOOK_URL"]
        if url is None:
            raise ValueError("DISCORD_WEBHOOK_URL environment variable is not set")
        return url

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
        self._url = cfg.WEBHOOK_URL()

    def log_notice(self, title:str, message:str):
        self.send_message(title=title, message=message, color=colors.BLUE())

    def log_error(self, title:str, message:str):
        self.send_message(title=title, message=message, color=colors.RED())

    def log_success(self, title:str, message:str):
        self.send_message(title=title, message=message, color=colors.GREEN())

    def send_message(self, title:str, message:str, avatar:str = None, color:int=colors.BLUE()):
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
            payload["embeds"][0]["avatar_url"] = avatar

        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url=self._url, headers=headers, json=payload, timeout=7)
        if response.status_code > 302:
            raise ValueError(f"Discord returned status code {response.status_code} - {response.text}")
