import requests
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
    
    @staticmethod
    def YELLOW():
        return 16705372
    
    @staticmethod
    def LIGHT_BLUE():
        return 1752220
    
class Author(dict):
    def __init__(self, name:str, icon_url:str):
        super().__init__(name=name, icon_url=icon_url)
        self.name = name
        self.icon_url = icon_url
    def __str__(self):
        return super().__str__()

class Footer(dict):
    def __init__(self, text:str, icon_url:str):
        super().__init__(text=text, icon_url=icon_url)
        self.text = text
        self.icon_url = icon_url
    def __str__(self):
        return super().__str__()

class Thumbnail(dict):
    def __init__(self, url:str):
        super().__init__(url=url)
        self.url = url
    def __str__(self):
        return super().__str__()

class Field(dict):
    def __init__(self, name:str, value:str):
        super().__init__(name=name, value=value)
        self.name = name
        self.value = value
    def __str__(self):
        return super().__str__()

class Embed(dict):
    def __init__(self, author:Author, title:str, description:str, color:int, footer:Footer, thumbnail:Thumbnail, fields:list[Field]):
        super().__init__(author=author, title=title, description=description, color=color, footer=footer, thumbnail=thumbnail, fields=fields)
        self.author = author
        self.title = title
        self.description = description
        self.color = color
        self.footer = footer
        self.thumbnail = thumbnail
        self.fields = fields
    def __str__(self):
        return super().__str__()

class WebhookMessage(dict):
    def __init__(self, username:str, avatar_url:str, content:str, embeds:list[Embed]):
        super().__init__(username=username, avatar_url=avatar_url, content=content, embeds=embeds)
        self.username = username
        self.avatar_url = avatar_url
        self.content = content
        self.embeds = embeds
    def __str__(self):
        return super().__str__()

class DiscordClient(EventLoggable):
    def __init__(self):
        self._url = cfg.WEBHOOK_URL()

    def log_success(self, title, message):
        self.send_message(title=title, message=message, color=colors.GREEN())

    def log_notice(self, title:str, message:str):
        self.send_message(title=title, message=message, color=colors.BLUE())

    def log_error(self, title:str, message:str):
        self.send_message(title=title, message=message, color=colors.RED())

    def send_webhook_message(self, msg:WebhookMessage) -> None:
        payload = msg.__dict__
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url=self._url, headers=headers, json=payload, timeout=10)
        if response.status_code > 302:
            raise ValueError(f"Discord returned status code {response.status_code} - {response.text}. Payload was {payload}")

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
            raise ValueError(f"Discord returned status code {response.status_code} - {response.text}. Payload was {payload}")
