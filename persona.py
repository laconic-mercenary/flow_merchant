
class Persona(dict):
    def __init__(self, name:str, avatar_url:str, portrait_url:str, quote:str, advice:str):
        super().__init__(name=name, avatar_url=avatar_url, portrait_url=portrait_url, quote=quote, advice=advice)
        self.name = name
        self.avatar_url = avatar_url
        self.portrait_url = portrait_url
        self.quote = quote
        self.advice = advice

