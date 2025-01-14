from persona import Persona

import random

class Database(dict):
    def __init__(self, author:Persona, winners:list[Persona], leaders:list[Persona], laggards:list[Persona], losers:list[Persona]):
        super().__init__(author=author, winners=winners, leaders=leaders, laggards=laggards, losers=losers)
        self.winners = winners
        self.leaders = leaders
        self.laggards = laggards
        self.losers = losers
        self.author = author

def database() -> Database:
    return Database(
        author=Persona(
            name="Flow Merchant Bot",
            avatar_url="https://i.imgur.com/4I95kIa.png",
            portrait_url="https://i.imgur.com/4I95kIa.png",
            quote="I am the Flow Merchant Bot",
            advice="I am here to help you make money"
        ),
        winners=[
           Persona(
               name="Tifa",
               avatar_url="https://i.imgur.com/xUt6upj.png",
               portrait_url="https://i.imgur.com/dMbTucn.jpeg",
               quote="You are a winner!",
               advice="Keep up the good work!"
           ) 
        ],
        leaders=[
            Persona(
                name="2B",
                avatar_url="https://i.imgur.com/9dVBduL.jpeg",
                portrait_url="https://i.imgur.com/lRDT0CI.jpeg",
                quote="STAY ON TARGET",
                advice="Consider selling these - take what you can get."
            )
        ],
        laggards=[
            Persona(
                name="KOS-MOS",
                avatar_url="https://i.imgur.com/xnAyrOR.gif",
                portrait_url="https://i.imgur.com/trBuZiN.png",
                quote="Target acquired.",
                advice="Get rid of these - or I will."
            )
        ],
        losers=[
            Persona(
                name="Indy Jones",
                avatar_url="https://i.imgur.com/N0fjihY.jpeg",
                portrait_url="https://i.imgur.com/N0fjihY.jpeg",
                quote="Never even got off the ground, kid.",
                advice="Get up, dust yourself off, and try again."
            )
        ]
    )

def _rand(collection:list[Persona]) -> Persona:
    if len(collection) == 0:
        raise ValueError("Collection is empty")
    min = 0
    max = len(collection) - 1
    index = random.randint(a=min, b=max)
    return collection[index]

def main_author(db:Database) -> Persona:
    return db.author

def next_winner_persona(db:Database) -> Persona:
    return _rand(db.winners)

def next_leader_persona(db:Database) -> Persona:
    return _rand(db.leaders)

def next_laggard_persona(db:Database) -> Persona:
    return _rand(db.laggards)

def next_loser_persona(db:Database) -> Persona:
    return _rand(db.losers)
