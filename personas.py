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
    ### original https://i.imgur.com/4I95kIa.png
    return Database(
        author=Persona(
            name="Flow Merchant Bot",
            avatar_url="https://i.imgur.com/xZpIYpS.png",
            portrait_url="https://i.imgur.com/xZpIYpS.png",
            quote="Follow the flow",
            advice="Follow the flow"
        ),
        winners=[
           Persona(
               name="Tifa",
               avatar_url="https://i.imgur.com/xUt6upj.png",
               portrait_url="https://i.imgur.com/xUt6upj.png",
               quote="You are a winner!",
               advice="Keep up the good work!"
           ),
           Persona(
               name="Obi-wan",
               avatar_url="https://i.imgur.com/76OLpT6.gif",
               portrait_url="https://i.imgur.com/76OLpT6.gif",
               quote="Another happy landing",
               advice="So uncivilized!"
           )
        ],
        leaders=[
            Persona(
                name="2B",
                avatar_url="https://i.imgur.com/Frj5QhZ.jpeg",
                portrait_url="https://i.imgur.com/Frj5QhZ.jpeg",
                quote="STAY ON TARGET",
                advice="Consider selling these - take what you can get."
            ),
            Persona(
                name="Gene",
                avatar_url="https://i.imgur.com/R5rqYTA.png",
                portrait_url="https://i.imgur.com/R5rqYTA.png",
                quote="A boy has a right to dream.",
                advice="Enjoy the show - or be smart and just sell the damn things."
            )
        ],
        laggards=[
            Persona(
                name="KOS-MOS",
                avatar_url="https://i.imgur.com/xnAyrOR.gif",
                portrait_url="https://i.imgur.com/dPtU0TQ.jpeg",
                quote="Target acquired.",
                advice="Get rid of these - or I will."
            ),
            Persona(
                name="Yor",
                avatar_url="https://i.imgur.com/EV9iNwo.jpeg",
                portrait_url="https://i.imgur.com/EV9iNwo.jpeg",
                quote="May I borrow some of your time?",
                advice="Sell these, or I'll get closer..."
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

def main_author(db:Database = database()) -> Persona:
    return db.author

def next_winner_persona(db:Database = database()) -> Persona:
    return _rand(db.winners)

def next_leader_persona(db:Database = database()) -> Persona:
    return _rand(db.leaders)

def next_laggard_persona(db:Database = database()) -> Persona:
    return _rand(db.laggards)

def next_loser_persona(db:Database = database()) -> Persona:
    return _rand(db.losers)
