import os

ROOT_DIR = os.getcwd()
DB_DIR = None
MAXLVL = 300
MINLVL = 1

Source_Links = {
    "EXP": "https://strategywiki.org/wiki/MapleStory/EXP_and_Pet_Closeness",
    "Starforce": "https://strategywiki.org/wiki/MapleStory/Spell_Trace_and_Star_Force",
    "Spell Trace": "https://strategywiki.org/wiki/MapleStory/Spell_Trace_and_Star_Force",
    "Flames": "https://strategywiki.org/wiki/MapleStory/Bonus_Stats",
    "Maple Union": "https://strategywiki.org/wiki/MapleStory/Maple_Union",
    "Hyper Stat": "https://strategywiki.org/wiki/MapleStory/Hyper_Stats",
    "Monster Life": "https://strategywiki.org/wiki/MapleStory/Monster_Life",
    "Potential": "https://strategywiki.org/wiki/MapleStory/Potential_System",
    "Armor": "https://maplestory.fandom.com/wiki/Equipment",
    "Weapon": "https://maplestory.fandom.com/wiki/Equipment",
    "Secondary": "https://maplestory.fandom.com/wiki/Equipment",
}

Allowed_Domains = [
    "strategywiki.org/wiki",
    "maplestory.fandom.com/wiki",
]

Data_Categories = ["Stat", "Equips", "Content"]


def GetSet_ProjectRoot(path=None):
    global ROOT_DIR
    if path is None:
        return ROOT_DIR
    else:
        ROOT_DIR = path


def GetSet_LocalDB(path=None):
    global LOCAL_DB
    if path is None:
        return LOCAL_DB
    else:
        LOCAL_DB = path


def GetSet_DB_DIR(path=None):
    global DB_DIR
    if path is None:
        return DB_DIR
    else:
        DB_DIR = path
    ...


def getSourceLink(ID):
    if ID in Source_Links:
        return Source_Links[ID]
    else:
        raise Exception("Invalid Spider tag")
