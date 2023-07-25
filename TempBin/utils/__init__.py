import json

DATABASENAME = "MapleSeaDB"
MAXLVL = 300
MINLVL = 0
ROOT_DIR = None
LocalDB = None
REJSON = None


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


def Set_ProjectRoot(root):
    global ROOT_DIR
    ROOT_DIR = root


def Get_ProjectRoot():
    return ROOT_DIR


def Set_LocalDB(path):
    global LocalDB
    LocalDB = path


def Get_LocalDB():
    return LocalDB


def setDBPath(destination):
    global DBPATH
    DBPATH = destination


def setMseaModule(status):
    global MseaModule
    MseaModule = bool(status)


def getSourceLink(ID):
    if ID in Source_Links:
        return Source_Links[ID]
    else:
        raise Exception("Invalid Spider tag")


def LoadRenameJson():
    with open("./scrapysea/ReName.json", "r") as file:
        global REJSON
        REJSON = json.load(file)
