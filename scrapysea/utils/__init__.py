from CustomLogger import *
from helper import *
import json

DATABASENAME = "MapleSeaDB"
MAXLVL = 300
MINLVL = 0

Source_Links = {
    "Potential": "https://strategywiki.org/wiki/MapleStory/Potential_System"
}


def setPath(destination):
    global APPFOLDER
    APPFOLDER = destination


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
