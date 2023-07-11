import os
import sys
import scrapysea.utils as utils
from pathlib import Path
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging


from scrapysea.spiders import CalculationsData, EquipmentData, CharacterData

import time


# logger = CustomLogger.Set_Custom_Logger(__name__, "./Logs/Base.log", propagate=False)

newsettings = {
    "LOG_ENABLED": True,
    "LOG_FORMATTER": utils.QuietLogFormatter,
    "LOG_LEVEL": "INFO",
    "LOG_FILE": "./Logs/BaseScrapy.log",  # Changed from None
    "LOG_FILE_APPEND": False,  # Changed from True
    "DUPEFILTER_DEBUG": True,
}


def exec_Crawler():
    # currentPath = Path(__file__).parent.parent.resolve().as_posix()
    # for dir in FolderDir:
    #    LogPath = os.path.join(currentPath,"DefaultData", dir)
    #    if not os.path.exists(LogPath):
    #        os.makedirs(LogPath)
    # DBPath = CF.APPFOLDER
    # DefaultPath = os.path.join(CF.APPFOLDER, "DefaultData\\")
    DBPath = os.path.join(utils.APPFOLDER, "Test")
    DefaultPath = os.path.join(utils.APPFOLDER, "Test\\DefaultData\\")
    if not os.path.exists(DefaultPath):
        os.makedirs(DefaultPath)
    FolderDir = ["CalculationData", "EquipmentData", "CharacterData"]
    for dir in FolderDir:
        # LogPath = os.path.join(utils.APPFOLDER,"DefaultData", dir)
        LogPath = os.path.join(utils.APPFOLDER, "Test", "DefaultData", dir)
        if not os.path.exists(LogPath):
            os.makedirs(LogPath)
    runner = CrawlerRunner(settings=sett)
    utils.APPFOLDER = DefaultPath
    utils.DBPATH = DBPath
    # runner.crawl(CharacterData.CharacterSpider)
    EquipmentSpiders = dict(
        [
            (name, cls)
            for name, cls in EquipmentData.__dict__.items()
            if isinstance(cls, type)
        ]
    )
    # for name, s in EquipmentSpiders.items():
    #    if utils.instring(name.lower(), "spider"):
    #        runner.crawl(s, rename = utils.REJSON)
    # CalculationSpiders = dict([(name, cls) for name, cls in CalculationsData.__dict__.items() if isinstance(cls, type)])
    # for name, s in CalculationSpiders.items():
    #    if utils.instring(name.lower(), "spider"):
    #        runner.crawl(s)

    # runner.crawl(CharacterData.CharacterSpider)
    runner.crawl(EquipmentData.TotalEquipmentSpider, rename=utils.REJSON)
    # runner.crawl(CalculationsData.StarforceSpider)

    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


if __name__ == "__main__":
    sett = get_project_settings()
    sett.update(newsettings)
    configure_logging()

    try:
        utils.setMseaModule(sys.argv[2])
    except:
        utils.setMseaModule(False)

    if utils.MseaModule:
        utils.setPath(sys.argv[1])
    else:
        utils.setPath(
            "C:\\Users\\edgar\\AppData\\Local\\Packages\\MseaCalculatorPackaged_h8rqv0gxgvjbt\\LocalState\\"
        )

    utils.LoadRenameJson()
    start = time.time()
    exec_Crawler()
    end = time.time()
    print("Scaped All in {0}".format(end - start))
