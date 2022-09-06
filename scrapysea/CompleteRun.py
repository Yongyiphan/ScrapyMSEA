
from modulefinder import Module
import CustomLogger
import os
import sys
import ComFunc as CF
from pathlib import Path
import QuietLogFormatter
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from TestProgressBar import ProgressBar



from scrapysea.spiders import CalculationsData
from scrapysea.spiders import EquipmentData
from scrapysea.spiders import CharacterData

import time


#logger = CustomLogger.Set_Custom_Logger(__name__, "./Logs/Base.log", propagate=False) 

newsettings = {
    "LOG_ENABLED" : True,
    "LOG_FORMATTER" : QuietLogFormatter.QuietLogFormatter,
    "LOG_LEVEL" : 'INFO',
    "LOG_FILE" : "./Logs/BaseScrapy.log", #Changed from None
    "LOG_FILE_APPEND" : False, #Changed from True
    "DUPEFILTER_DEBUG" : True
}

def exec_Crawler():
    #currentPath = Path(__file__).parent.parent.resolve().as_posix()
    #for dir in FolderDir:
    #    LogPath = os.path.join(currentPath,"DefaultData", dir)
    #    if not os.path.exists(LogPath):
    #        os.makedirs(LogPath)
    DefaultPath = os.path.join(CF.APPFOLDER, "DefaultData\\") 
    if not os.path.exists(DefaultPath):
        os.makedirs(DefaultPath)
    FolderDir = ["CalculationData","EquipmentData", "CharacterData"]
    for dir in FolderDir:
        LogPath = os.path.join(CF.APPFOLDER,"DefaultData", dir)
        if not os.path.exists(LogPath):
            os.makedirs(LogPath)
    runner = CrawlerRunner(settings=sett)
    CF.APPFOLDER = DefaultPath
 
    #runner.crawl(CharacterData.CharacterSpider)
    #EquipmentSpiders = dict([(name, cls) for name, cls in EquipmentData.__dict__.items() if isinstance(cls, type)])
    #for name, s in EquipmentSpiders.items():
    #    if CF.if_In_String(name.lower(), "spider"):
    #        runner.crawl(s)
    #CalculationSpiders = dict([(name, cls) for name, cls in CalculationsData.__dict__.items() if isinstance(cls, type)])
    #for name, s in CalculationSpiders.items():
    #    if CF.if_In_String(name.lower(), "spider"):
    #        runner.crawl(s)
    

    #runner.crawl(CharacterData.CharacterSpider)
    #runner.crawl(EquipmentData.TotalEquipmentSpider)
    runner.crawl(CalculationsData.TestSpider)
    
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

if __name__ == "__main__":
    sett  = get_project_settings()
    sett.update(newsettings)
    configure_logging()

    
    try:
        CF.setMseaModule(sys.argv[2])
    except:
        CF.setMseaModule(False)



    if(CF.MseaModule):
        CF.setPath(sys.argv[1])
    else:  
        CF.setPath("C:\\Users\\edgar\\AppData\\Local\\Packages\\MseaCalculatorPackaged_h8rqv0gxgvjbt\\LocalState\\")
    

    start = time.time()
    exec_Crawler()
    #ComFunc.main()
    #ProgressBar(50)
    end = time.time()
    print("Scaped All in {0}".format(end - start))