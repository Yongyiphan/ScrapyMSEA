
import CustomLogger
import QuietLogFormatter
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from ComFunc import *

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
    runner = CrawlerRunner(settings=sett)
    runner.crawl(CharacterData.CharacterSpider)
    #EquipmentSpiders = dict([(name, cls) for name, cls in EquipmentData.__dict__.items() if isinstance(cls, type)])
    #for name, s in EquipmentSpiders.items():
    #    if if_In_String(name.lower(), "dataframe"):
    #        continue
    #    runner.crawl(s)

    #CalculationSpiders = dict([(name, cls) for name, cls in CalculationsData.__dict__.items() if isinstance(cls, type)])
    #for name, s in CalculationSpiders.items():
    #    if if_In_String(name.lower(), "dataframe"):
    #        continue
    #    runner.crawl(s)

    #runner.crawl(CalculationsData.BonusStatSpider)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

if __name__ == "__main__":
    sett  = get_project_settings()
    sett.update(newsettings)
    configure_logging()

    start = time.time()
    
    exec_Crawler()
    #ComFunc.main()
    end = time.time()
    print(f"Scaped All in {end - start}")