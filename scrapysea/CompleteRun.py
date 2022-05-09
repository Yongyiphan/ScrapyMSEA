
import CustomLogger
import QuietLogFormatter
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from scrapysea.spiders import EquipmentData
from scrapysea.spiders import CharacterData
import ComFunc


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
    runner.crawl(EquipmentData.TotalEquipmentSpider)
    #runner.crawl(EquipmentData.EquipmentSetSpider)
    #runner.crawl(CharacterData.CharacterSpider)
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