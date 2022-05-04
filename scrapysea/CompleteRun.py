
import imp
import CustomLogger
import QuietLogFormatter
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from scrapysea.spiders import EquipmentData
from scrapy.settings import Settings


logger = CustomLogger.Set_Custom_Logger(__name__, "./Logs/Base.log", propagate=False) 

newsettings = {
    "LOG_ENABLED" : True,
    "LOG_FORMATTER" : QuietLogFormatter.QuietLogFormatter,
    "LOG_LEVEL" : 'DEBUG',
    "LOG_FILE" : "./Logs/BaseScrapy.log", #Changed from None
    "LOG_FILE_APPEND" : False #Changed from True
}

if __name__ == "__main__":
    sett  = get_project_settings()
    sett.update(newsettings)
    configure_logging()

    runner = CrawlerRunner(settings=sett)
    runner.crawl(EquipmentData.TotalEquipmentSpider)
    runner.crawl(EquipmentData.EquipmentSetSpider)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()