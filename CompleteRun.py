

from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapysea.scrapysea.spiders import EquipmentData


if __name__ == "__main__":
    runner = CrawlerRunner(settings=get_project_settings())
    runner.crawl(EquipmentData.TotalEquipmentSpider)
    runner.crawl(EquipmentData.EquipmentSetSpider)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()
