# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.


from scrapy.crawler import CrawlerRunner

from scrapysea.spiders.EXPSpider import ExpSpider
from scrapysea.spiders.SpellTraceSpider import ScrollTraceSpider
from scrapysea.spiders.StarforceSpider import StarforceSpider


def RunSpellTrace(crawler: CrawlerRunner):
    # Runnable (22/08/2023)
    crawler.crawl(ScrollTraceSpider)
    ...


def RunEXP(crawler: CrawlerRunner):
    # Runnable (22/08/2023)
    crawler.crawl(ExpSpider)
    ...


def RunDev(crawler: CrawlerRunner):
    crawler.crawl(StarforceSpider)
