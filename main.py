# Entry Point of Code

import globals as g
import os
import time

from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings

# import Spiders
from scrapysea.spiders import DevSpider
import scrapysea.spiders as spiders


def Initialise_DB():
    Root = g.GetSet_ProjectRoot()
    DataTempPath = os.path.join(Root, "TempData")
    g.GetSet_LocalDB(DataTempPath)
    if not os.path.exists(DataTempPath):
        os.mkdir(DataTempPath)
        print("Created Temp Data Folder")

    for cat in g.Data_Categories:
        tPath = os.path.join(DataTempPath, cat)
        if not os.path.exists(tPath):
            print(f"Created {cat} Folder")
            os.mkdir(tPath)
        ...

    ...


def CrawlAll(crawler: CrawlerRunner):
    # StatSpiders = {
    #     name: cls for name, cls in DevSpider.__dict__.items() if isinstance(cls, type)
    # }
    # for name, s in StatSpiders.items():
    #     print(name)
    #     if "spider" in name.lower():
    #         crawler.crawl(s)
    # spiders.RunEXP(crawler)
    # spiders.RunSpellTrace(crawler)

    spiders.RunDev(crawler)
    ...


def Run():
    Initialise_DB()
    start = time.time()
    settings = get_project_settings()
    runner = CrawlerRunner(settings)
    CrawlAll(runner)

    runner.join().addBoth(lambda _: reactor.stop())
    reactor.run()

    end = time.time()
    print(f"Process complete in {end - start}")
    ...


if __name__ == "__main__":
    Run()
