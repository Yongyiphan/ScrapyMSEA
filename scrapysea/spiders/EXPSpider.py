import scrapy
from scrapy.selector import Selector
from scrapysea.items import ExpItem
from tqdm import tqdm

import utils
from utils import logger as CL

DLog = CL.Create_Log("Exp Spider", logTo="EXP.log", propagate=False)


class ExpSpider(scrapy.Spider):
    ID = "EXP"
    Folder = "Stat"
    name = "ExpSpider"
    allowed_domains = utils.g.Allowed_Domains
    start_urls = [utils.g.getSourceLink(ID)]
    custom_settings = {
        "ITEM_PIPELINES": {
            "scrapysea.pipelines.ExportPipeline": 800,
        }
    }

    Crashed: bool = False

    def parse(self, response):
        Tables = response.css(".wikitable.prettytable")
        E_Bar = tqdm(total=0)
        for table in Tables:
            for e in self.formatTable(table, E_Bar):
                yield e

        # E_Bar.desc = "(C) Level Exp: "
        E_Bar.desc = "(E)" if self.Crashed else "(C)" + " Level Exp: "
        E_Bar.close()

    def formatTable(self, t: Selector, E_Bar: tqdm):
        CurrentHeader = (
            t.xpath(".//preceding-sibling::h2").css(".mw-headline ::text").getall()[-1]
        )
        DataPath = (
            utils.g.GetSet_LocalDB()
            + f"/{self.Folder}/{utils.RemoveDeli(CurrentHeader, {' ': ''})}.json"
        )
        trs = t.css("tr")[1:]
        E_Bar.total += len(trs)
        DLog.info(f"Total Entry: {E_Bar.total} | {CurrentHeader}: {len(trs)}")
        for e in trs:
            ExpEntry = ExpItem()
            row = e.css("td ::text").getall()
            row = utils.RemoveDeli(row, {"N/A": "0.0"})
            assert len(row) > 0
            E_Bar.desc = f"Level At: {row[0]}"
            try:
                ExpEntry["Category"] = CurrentHeader
                ExpEntry["Level"] = int(row[0])
                ExpEntry["TotalExp"] = row[1]
                ExpEntry["ExpGap"] = row[2]
                ExpEntry["Multiplier"] = row[3]
                ExpEntry["Path"] = DataPath
                E_Bar.desc = f"{CurrentHeader}({row[0]})"
                E_Bar.update(1)
                yield ExpEntry
            except IndexError:
                DLog.error(f"Index Error: {CurrentHeader}({row[0]})")
                self.Crashed = True
            except AssertionError:
                DLog.error(f"Nothing Scraped: {CurrentHeader}")
                self.Crashed = True

    ...
