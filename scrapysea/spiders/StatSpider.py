import scrapy
from scrapysea.items import ExpItem
import utils
import scrapysea.pipelines as pl


class ExpSpider(scrapy.Spider):
    ID = "EXP"
    Folder = "Stat"
    name = "ExpSpider"
    allowed_domains = utils.g.Allowed_Domains
    start_urls = [utils.g.getSourceLink(ID)]
    custom_settings = {
        "ITEM_PIPELINES": {
            "msspider.pipelines.ExpPipeline": 200,
            "msspider.pipelines.ExportPipeline": 800,
        }
    }

    def parse(self, response):
        Tables = response.css(".wikitable.prettytable")
        for i, table in enumerate(Tables):
            for e in self.formatTable(table):
                yield e

    def formatTable(self, t):
        CurrentHeader = (
            t.xpath(".//preceding-sibling::h2").css(".mw-headline ::text").getall()[-1]
        )
        for e in t.css("tr")[1:]:
            ExpEntry = ExpItem()
            row = e.css("td ::text").getall()
            ExpEntry["Category"] = CurrentHeader
            ExpEntry["Level"] = row[0]
            ExpEntry["TotalExp"] = row[1]
            ExpEntry["ExpGap"] = row[2]
            ExpEntry["Multiplier"] = row[3]
            ExpEntry["Path"] = (
                "./TempData/"
                + self.Folder
                + "/"
                + utils.RemoveString(CurrentHeader, " ")
                + ".json"
            )
            yield ExpEntry

    ...


class StarforceSpider(scrapy.Spider):
    ID = "Starforce"
    Folder = "Stat"
    name = "SFSpider"

    start_urls = [utils.g.getSourceLink(ID)]

    def parse(self, response):
        print(response)
        ...

    ...
