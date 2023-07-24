import scrapy

import utils
from utils import logger as CL

DLog = CL.Create_Log("Dev Spider", logTo="Dev.log", propagate=False)


class ScrollTraceSpider(scrapy.Spider):
    ID = "Spell Trace"
    Folder = "Stat"
    name = "TraceSpider"
    start_urls = [utils.g.getSourceLink(ID)]

    def parse(self, response):
        ST = response.xpath("//h2[./span[contains(@class,'mw-headline')]]")[0]
        Tables = ST.xpath(
            "./following-sibling::dl[./dt[contains(text(), 'Stat Values')]]"
        )
        for t in Tables:
            self.SpellTrace(t)
        ...

    def SpellTrace(self, response):
        stat_table = response.xpath("./following-sibling::table[1]")
        print("here")
        header = stat_table.css("th ::text").getall()
        trs = utils.H.RemoveString(stat_table.css("td ::text").getall())
        print(trs[0])
        ...
