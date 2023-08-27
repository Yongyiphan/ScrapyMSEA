import scrapy
import re
from scrapy.http.response import text
from scrapy.selector import Selector
from globals import MAXLVL
from scrapysea.items import (
    EnhancementItemLoader,
    ItemStatBoost,
    ItemSuccessRate,
    ItemStarLimit,
)
from tqdm import tqdm

import utils
from utils import logger as CL

DLog = CL.Create_Log("Starforce Spider", logTo="Starforce.log", propagate=False)


class StarforceSpider(scrapy.Spider):
    ID = "Starforce"
    Folder = "Stat"
    name = "SFSpider"
    allowed_domains = utils.g.Allowed_Domains
    start_urls = [utils.g.getSourceLink(ID)]
    custom_settings = {
        "ITEM_PIPELINES": {
            "scrapysea.pipelines.ExportPipeline": 800,
        }
    }

    Crashed: bool = False
    RangeDeli = "→"
    StarDeli = "★"

    # tag = "_2" will be used to identify scraping superior items
    def parse(self, response: Selector):
        self.DataPath = utils.g.GetSet_LocalDB() + f"/{self.Folder}/"
        ItemTag = ["Normal_Equip", "Superior_Items"]
        Nor_Equip = response.xpath(f"//h3[./span[@id='{ItemTag[0]}']]")
        for ne in self.GetStarforce(Nor_Equip):
            yield ne
        Sup_Equip = response.xpath(f"//h3[./span[@id='{ItemTag[1]}']]")
        ...

    def GetStarforce(self, response: Selector, tag: str = ""):
        S_L = self.GetTable(response, "h4", "Star_Limit" + tag)
        S_R = self.GetTable(response, "h4", "Success_Rates" + tag)
        # M_C = self.GetTable(response, "h4", "Meso_Cost" + tag)
        DLog.info("(I) Meso Cost requires Manual Scrape")
        S_B = self.GetTable(response, "h4", "Stats_Boost" + tag)
        T_S = self.GetTable(response, "h5", "Total_Stats" + tag)
        for item in self.StarLimit(S_L):
            yield item

        for item in self.Success_Rates(S_R):
            yield item

        for item in self.Stat_Boosts(S_B):
            yield item
        ...

    def Stat_Boosts(self, response: Selector, tag: str = ""):
        # Stat at Individual Star
        # Stat at Star
        Category = "SF_Stat"
        OutPath = self.DataPath + f"{Category}.json"

        for tr in tqdm(response.xpath("./tbody/tr")[1:], desc="Starforce Stat: "):
            row = tr.xpath("./td/text()").get()
            Attempt = tr.xpath("./td[1]/text()").get().split(",")
            for Star in Attempt:
                IL = EnhancementItemLoader(
                    item=ItemStatBoost(), selector=tr.xpath("./td[2]")
                )
                Stats = tr.xpath("./td[2]/ul/li/text()").getall()
                SubTable = tr.xpath("./td[2]//table")
                if SubTable != []:
                    print(SubTable)
                IL.add_value("Star", int(re.findall(r"[0-9]+", Star)[0]))
                StatKeyVP = {
                    "Stat": "Stat",
                    "VDEF": "Visible DEF",
                    "AMHP": "Max HP",
                    "WMMP": "Max MP",
                    "WATK": "Visible ATT",
                    "SSpd": "Speed",
                    "SJmp": "Jump",
                    "GATK": "Gloves",
                }
                IL.get_value(".//li[text()]")
                # Stat   # Stat/ Visible Stat
                # VDEF   # Overalls = this * 2
                # AMHP   # Cat A's Max HP
                # WMMP   # Weapon's Max HP
                # WATK   # Weapon's Atk
                # SSpd   # Shoe Speed
                # SJmp   # Shoe Jump
                # GATK   # Glove's Atk
                # MinL
                # MaxL
                ...
            print(Attempt)
            ...
        ...

    # Completed
    def StarLimit(self, response: Selector, tag: str = ""):
        Category = "SF_Limit"
        OutPath = self.DataPath + f"{Category}.json"
        if tag == "":
            for tr in tqdm(response.xpath(".//tr")[1:], desc="Starforce Limit: "):
                texts = utils.RemoveDeli(tr.xpath(".//td/text()").getall())
                Item = ItemStarLimit()
                MMLvl = texts[0].split("~")
                if "and" in texts[0]:
                    MMLvl = texts[0].split("and")
                    MMLvl[1] = MAXLVL
                Item["MinLvl"] = int(MMLvl[0])
                Item["MaxLvl"] = int(MMLvl[1])
                Item["Stars"] = int(texts[1])
                Item["Category"] = Category
                Item["Path"] = OutPath
                DLog.info(f"(C): Star Limit at {MMLvl[0]}~{MMLvl[1]}")
                yield Item

        elif tag == "_2":
            ...
        ...

        DLog.info("Scraped (C): Star Limit")

    # Completed
    def Success_Rates(self, response: Selector, tag: str = ""):
        Category = "SuccessRates"
        OutPath = self.DataPath + "SF_SuccessRates.json"
        for tr in tqdm(response.xpath(".//tr")[1:], desc="Starforce Success Rates: "):
            row = utils.RemoveDeli(tr.xpath(".//td/text()").getall())
            item = ItemSuccessRate()
            item["Category"] = Category
            item["Path"] = OutPath
            item["Star"] = row[0].split(self.RangeDeli)[0].strip().strip(self.StarDeli)
            item["Success"] = row[1]
            item["Maintain"] = row[2]
            if len(row) == 5:
                item["Decrease"] = row[3]
                item["Destroy"] = row[4]

            DLog.info(f"(C): Success Rate at {item['Star']}")
            yield item
            ...

        DLog.info("Scraped (C): Star Success Rates")
        ...

    def GetTable(self, response: Selector, tag: str, id: str):
        T = response.xpath(
            f"./following-sibling::{tag}[./span[@id='{id}']]"
            + "/following-sibling::table[1]"
        )
        return T

    ...
