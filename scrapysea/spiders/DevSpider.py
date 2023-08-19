import scrapy
import re
from scrapy.selector import Selector
from scrapysea.items import ScrollItem, EnhancementItemLoader
from tqdm import tqdm

import utils
from utils import logger as CL

DLog = CL.Create_Log("Dev Spider", logTo="Dev.log", propagate=False)


class ScrollTraceSpider(scrapy.Spider):
    ID = "Spell Trace"
    Folder = "Stat"
    name = "TraceSpider"
    start_urls = [utils.g.getSourceLink(ID)]
    custom_settings = {
        "ITEM_PIPELINES": {
            "scrapysea.pipelines.ExportPipeline": 800,
        }
    }

    def parse(self, response):
        self.DB_Loc = utils.g.GetSet_LocalDB() + "/" + self.Folder
        ST = response.xpath("//h2[./span[@id='Scrolling']]")[0]
        Tables = ST.xpath(
            "./following-sibling::dl[./dt[contains(text(), 'Stat Values')]]"
        )
        # Category = ST.css("span.mw-headline ::text").get()
        ST_Bar = tqdm(total=0)
        for t in Tables:
            for item in self.SpellTrace(t, "SpellTrace", ST_Bar):
                yield item
                ...
        ST_Bar.desc = "Spell Trace Stats: "
        UpgradeCostTable = response.xpath(
            "//h4[./span[@id='Upgrade_Cost']]/following-sibling::table[1]"
        )
        for uc in self.SpellTraceCost(UpgradeCostTable, "SpellTrace_Cost"):
            yield uc
        ...

    def SpellTraceCost(self, response, Category):
        header = response.xpath(".//tr//th/text()").getall()[1:]
        header = utils.RemoveDeli(header, {"\n": "", " cost": ""})
        DataPath = self.DB_Loc + "/SpellTraceCost.json"
        # total=len(response.xpath(".//tr")[1:]), desc="Spell Trace Costs: "
        UC_Bar = tqdm(total=0, desc="Spell Trace Costs: ")
        for tr in response.xpath(".//tr")[1:]:
            MinMaxL = tr.xpath(".//td[1]/text()").get().replace("\n", "").split("~")
            tds = tr.xpath(".//td")[1:]
            UC_Bar.total += len(tds)
            for n, td in enumerate(tds):
                UC_Item = {
                    "MinLvl": MinMaxL[0],
                    "MaxLvl": MinMaxL[1],
                    "SuccessRate": header[n],
                    "Path": DataPath,
                    "Category": Category,
                }
                for li in td.xpath(".//li"):
                    t = utils.RemoveDeli(li.css("::text").get().split(":"), {",": ""})
                    UC_Item[t[0]] = 0 if "?" in t[-1] else int(t[-1])
                    ...
                DLog.info(
                    f"INFO(C): Upgrade Cost ({header[n]}) at {MinMaxL[0]}~{MinMaxL[1]}"
                )
                UC_Bar.update(1)
                yield UC_Item
                ...
            ...
        UC_Bar.close()
        ...

    def SpellTrace(self, response, Category, ST_Bar: tqdm):
        stat_table = response.xpath("./following-sibling::table[1]")
        header = utils.RemoveDeli(stat_table.css("th ::text").getall())
        DataPath = self.DB_Loc + "/SpellTrace.json"
        ItemGroup = response.xpath(
            "./preceding-sibling::h5[1]/span[@class='mw-headline']/text()"
        ).get()
        MinMaxL = [
            tier.split("level")[-1].replace(")", "").strip() for tier in header[1:]
        ]
        for i, lr in enumerate(MinMaxL):
            MinL = utils.g.MINLVL
            MaxL = utils.g.MAXLVL
            if "~" in lr:
                MinMax = lr.split("~")
                MinL = int(MinMax[0])
                MaxL = int(MinMax[-1])
            elif "or" in lr:
                MinMax = lr.split("or")
                MinL = int(MinMax[0].strip())
            MinMaxL[i] = [MinL, MaxL]
        ST_Bar.desc = ItemGroup
        for tr in stat_table.xpath(".//tr"):
            Success = tr.xpath(".//td[1]/text()").get()
            tds = tr.xpath(".//td")[1:]
            ST_Bar.total += len(tds)
            for n, td in enumerate(tds):  # Every Item
                scrolli = EnhancementItemLoader(item=ScrollItem(), selector=td)
                scrolli.add_value("Category", Category)
                scrolli.add_value("Path", DataPath)
                scrolli.add_value("SuccessRate", Success)
                scrolli.add_value("MinLvl", MinMaxL[n][0])
                scrolli.add_value("MaxLvl", MinMaxL[n][-1])
                scrolli.add_value("ItemGroup", ItemGroup)
                self.CleanScrollItem(scrolli, td)

                DLog.info(
                    f"INFO(C): {Category}:{ItemGroup}({Success}) at {MinMaxL[n][0]}~{MinMaxL[n][-1]}"
                )
                ST_Bar.update(1)
                yield scrolli.load_item()
                ...

    def CleanScrollItem(self, loader: EnhancementItemLoader, selector: Selector):
        StatValueKey = {
            "MS_Stat": "Primary Stat",
            "MS_MaxHP": "HP stat",
            "MS_AS": "All Stat",
            "MaxHP": "Max HP",
            "DEF": "DEF",
            "Atk": "ATT",
            "Mtk": "ATT",
        }

        # Primary Stat
        # Primary Stat = HP
        # Primary Stat = All Stat
        # DEF
        # Atk
        # Mtk
        for k, v in StatValueKey.items():
            scrollvalue = selector.xpath(
                f".//li[contains(text(),'{v}')]/text()"
            ).getall()
            if scrollvalue == []:
                continue

            valueregex = r"[A-Za-z]+\s[A-Za-z]+\s+\+[0-9]+"
            if "MS" in k:
                # Stat entries
                StatValue = re.findall(valueregex, scrollvalue[0])
                position = 0
                if k == "MS_MaxHP" and len(StatValue) > 1:
                    position = 1
                elif k == "MS_AS" and len(StatValue) > 2:
                    position = 2
                loader.add_value(k, int(StatValue[position].split("+")[-1]))
                continue

            if "MaxHP" == k:
                value = scrollvalue[-1]
                if "stat" not in value.lower():
                    loader.add_value(k, int(value.split("+")[-1]))
                continue

            StatValue = [v for v in scrollvalue[0].split(" ") if "+" in v]
            loader.add_value(k, int(StatValue[0].split("+")[-1]))
            ...
        return

    ...
