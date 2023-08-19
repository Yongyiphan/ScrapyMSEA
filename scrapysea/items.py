# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst
import utils


class BaseItem(Item):
    Category = Field()
    Path = Field()


def defaultZero(value):
    if value is None:
        return 0
    else:
        return value


def CleanDelimiters(value):
    D = ["N/A", "\n"]
    for d in D:
        if d in value:
            value = value.replace(d, "")
    return value
    ...


class ExpItem(BaseItem):
    Level = Field()
    TotalExp = Field()
    ExpGap = Field()
    Multiplier = Field()


class ScrollItem(BaseItem):
    SuccessRate = Field()
    ItemGroup = Field()
    # MS_Stat = Field(input_processor=MapCompose(GetScrollValue))
    MS_Stat = Field()
    MS_MaxHP = Field()
    MS_AS = Field()
    MaxHP = Field()
    DEF = Field()
    Atk = Field()
    Mtk = Field()
    MinLvl = Field()
    MaxLvl = Field()


class StarforceItem(BaseItem):
    JobStat = Field()
    NonWeapVDef = Field()
    OverallVDef = Field()
    MaxHP = Field()
    WeapMP = Field()
    WeapAtk = Field()
    WeapMtk = Field()
    Speed = Field()
    Jump = Field()
    GloveAtk = Field()
    GloveMtk = Field()
    LevelScope = Field()


class EnhancementItemLoader(ItemLoader):
    default_input_processor = MapCompose(defaultZero)
    default_output_processor = TakeFirst()
    ...
