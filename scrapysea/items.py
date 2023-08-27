# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst
from globals import MINLVL
import utils


class BaseItem(Item):
    Category = Field()
    Path = Field()


# General Cleaning Methods
def defaultZero(value):
    if value is None:
        return 0
    else:
        return value


# Stat Folders


# Exp Spider
class ExpItem(BaseItem):
    Level = Field()
    TotalExp = Field()
    ExpGap = Field()
    Multiplier = Field()


# Spell Trace Spider
class ScrollItem(BaseItem):
    SuccessRate = Field()
    ItemGroup = Field()
    MS_Stat = Field()
    MS_MaxHP = Field()
    MS_AS = Field()
    MaxHP = Field()
    DEF = Field()
    Atk = Field()
    Mtk = Field()
    MinLvl = Field()
    MaxLvl = Field()


# Star Force Spider Start


class ItemStarLimit(BaseItem):
    MinLvl = Field()
    MaxLvl = Field()
    Stars = Field()


class ItemSuccessRate(BaseItem):
    Star = Field()
    Success = Field()
    Maintain = Field()
    Decrease = Field()
    Destroy = Field()
    ...


class ItemStatBoost(BaseItem):
    Star = Field()
    Stat = Field()  # Stat/ Visible Stat
    VDEF = Field()  # Overalls = this * 2
    AMHP = Field()  # Cat A's Max HP
    WMMP = Field()  # Weapon's Max HP
    WATK = Field()  # Weapon's Atk
    SSpd = Field()  # Shoe Speed
    SJmp = Field()  # Shoe Jump
    GATK = Field()  # Glove's Atk
    MinL = Field()
    MaxL = Field()


# Star Force Spider End


class EnhancementItemLoader(ItemLoader):
    default_input_processor = MapCompose(defaultZero)
    default_output_processor = TakeFirst()
    ...


# Content Folder
# Equip Folder
