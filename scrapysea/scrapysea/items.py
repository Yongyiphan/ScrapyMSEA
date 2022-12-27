# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapyseaItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


from itemloaders.processors import TakeFirst, MapCompose, Join
from scrapy.item import Item
from scrapy.loader import ItemLoader
from w3lib.html import remove_tags
from dataclasses import dataclass, field
from typing import Optional
def cEquipSlot(value):
    return value.strip('\n\t').split('/')
    ...
import ComFunc as CF

#CLEAN
def cCategory(value):
    return CF.replacen(value, ",", ";")

def removebr(list):
    s = list.strip('\n')
    if s != "":
        return s

def cNumber(value):
    if value.isdigit():
        return int(value)
    return value

def cEquipLevel(td):
    if "level" in td.lower():
        return int(td.split(" ")[1])
    elif "none" in td.lower():
        return 0

#OUTPUTS

def oEquipName(D):
    return CF.replacen(D[0], [',','<','>'])


class EquipArmorItem(Item):

    EquipSlot = scrapy.Field()
    EquipName = scrapy.Field(input_processor = MapCompose(removebr), output_processor = oEquipName)

    EquipSet: Optional[str] = field(default="None")
    EquipLevel = scrapy.Field()
    
    Category = scrapy.Field(default = "", input_processor = MapCompose(cCategory))

    def AddNField(self, fieldname):
        self.fields[fieldname] = scrapy.Field()
        ...

    ...

class EquipLoader(ItemLoader):
    default_output_processor = TakeFirst()
    default_input_processor = MapCompose(removebr, cNumber)
    EquipSlot_in = MapCompose(cEquipSlot)
    EquipLevel_in = MapCompose(cEquipLevel)
    EquipLevel_out = TakeFirst()
    default_item_class = EquipArmorItem
    def add_New(self, fieldname, value):
        try:
            if ("HP" in fieldname or "MP" in fieldname) and "%" in value:
                fieldname = "Perc " + fieldname
            value = CF.replacen(value, ["+", "%"]).strip()
        except:
            pass

        self.item.AddNField(fieldname)
        self.add_value(fieldname, value)
    

