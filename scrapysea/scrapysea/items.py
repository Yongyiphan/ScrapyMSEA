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
def cClassType(D):
    for k , v in CF.REJSON["ClassTypes"].items():
        if D in v:
            return k
    return D

class CustomItem(Item):
    Destination = scrapy.Field()
    def AddNField(self, fieldname):
        self.fields[fieldname] = scrapy.Field()

#EQUIPMENT STATS
class EquipItem(CustomItem):
    PrimaryKeys = ["EquipSlot", "EquipName", "EquipSet", "ClassType"]

    EquipSlot  = scrapy.Field()
    EquipName  = scrapy.Field(input_processor = MapCompose(removebr), output_processor = oEquipName)
    EquipLevel = scrapy.Field()
    EquipSet   = scrapy.Field()
    ClassType  = scrapy.Field(input_processor = MapCompose(cClassType))
    Category   = scrapy.Field(default = "", input_processor = MapCompose(cCategory))

    ...

class WeaponItem(EquipItem):
    def __init__(self):
        super().__init__()
        self.PrimaryKeys.append("WeaponType")
    WeaponType = scrapy.Field()
    ...


class CustomLoaderBase(ItemLoader):
    default_output_processor = TakeFirst()
    default_input_processor = MapCompose(removebr, cNumber)

    def add_value(self, fieldname, value, *processors, **kw):
        try:
            fieldname = CF.replacen(fieldname, ["REQ", "\n"]).strip()
            if ("HP" in fieldname or "MP" in fieldname) and "%" in value:
                fieldname = "Perc " + fieldname
            value = CF.replacen(value, ["+", "%"]).strip()
            fieldname = fieldname.replace(" ", "")
            if fieldname.lower() in CF.REJSON["DBColumn"]:
                fieldname = CF.REJSON["DBColumn"][fieldname.lower()]
        except:
            pass
        try:
            self.replace_value(fieldname, value)
        except:
            self.item.AddNField(fieldname)
            super().add_value(fieldname, value, *processors, **kw)





class EquipLoader(CustomLoaderBase):
    EquipSlot_in = MapCompose(cEquipSlot)
    EquipLevel_in = MapCompose(cEquipLevel)
    EquipLevel_out = TakeFirst()
    default_item_class = EquipItem
    

#EQUIPMENT SET EFFECTS
class EquipSetItem(CustomItem):
    PrimaryKeys = ["EquipSet", "ClassType", "SetAt"]
    EquipSet = scrapy.Field()
    ClassType = scrapy.Field()
    SetAt = scrapy.Field()
    Test = scrapy.Field()

class ESLoader(CustomLoaderBase):
    default_item_class = EquipSetItem()



