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
from w3lib.html import remove_tags, strip_html5_whitespace
import ComFunc as CF


#CLEAN
def cRemovebr(list):
    s = list.strip('\n')
    if s != "":
        return s

def cNumber(value):
    if value.isdigit():
        return int(value)
    return value

def cEquipSlot(value):
    return value.strip('\n\t').split('/')
    ...

def cEquipLevel(td):
    if "level" in td.lower():
        return td.split(" ")[1]
    elif "none" in td.lower():
        return 0

def cCategory(value):
    return CF.replacen(value, ",", ";")


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

    #Processor (Middle Precedence)
    EquipSlot  = scrapy.Field()
    EquipName  = scrapy.Field()
    EquipLevel = scrapy.Field()
    EquipSet   = scrapy.Field()
    ClassType  = scrapy.Field()
    Category   = scrapy.Field()

    ...

class WeaponItem(EquipItem):
    def __init__(self):
        super().__init__()
        self.PrimaryKeys.append("WeaponType")
    WeaponType = scrapy.Field()
    ...


class LBase(ItemLoader):
    #Processor (Least Precedence)
    default_output_processor = TakeFirst()

    def add_value(self, fieldname, value, *processors, **kw):
        try:
            fieldname = CF.replacen(fieldname, ["REQ", "\n"]).strip()
            if "%" in value and ("HP" in fieldname or "MP" in fieldname):
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

class EquipLoader(LBase):
    default_item_class = EquipItem
    #Processor (Highest Precedence)
    EquipSlot_in   = MapCompose(cRemovebr, cEquipSlot)
    EquipName_in   = MapCompose(cRemovebr)
    EquipLevel_in  = MapCompose(cRemovebr, cEquipLevel, cNumber)
    ClassType_in   = MapCompose(cRemovebr, cClassType)
    Category_in    = MapCompose(cRemovebr, cCategory)
    
    EquipName_out  = oEquipName
    

#EQUIPMENT SET EFFECTS
class EquipSetItem(CustomItem):
    PrimaryKeys = ["EquipSet", "ClassType", "SetAt"]
    EquipSet = scrapy.Field()
    ClassType = scrapy.Field()
    SetAt = scrapy.Field()

class ESLoader(LBase):
    default_item_class = EquipSetItem()



