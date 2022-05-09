
import logging
from tkinter.tix import Tree
import traceback
import pandas as pd
from pandas import DataFrame
import CustomLogger
import scrapy
import re

from ComFunc import if_In_String

#Naming Convention
#ClassName = Hero, Ark, etc
#ClassType = Warrior, Magician, etc



Eqlogger = CustomLogger.Set_Custom_Logger("EquipmentAllSpider", logTo ="./Logs/Equipment.log", propagate=False)
Eslogger = CustomLogger.Set_Custom_Logger("EquipmentSetSpider", logTo ="./Logs/EquipmentSet.log", propagate=False)
#configure_logging(install_root_handler=False)

class TotalEquipmentSpider(scrapy.Spider):
    name = "TotalEquipmentData"
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ['https://maplestory.fandom.com/wiki/Equipment']
    custom_settings = {
        "LOG_SCRAPED_ITEMS": False
        
    }
    
    Mclasses = ['Warrior','Knight', 'Bowman', 'Archer', 'Magician','Mage','Thief', 'Pirate']

    NonMseaClasses = {
            "Jett" : {
                "Weapon" :"Gun",
                "Secondary" : "Fist" }, 
            "Beast Tamer": {
                "Weapon" : "Scepter", 
                "Secondary" : "Whistle" }
            }
    FinalEquipTrack ={
        "Armor" : [],
        "Accessory" : [],
        "Weapon" : [],
        "Secondary" : [],
        "Android" : [],
        "Medal" : []
    }
    ArmorEquip = ['Hat','Top', 'Overall', 'Bottom', 'Shoes', 'Cape', 'Gloves']
    AccessoriesEquip = ['Shoulder', 'Face Accessory', 'Eye Accessory', 'Ring', 'Pendant', 'Earrings','Belt', 'Pocket Item', 'Android Heart', 'Badge', 'Emblem']
        
    EquipLevelMin = {
        "Hat" : 120, "Top" : 150, "Overall" : 120, "Bottom" :150, "Shoes" :120, "Cape" : 120, "Gloves" :120,
        "Shoulder" :120, 
        "Face Accessory" : 100,
        "Eye Accessory" : 100, 
        "Ring" : 30, "Pendant" : 75, 
        "Earrings" : 75, 
        "Belt" : 100, 
        "Pocket Item" : 0, 
        "Android": 0,"Android Heart":30,
        "Emblem" : 100, "Badge": 100, "Medal": 0,
        "Shield" : 110
    }

    EquipSetTrack = [
        'Sengoku', 'Boss Accessory', 'Pitched Boss', 'Seven Days', "Ifia's Treasure",'Mystic','Ardentmill','Blazing Sun'
        ,'8th', 'Root Abyss','AbsoLab', 'Arcane'
        ,'Lionheart', 'Dragon Tail','Falcon Wing','Raven Horn','Shark Tooth'
        ,'Gold Parts','Pure Gold Parts'
    ]
    
    WeapSetTrack = [
        'Utgard', 'Lapis','Lazuli'
        ,'Fafnir','AbsoLab', 'Arcane', 'Genesis'
    ]

        
    ignoreSecondary = ["evolving", "frozen"]
    
    ignoreSet = [
        'Immortal', 'Eternal','Walker','Anniversary', 
        "Sweetwater", "Commerci", "Gollux", "Alien", "Blackgate", "Glona",
        "Abyss", "Fearless", "Eclectic", "Reverse", "Timeless"]
    
    PageContentSkip = [
        "Extra Stats", "Notes", "Sold for", 
        "Tradability", "Bought from", "Dropped by",
        "Rewarded from", "Used In", "Used To", "Durability",
        "when first equipped", "Crafted via", "EXP"]
    
    
    
    def parse(self, response):

        TempLink = []
        ArmorT = response.xpath("//span[@id = 'Armor']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1]").css('table.maplestyle-equiptable')[0]
        
        for Slot in ArmorT.xpath(".//a[not(descendant::img)]/@href"):
            Link = Slot.extract()
            if Link.find("Totem")  !=  -1:
                continue
            nurl = response.urljoin(Link)
            TempLink.append(nurl)
            yield scrapy.Request(nurl, callback=self.HandleEquipment)
        
        weaponT = response.xpath("//span[@id = 'Weapon']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1] //a/@href").getall()
        for weaponLinks in weaponT:
            if if_In_String(weaponLinks, '/') == False:
                continue
            weaponType = " ".join(re.split('_|-', weaponLinks.split('/')[-1]))
            if if_In_String(weaponType, '('):
                weaponType = weaponType.split('(')[0]
            nurl = response.urljoin(weaponLinks)
            yield scrapy.Request(nurl, callback=self.HandleWeapon, meta={"WeaponType":weaponType.rstrip()}, dont_filter=True)
        
        secondaryT = response.xpath("//span[@id = 'Secondary_Weapon']/parent::h3/following-sibling::table[1]")
        for secondaryLinks  in secondaryT.css("a::attr(href)"):
            weaponType = " ".join(re.split('_|-', secondaryLinks.extract().split('/')[-1]))
            nurl = response.urljoin(secondaryLinks.extract())
            
            if weaponType == "Shield":
               yield scrapy.Request(nurl, callback=self.HandleEquipment, meta = {"Class" : "Any"})
            else:
                yield scrapy.Request(nurl, callback=self.HandleSecondary, meta={"WeaponType":weaponType})

    def close(self, reason):
        start_time = self.crawler.stats.get_value('start_time')
        
        
        try:
            pd.set_option("display.max_rows", None, "display.max_columns", None)
            #Weapon Dataframe
            WDF = pd.concat(self.FinalEquipTrack['Weapon'], ignore_index=True)
            WDF = CleanWeaponDF(WDF)
            #Secondary Dataframe
            SDF = pd.concat(self.FinalEquipTrack['Secondary'], ignore_index=True)
            SDF = CleanSecondaryDF(SDF)

            #Armor, Accessories, Android, Medal Dataframe
            ADF = pd.concat(self.FinalEquipTrack['Armor'], ignore_index=True)
            ADF = CleanArmorDF(ADF)
            AccDF = pd.concat(self.FinalEquipTrack['Accessory'], ignore_index=True)
            AccDF = CleanAccessoryDF(AccDF)
            AndroidDF = pd.concat(self.FinalEquipTrack['Android'], ignore_index=True)
            AndroidDF = CleanAndroidDF(AndroidDF)
            MDF = pd.concat(self.FinalEquipTrack['Medal'], ignore_index=True)
            MDF = CleanMedalDF(MDF)

            WDF.to_csv('./DefaultData/EquipmentData/WeaponData.csv')
            SDF.to_csv('./DefaultData/EquipmentData/SecondaryData.csv')
            ADF.to_csv('./DefaultData/EquipmentData/ArmorData.csv')
            AccDF.to_csv('./DefaultData/EquipmentData/AccessoryData.csv')
            AndroidDF.to_csv('./DefaultData/EquipmentData/AndroidData.csv')
            MDF.to_csv('./DefaultData/EquipmentData/MedalData.csv')

        except Exception:
            Eqlogger.warn(traceback.format_exc())
        finally:
            finish_time = self.crawler.stats.get_value('finish_time')
            print("Equipment scraped in: ", finish_time-start_time)

    
    def HandleWeapon(self, response):
        
        weaponTablesLinks = response.xpath("//div[@class='wds-tab__content wds-is-current']").css('b > a::attr(href)')
        if weaponTablesLinks == []:
            weaponTablesLinks = response.xpath("//div[@class='mw-parser-output'] //table[@class='wikitable'] //a[not(contains(@class,'image'))]/@href")
        for links in weaponTablesLinks:
            #Reformat link to retrieve weapon name
            weaponName = " ".join(re.split("%|_|-|#|27s", links.extract().split("/")[-1]))
            if weaponName.find('Sealed') != -1:
                continue
            commonStr = list(set(weaponName.split(' '))&set(self.WeapSetTrack))
            if commonStr == []:
                continue
            Nurl = response.urljoin(links.extract())
            ItemDict = {
                "EquipSlot" : "Weapon",
                "EquipName" : weaponName,
                "EquipSet" : commonStr[0],
                "WeaponType" : response.meta['WeaponType']
            }
            yield scrapy.Request(Nurl, callback=self.RetrieveByPage, cb_kwargs = {"ItemDict" : ItemDict})
   
    
    def HandleSecondary(self, response):
        Mdiv = response.css('div.mw-parser-output')
        classType = []
        CStart = False
        WeaponType = response.meta["WeaponType"]
        #Iterate through header para to get classnames
        for i in Mdiv.css("p"):
            texts = i.css('::text').getall()
            if texts == ['\n']:
                continue
            linktext = i.xpath('.//a//text()').getall()
            for s in texts:
                if s.find('exclusive') != -1:
                    CStart = True
                    continue
                if s.find("conjunction") != -1:
                    CStart = False
                    break
                if s.find(".") != -1:
                    CStart = False
                    break
                if CStart and s.strip(' ') != '\n':
                    if "Shade" in s:
                        continue
                    if s not in classType and s in linktext:
                        classType.append(s)
        
        tables = Mdiv.css('table.wikitable')
        
        if WeaponType == "Demon Aegis":
            classType.append("Demon")
        HTitle = []
        for i, C in enumerate(classType):
            #if other server exclusive classes, break
            if C.lower() in list(map(lambda x:x.lower(), self.NonMseaClasses)):
                break
            for row in tables[i].css('tr'):
                tda = self.removeBRN(row.xpath('.//text()').getall())
                if tda != []:
                    FirstEle = tda[0]
                    tda.pop(0)
                    if if_In_String(FirstEle.lower(), 'picture'):
                        HTitle = tda
                        continue
                    if any(value in FirstEle.lower() for value in self.ignoreSecondary):
                        continue
                    if WeaponType == "Katara" and list(set(FirstEle.split(' '))&set(self.WeapSetTrack)) == []:
                            continue
                    if if_In_String(FirstEle.lower(), 'equipment'):
                        break      

                    ItemDict = {
                        "EquipSlot" : "Secondary",
                        "ClassName" : C,
                        "WeaponType" : WeaponType,
                        "EquipName" : FirstEle
                        
                    }
                    for i, col in enumerate(HTitle):
                        Ctd = self.removeBRN(row.xpath(f'.//td[{i+2}] /text()').getall())
                        if if_In_String(col.lower(), ['requirements', 'effects']):
                            ItemDict.update(self.RetrieveByTDContent(Ctd, ItemDict))
                        else:
                            try:
                                ItemDict[col] = " ".join(Ctd)
                            except:
                                continue
                
                    self.RecordItemDict(ItemDict)
                    yield ItemDict
      
    
    def HandleEquipment(self, response):
    
        EquipSlot = response.xpath("//h1[@id = 'firstHeading']/text()").get().strip('\n\t').split('/')[0]
        
        tabsTitle = response.xpath("//div[@class='tabber wds-tabber'] //ul[@class='wds-tabs'] //li").css("::text").getall()
        Unobtainable =[i for i, j in enumerate(tabsTitle) if "unobtainable" in j.lower()]
        
        Wikitable = response.xpath("//div[@class='tabber wds-tabber'][1] //div[contains(@class, 'wds-tab__content')] //table[@class='wikitable']")
        if Wikitable == []:
            Wikitable = response.xpath("//div[@class='mw-parser-output'] //table[@class='wikitable']")

        if Unobtainable != []:
            Wikitable.pop(Unobtainable[0])
            tabsTitle.pop(Unobtainable[0])
        try:
            altTable = Wikitable.xpath("./ancestor::div //a[contains(@title, '{}/')]/@href".format(EquipSlot))
            for altLink in altTable:
                nurl = response.urljoin(altLink.extract())
                yield scrapy.Request(nurl, callback=self.HandleEquipment)
                
        except Exception as E:
            Eqlogger.critical(E)
            pass
        HTitle = []
        for i, table in enumerate(Wikitable):
            Category = tabsTitle[i] if tabsTitle != [] else ""
            for row in table.xpath(".//tr"):
                
                link =  row.xpath(".//a[not(contains(@class,'image')) and not(contains(@href,'redlink'))] //@href").get()
                td = self.removeBRN(row.xpath(".//text()").extract())
                EquipName = td[0]
                td.pop(0)
                if if_In_String(EquipName.lower(),'picture'): 
                    HTitle = td
                    continue
                if not EquipName.isascii() or any(value in EquipName for value in self.ignoreSet):
                    continue
                LevelT = [value for value in td if "Level" in value or "None" in value]
                clvl = 0 if "None" in LevelT[0] else int(LevelT[0].split(" ")[1])
                if EquipSlot in self.EquipLevelMin.keys():
                    if clvl < self.EquipLevelMin[EquipSlot]:
                        continue
                if link == None or EquipSlot == "Android":
                    ItemDict = {
                        "EquipSlot" :EquipSlot,
                        "EquipName" : EquipName,
                        "Category" : Category,
                        "Level" : clvl 
                    }
                    for i, col in enumerate(HTitle):
                        Ctd = self.removeBRN(row.xpath(f'.//td[{i+2}] /text()').getall())
                        if if_In_String(col.lower(), ['requirements', 'effects']):
                            ItemDict.update(self.RetrieveByTDContent(Ctd, ItemDict))
                        else:
                            try:
                                ItemDict[col] = " ".join(Ctd)
                            except:
                                continue
                    self.RecordItemDict(ItemDict)
                    yield ItemDict
                else:
                    nurl = response.urljoin(link)
                    ItemDict = {
                        "EquipSlot" : EquipSlot,
                        "EquipName" : EquipName,
                        "Category" : Category
                    }
                    yield scrapy.Request(nurl, callback=self.RetrieveByPage, cb_kwargs={"ItemDict": ItemDict})
                        
    
    def RetrieveByPage(self, response, ItemDict):
        if not response:
            return
        PageTitle = response.xpath("//h1[@class='page-header__title']/text()").get()
        TableTrack = 1
        
        AlternateTablesTitles = response.xpath("//div[@class = 'toc'] //ul/li[contains(@class, 'toclevel-2')]/a /span[@class='toctext']/text()").getall()
        if AlternateTablesTitles == []:
            AlternateTablesTitles = response.xpath("//div[@class = 'toc'] //ul/li[contains(@class, 'toclevel-1')]/a /span[@class='toctext']/text()").getall()
        
        TableContent = response.css("div.mw-parser-output table")
        if PageTitle.find("Genesis") != -1:
            TableContent = response.xpath("//h2/span[@id = 'Unsealed']/parent::h2/following-sibling::table")
        
        TableCount = 0
        for ctable in TableContent:
            if AlternateTablesTitles != []:
                TableTrack = len(AlternateTablesTitles)
                H3 = ctable.xpath("./preceding-sibling::*[1][self::h3] //span/text()").get()
                H2 = ctable.xpath("./preceding-sibling::*[1][self::h2] //span/text()").get()
                try:
                    if list(set(AlternateTablesTitles)&set([H3, H2])) == []:
                        continue
                except Exception as E:
                    print(E)
            TableCount += 1
            if TableCount > TableTrack:
                break
            td = ["".join(value.css("td ::text").getall()).strip('\n') for value in ctable.css('tr') if value.css("td ::text").getall() != ['\n']]
            th = self.removeBRN(ctable.css('tr > th ::text').getall())
            ItemDict["EquipName"] = td[0].rstrip(' \n')
            for key, value in zip(th, td[1:]):
                if any(value.lower() in key.lower() for value in self.PageContentSkip):
                    continue
                try:
                    if "(" in value:
                        value = value.split('(')[0]
                    nvalue = value.strip('+%').rstrip(' ')
                    if "REQ" in key:
                        if "Level" in key:
                            ItemDict["Level"] = nvalue
                        elif "Job" in key:
                            if "ClassName" in ItemDict.keys():
                                ItemDict['ClassType'] = nvalue
                            else:
                                ItemDict['ClassName'] = nvalue
                        else:
                            continue
                    else:
                        ItemDict[key] = nvalue
                except:
                    continue
            if "Equipment Set" in ItemDict.keys():
                Set = ItemDict['Equipment Set']
                SetI = Set.find('Set')
                Set = Set[:SetI]
                ClassFound = list(set(Set.split(' '))&set(self.Mclasses))
                if ClassFound != []:
                    Set = Set.replace(ClassFound[0], '')
                ItemDict['Equipment Set'] = Set.rstrip(' ')
                       
            self.RecordItemDict(ItemDict=ItemDict)
            yield ItemDict
    
    def RetrieveByTDContent(self, content, ItemDict = {}):
        
        for stat in content:
            if stat.find(':') != -1:
                key, value = stat.split(':')
                if value.strip(' ') == "":
                    continue
                ItemDict[key] = value.strip(' +\n')
            else:
                try:                   
                    key, value = stat.split(' ')
                    ItemDict[key] = value.rstrip(' \n')
                except:
                    continue
        return ItemDict
    
    def RecordItemDict(self, ItemDict):
        #Save to respective dataframes
        try:
            EquipSlot = ItemDict['EquipSlot']
            if EquipSlot is not None:
                if EquipSlot in self.ArmorEquip:
                    self.FinalEquipTrack['Armor'].append(DataFrame(ItemDict, index=[0]))
                elif EquipSlot in self.AccessoriesEquip:
                    self.FinalEquipTrack['Accessory'].append(DataFrame(ItemDict, index=[0]))
                else:
                    if EquipSlot == "Shield":
                        self.FinalEquipTrack["Secondary"].append(DataFrame(ItemDict, index=[0]))
                    else:                        
                        self.FinalEquipTrack[EquipSlot].append(DataFrame(ItemDict, index=[0]))
                Eqlogger.info(f"Adding {ItemDict['EquipName']}")
        except Exception:
            Eqlogger.critical(traceback.format_exc())

    def removeBRN(self, List):
        return [value.strip('\n') for value in List if value.strip(' ') != '\n']

class EquipmentSetSpider(scrapy.Spider):
    name = "EquipmentSetData"
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ['https://maplestory.fandom.com/wiki/Equipment']
    custom_settings = {
        "LOG_SCRAPED_ITEMS": False
        
    }
    TrackEquipSets = {
        "Set" : [],
        "Cumulative" : []
    }
    
    EquipSetTrack = [
        'Sengoku', 'Boss Accessory', 'Pitched Boss', 'Seven Days', "Ifia's Treasure",'Mystic','Ardentmill','Blazing Sun'
        ,'8th', 'Root Abyss','AbsoLab', 'Arcane'
        ,'Lionheart', 'Dragon Tail','Falcon Wing','Raven Horn','Shark Tooth'
        ,'Gold Parts','Pure Gold Parts'
    ]
    
    ignoreSet = [
        'Immortal', 'Eternal','Walker','Anniversary', 
        "Sweetwater", "Commerci", "Gollux", "Alien", "Blackgate", "Glona"]
    

    def parse(self, response):

        EquipmentSet =response.xpath("//span[@id = 'Equipment_Set']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1] //table[@class='wikitable']")
        ClassHeaders = response.xpath("//span[@id = 'Equipment_Set']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1] //li[contains(@class,'wds-tabs__tab')] //text()").getall()
        #TempL = []
        for ix, tables in enumerate(EquipmentSet):
            ClassType = "Any" if ClassHeaders[ix] == "Common" else ClassHeaders[ix]
            for equipLinks in tables.xpath(".//a[not(descendant::img)]/@href"):
                setName = " ".join(re.split("%|_|-|#|27s", equipLinks.extract().split("/")[-1])).replace("Set", '').replace(ClassType, '').rstrip()
                if if_In_String(setName, '('):
                    setName = setName.split('(')[0].rstrip()
                if any(value in setName for value in self.EquipSetTrack):
                    if any(value in setName for value in self.ignoreSet):
                        continue
                    nurl = response.urljoin(equipLinks.extract())
                    #TempL.append((nurl, setName))
                    yield scrapy.Request(nurl, callback=self.HandleEquipmentSet, meta = {"EquipSet" : setName, "ClassType":ClassType})
            

        
        #CU = TempL[1]
        #yield scrapy.Request(CU[0], callback=self.HandleEquipmentSet, meta={"EquipSet" : CU[1]})
            
        print("Donezo")
        pass

    def close(self):
        start_time = self.crawler.stats.get_value('start_time')
        finish_time = self.crawler.stats.get_value('finish_time')
        print("Set Effects scraped in: ", finish_time-start_time)
        SetDF = pd.concat(self.TrackEquipSets["Set"], ignore_index=True)
        SetDF = CleanSetEffect(SetDF)
        CulDF = pd.concat(self.TrackEquipSets['Cumulative'], ignore_index=True)
        CulDF = CleanSetEffect(CulDF)
        
        SetDF.to_csv("./DefaultData/EquipmentData/EquipSetData.csv")
        CulDF.to_csv("./DefaultData/EquipmentData/EquipSetCulData.csv")
        
        

    def HandleEquipmentSet(self, response):

        Wikitable = response.xpath("//div[@class='mw-parser-output'] //table[@class='wikitable'][1]")
        EquipSet = response.meta["EquipSet"]
        ClassType = response.meta['ClassType']
        TRRows = Wikitable.xpath(".//tr")
        Header =  self.removeBRN(TRRows.xpath(".//th /text()").getall())
        try:
            for i, SetType in enumerate(Header):
                
                if "Set" in SetType:
                    SetType = "Set"
                if "Cumulative" in SetType:
                    SetType = "Cumulative"
                for row in TRRows: 
                    
                    SetAt = row.xpath(".//td[1] /b/text()").getall()
                    if SetAt == []:
                        continue
                    else:
                        SetAt = SetAt[0].split(' ')[0]
                    ItemDict = {
                        "EquipSet" : EquipSet,
                        "ClassType" : ClassType,
                        "Set At" : SetAt
                    }
                    SetEffectAt  = self.removeBRN(row.xpath(f".//td[{i+1}] /text()").getall())
                    self.TrackEquipSets[SetType].append(DataFrame(self.RetrieveByTDContent(SetEffectAt, ItemDict), index=[0]))
        except:
            Eslogger.critical(traceback.format_exc())
        Eslogger.info(f"Adding {EquipSet}")
        return ItemDict


    def RetrieveByTDContent(self, content, ItemDict = {}):
        
        for stat in content:
            if if_In_String(stat, ["Max", "%"], mode="All"):
                key, value = stat.split(':')
                ItemDict["Perc " + key] = value.strip(' +%\n')
                continue
            if if_In_String(stat, ":"):
                key, value = stat.split(':')
                if if_In_String(value, "("):
                    value = value.split('(')[0]
                ItemDict[key] = value.strip(' +%\n').replace(",", '')
            else:
                try:                   
                    splitvalue = stat.split(' ')
                    value = splitvalue[-1]
                    key = " ".join(splitvalue[:-1])
                    ItemDict[key] = value.rstrip(' +%\n').replace(',', '')
                except:
                    continue
        return ItemDict

    def removeBRN(self, List):
        return [value.strip('\n') for value in List if value.strip(' ') != '\n']



def CleanWeaponDF(CDF):
    ColumnOrder = [
        "EquipSlot","EquipSet","EquipName","WeaponType",
        "Level","STR","DEX","LUK","INT","Max HP","Defense",
        "Weapon Attack","Magic Attack","Attack Speed","Boss Damage","Ignored Enemy Defense",
        "Movement Speed","Knockback Chance", "Number of Upgrades","Equipment Set"
    ]
    CDF = CDF[ColumnOrder]
    CDF = CDF.rename(columns={
        'Level' : 'EquipLevel'
    })
    CDF = CDF.fillna(0)
    return CDF

def CleanSecondaryDF(CDF):
    ColumnOrder =[
        "EquipSlot","ClassName","EquipName","WeaponType",
        "Level", "STR","DEX","LUK","INT","All Stats","Max HP","Max MP","Defense",
        "Weapon Attack","Magic Attack","Attack Speed",
        "Max DF"     
    ]
    CDF = CDF[ColumnOrder]
    CDF = CDF.rename(columns={
        "Level" : "EquipLevel"
    })
    CDF.loc[CDF["WeaponType"] == 'Arrowhead', 'WeaponType'] = "Arrow Head"
    CDF.loc[CDF["WeaponType"] == 'Bladebinder', 'WeaponType'] = "Bracelet"
    CDF = CDF.fillna(0)
    return CDF

def CleanArmorDF(CDF):
    CDF.drop("Category", axis=1, inplace=True)
    ColumnOrder = [
        "EquipSlot","ClassName","EquipName","Equipment Set",
        "Level","STR","DEX","LUK","INT","Max HP","Max MP","Defense",
        "Weapon Attack","Magic Attack", "Ignored Enemy Defense",
        "Movement Speed","Jump", 
        "Number of Upgrades"
        ]
    CDF = CDF[ColumnOrder]
    
    CDF = CDF.rename(columns={
        "Level" : "EquipLevel",
        "Equipment Set" : "EquipSet",
        "Defense" : "DEF",
        "ClassName" : "ClassType"
    })

    CDF['EquipSet'] = CDF['EquipSet'].fillna('None')
    CDF.loc[CDF['Number of Upgrades'] == "None", "Number of Upgrades"] = 0
    #CDF['EquipSet'].fillna('None', inplace =True)
    CDF = CDF.fillna(0)

    return CDF

def CleanAccessoryDF(CDF):
    print(CDF._is_view)
    ColumnOrder = [
    "EquipSlot","ClassName","EquipName","Equipment Set",
    "Category",
    "Level","STR","DEX","LUK","INT","All Stats","Max HP","Max MP","Defense",
    "Weapon Attack","Magic Attack","Ignored Enemy Defense",
    "Movement Speed","Jump",
    "Number of Upgrades",
    "Job",
    "Rank"]
    CDF = CDF[ColumnOrder]
    CDF = CDF.rename(columns={
        "Level" : "EquipLevel",
        "Equipment Set" : "EquipSet"
    })
    

    CDF.loc[CDF['Job'].isnull(), "Job"] = "Any"    
    CDF.loc[CDF["ClassName"].isnull(), "ClassName"] = CDF['Job']
    CDF.drop("Job", axis=1, inplace=True)
    CDF.loc[CDF['Number of Upgrades'] == "None", "Number of Upgrades"] = 0

    CDF.loc[CDF['Category'].isnull(), "Category"] = "Obtainable"    
    CDF.loc[CDF['EquipSet'].isnull(), "EquipSet"] = "None"    


    CDF = CDF.fillna(0) 

    return CDF

def CleanAndroidDF(CDF):
    CDF.drop(['Appearance', '[1]'], axis = 1, inplace = True)
    return CDF

def CleanMedalDF(CDF):

    ColumnOrder = [
        "EquipSlot","ClassName","EquipName","Equipment Set",
        "Category",
        "Level","STR","DEX","LUK","INT","All Stats","Max HP","Max MP","Defense",
        "Weapon Attack","Magic Attack","Ignored Enemy Defense","Boss Damage",
        "Movement Speed","Jump","Number of Upgrades"
    ]

    CDF = CDF[ColumnOrder]
    CDF.rename(columns ={
        "Equipment Set" :"EquipSet",
        "Level" : "EquipLevel"
    }, inplace = True) 
    CDF['ClassName'] = CDF['ClassName'].fillna("Any")
    CDF['EquipSet'] = CDF['EquipSet'].fillna("None")
    CDF.loc[CDF['Category'] == "Uncategorized", "Category"] = "Obtainable"
    CDF.drop("Number of Upgrades", axis=1, inplace=True)
    CDF = CDF.fillna(0) 


    return CDF

def CleanSetEffect(CDF):
    try:
        ColumnOrder = [
            "EquipSet","ClassType","Set At",
            "STR","DEX","LUK","INT","All Stats","Max HP","Max MP","Perc Max HP","Perc Max MP","Defense",
            "Weapon Attack","Magic Attack","Ignored Enemy Defense","Boss Damage",
            "Critical Damage", "Damage","All Skills","Damage Against Normal Monsters","Abnormal Status Resistance"
        ]

        
        CDF = CDF[ColumnOrder]
        
        CDF = CDF.fillna(0) 
    except:
        Eslogger.critical(traceback.format_exc())

    return CDF