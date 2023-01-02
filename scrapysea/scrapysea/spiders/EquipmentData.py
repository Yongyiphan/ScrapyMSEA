
import traceback
import pandas as pd

import CustomLogger
import scrapy
import re
import ComFunc as CF


from ComFunc import *
from tqdm import *

#Naming Convention
#ClassName = Hero, Ark, etc
#ClassType = Warrior, Magician, etc


Eqlogger = CustomLogger.Set_Custom_Logger("EquipmentAllSpider", logTo ="./Logs/Equipment.log", propagate=False)
Eslogger = CustomLogger.Set_Custom_Logger("EquipmentSetSpider", logTo ="./Logs/EquipmentSet.log", propagate=False)

#configure_logging(install_root_handler=False)
from scrapy.loader import ItemLoader
from scrapysea.items import *

class TotalEquipmentSpider(scrapy.Spider):
    name = "TotalEquipmentData"
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ['https://maplestory.fandom.com/wiki/Equipment']
    custom_settings = {
        "LOG_SCRAPED_ITEMS": False,
        "ITEM_PIPELINES" : {
            #'scrapysea.pipelines.ScrapyseaPipeline': 300,
            'scrapysea.pipelines.ItemRenamePipeline'  : 800,
            'scrapysea.pipelines.SqliteDBItemPipeline': 1000
        }
        
    }
    FinalCategory = ["Armor", "Accessory", "Weapon", "Secondary", "Android", "Medal"] 

    ArmorEquip = [
            'Hat',
            'Top',      'Overall', 
            'Bottom',   'Gloves',   'Cape', 
            'Shoes',]
    AccessoriesEquip = [
                                                            'Emblem', 
        'Ring', 'Pendant',  'Face Accessory',               'Badge',
                            'Eye Accessory',    'Earrings',
                                                'Shoulder',
        'Pocket','Belt',
                                                            'Heart']
        
    EquipLevelMin = {
        "Hat" : 120, "Top" : 150, "Overall" : 120, "Bottom" :150, "Shoes" :120, "Cape" : 120, "Gloves" :120,
        "Shoulder" :120, 
        "Face Accessory" : 100,
        "Eye Accessory" : 100, 
        "Ring" : 30, "Pendant" : 75, 
        "Earrings" : 75, 
        "Belt" : 140, 
        "Pocket Item" : 0, 
        "Android": 0,"Android Heart":30,
        "Emblem" : 100, "Badge": 100, "Medal": 0,
        "Shield" : 110
    }

    #EquipSetTrack = [
    #    'Sengoku', 'Boss Accessory', 'Pitched Boss', 'Seven Days', "Ifia's Treasure",'Mystic','Ardentmill','Blazing Sun'
    #    ,'8th', 'Root Abyss','AbsoLab', 'Arcane','Eternal'
    #    ,'Lionheart', 'Dragon Tail','Falcon Wing','Raven Horn','Shark Tooth'
    #    ,'Gold Parts','Pure Gold Parts'
    #]
    
    WeapSetTrack = [
        'Utgard', 'Lapis','Lazuli'
        ,'Fafnir','AbsoLab', 'Arcane', 'Genesis'
    ]
    Ignore = {
        "ignoreSecondary" : ["evolving", "frozen"],
        "ignoreSet" : [
            'Immortal', 'Walker','Anniversary', 
            "Sweetwater", "Commerci", "Gollux", "Alien", "Blackgate", "Glona",
            "Abyss", "Fearless", "Eclectic", "Reverse", "Timeless"],
        "PageContentSkip" : [
            "extra stats", "notes", "sold for", 
            "tradability", "bought from", "dropped by",
            "rewarded from", "used in", "used to", "durability",
            "when first equipped", "crafted via", "exp"]
    }
    ItemCount = {}
    FinalEquipTrack = {}
    ColStruct = {}
    rename = {}
    #renamed/ exclude lists in ReName.json <=> self.rename
    def parse(self, response):
        try:
            for t in self.FinalCategory:
                t = "Equip" + t
                self.FinalEquipTrack[t] = pd.DataFrame()
                self.ColStruct[t] = []
                self.ItemCount[t] = tqdm(desc=t, leave=True, total=0)

            ArmorT = response.xpath("(//span[@id ='Armor']/parent::h2/following-sibling::div//table[@class='maplestyle-equiptable'])[1] //table//a[contains(@href, 'wiki/')]")
            for Slot in ArmorT:
                Link = Slot.xpath(".//@href").get()
                S = Slot.xpath(".//@title").get()
                if S == 'Totem':
                    continue
                RenameSlot =[k for k, v in self.rename['Equipment']['Slot'].items() if CF.replacen(S.lower().strip(), " ") in v] 
                S = RenameSlot[0] if RenameSlot != [] else S
                nurl = response.urljoin(Link)
                yield scrapy.Request(nurl, callback=self.HandleEquipment, cb_kwargs={"EquipSlot":S, "Link":Link})
                break
            #weaponT = response.xpath("//span[@id = 'Weapon']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1] //a/@href").getall()
            #weapignore = [i[0] for i in self.rename['NonMseaClasses'].values()]
            #secignore = [i[1] for i in self.rename['NonMseaClasses'].values()]
            #for weaponLinks in weaponT:
            #    if CF.instring(weaponLinks, '/') == False:
            #        continue
            #    weaponType = " ".join(re.split('_|-', weaponLinks.split('/')[-1]))
            #    if CF.instring(weaponType, '('):
            #        weaponType = weaponType.split('(')[0]
            #    if weaponType.rstrip().lower() in weapignore:
            #        continue
            #    nurl = response.urljoin(weaponLinks)
            #    yield scrapy.Request(nurl, callback=self.HandleWeapon, meta={"WeaponType":weaponType.rstrip()}, dont_filter=True)
            #
            #secondaryT = response.xpath("//span[@id = 'Secondary_Weapon']/parent::h3/following-sibling::table[1]")
            #for secondaryLinks  in secondaryT.css("a::attr(href)"):
            #    weaponType = " ".join(re.split('_|-', secondaryLinks.extract().split('/')[-1]))
            #    if weaponType.rstrip().lower() in secignore:
            #        continue
            #    nurl = response.urljoin(secondaryLinks.extract())
            #    
            #    if weaponType == "Shield":
            #        yield scrapy.Request(nurl, callback=self.HandleEquipment, meta = {"Class" : "Any"})
            #    else:
            #        yield scrapy.Request(nurl, callback=self.HandleSecondary, meta={"WeaponType":weaponType})
        except Exception as E:
            Eqlogger.warn(traceback.format_exc())
    def close(self):
        try:
            for val in sum(self.rename['Dataframe ReCol'].values(), []):
                for k, l in self.FinalEquipTrack.items():
                    if val in l.columns.values.tolist():
                        ...
                    ...

            pd.set_option("display.max_rows", None, "display.max_columns", None)
            #Weapon pd.pd.DataFrame
            #WDF = pd.concat(self.FinalEquipTrack['Weapon'], ignore_index=True)
            #WDF = CleanWeaponDF(WDF)
            ##Secondary pd.DataFrame
            #SDF = pd.concat(self.FinalEquipTrack['Secondary'], ignore_index=True)
            #SDF = CleanSecondaryDF(SDF)

            ##Armor
            #ADF = pd.concat(self.FinalEquipTrack['Armor'], ignore_index=True)
            #ADF = CleanArmorDF(ADF)
            ##Accessories
            #AccDF = pd.concat(self.FinalEquipTrack['Accessory'], ignore_index=True)
            #AccDF = CleanAccessoryDF(AccDF)
            ##Android
            #AndroidDF = pd.concat(self.FinalEquipTrack['Android'], ignore_index=True)
            #AndroidDF = CleanAndroidDF(AndroidDF)
            ##Medals
            #MDF = pd.concat(self.FinalEquipTrack['Medal'], ignore_index=True)
            #MDF = CleanMedalDF(MDF)

            #WDF.to_csv(CF.APPFOLDER + 'EquipmentData\\WeaponData.csv')
            #SDF.to_csv(CF.APPFOLDER + 'EquipmentData\\SecondaryData.csv')
            #ADF.to_csv(CF.APPFOLDER + 'EquipmentData\\ArmorData.csv')
            #AccDF.to_csv(CF.APPFOLDER + 'EquipmentData\\AccessoryData.csv')
            #AndroidDF.to_csv(CF.APPFOLDER + 'EquipmentData\\AndroidData.csv')
            #MDF.to_csv(CF.APPFOLDER + 'EquipmentData\\MedalData.csv')

            #Eqlogger.info("Scraped {0} Weapons".format(len(WDF)))
            #Eqlogger.info("Scraped {0} Secondaries".format(len(SDF)))
            #Eqlogger.info("Scraped {0} Armors".format(len(ADF)))
            #Eqlogger.info("Scraped {0} Accessories".format(len(AccDF)))
            #Eqlogger.info("Scraped {0} Androids".format(len(AndroidDF)))
            #Eqlogger.info("Scraped {0} Medals".format(len(MDF)))

        except Exception:
            Eqlogger.warn(traceback.format_exc())
        finally:
            CF.TimeTaken(self)

    
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
            for row in tables[i].css('tr'):
                tda = CF.removebr(row.xpath('.//text()').getall())
                if tda != []:
                    FirstEle = tda[0]
                    tda.pop(0)
                    if CF.instring(FirstEle.lower(), 'picture'):
                        HTitle = tda
                        continue
                    if WeaponType == "Katara" and list(set(FirstEle.split(' '))&set(self.WeapSetTrack)) == []:
                            continue
                    if CF.instring(FirstEle.lower(), 'equipment'):
                        break      

                    ItemDict = {
                        "EquipSlot" : "Secondary",
                        "ClassName" : CF.replacen(C, ','),
                        "WeaponType" : WeaponType,
                        "EquipName" : CF.replacen(FirstEle, [',','<','>']).strip()
                        
                    }
                    for i, col in enumerate(HTitle):
                        Ctd = CF.removebr(row.xpath(f'.//td[{i+2}] /text()').getall())
                        if CF.instring(col.lower(), ['requirements', 'effects']):
                            ItemDict.update(self.RetrieveByTDContent(Ctd, ItemDict))
                        else:
                            try:
                                ItemDict[col] = " ".join(Ctd)
                            except:
                                continue
                
                    self.RecordItemDict(ItemDict)
                    yield ItemDict
      
    def HandleEquipment(self, response, EquipSlot, Link):
        try:
            altTable = response.xpath("//div[@class='mw-parser-output'] //a[contains(@href, '{}')]/@href".format(Link)).getall()
            for altLink in altTable:
                yield scrapy.Request(response.urljoin(altLink), callback=self.HandleEquipment, cb_kwargs={"EquipSlot":EquipSlot, "Link":Link})
            
            tabsTitle = response.xpath("//div[@class='tabber wds-tabber'] //ul[@class='wds-tabs'] //li //text()").getall()
            Unobtainable = [i for i, j in enumerate(tabsTitle) if "unobtainable" in j.lower()]
            
            Wikitable = response.xpath("//div[@class='tabber wds-tabber'][1] //div[contains(@class, 'wds-tab__content')] //table[@class='wikitable']")
            if Wikitable == []:
                Wikitable = response.xpath("//div[@class='mw-parser-output'] //table[@class='wikitable']")
            
            if Unobtainable != []:
                Wikitable.pop(Unobtainable[0])
                tabsTitle.pop(Unobtainable[0])
            
            Cat = self.CatID(EquipSlot)
            for i, table in enumerate(Wikitable):
                Content = table.xpath(".//tr")
                self.ItemCount[Cat].total += (len(Content) - 1 )
                HTitle = CF.removebr(Content[0].xpath(".//text()").getall())
                for j, row in enumerate(Content[1:]):
                    citem = EquipItem()
                    l = EquipLoader(item = citem, response=response)
                    l.selector = row
                    l.add_value("EquipSlot", EquipSlot)
                    l.replace_xpath("EquipName", ".//text()")
                    EquipName = l.get_output_value("EquipName")

                    l.replace_xpath("EquipLevel", ".//text()")
                    clvl = l.get_output_value("EquipLevel")

                    if not EquipName.isascii() or clvl < self.EquipLevelMin[EquipSlot] or any(value in EquipName for value in self.Ignore["ignoreSet"]):
                        self.ItemCount[Cat].total -= 1
                        self.ItemCount[Cat].refresh()
                        continue
                    
                    l.add_value("Category", tabsTitle[i] if tabsTitle != [] else "")
                    l.add_value("Destination", Cat)
                    ItemDict = {}
                    link =  row.xpath(".//a[not(contains(@class,'image')) and not(contains(@href,'redlink'))] //@href").get()
                    if link == None or EquipSlot == "Android":
                        for i, col in enumerate(HTitle):
                            Ctd = CF.removebr(row.xpath(f'.//td[{i+1}] /text()').getall())
                            if CF.instring(col.lower(), ['requirements', 'effects']):
                                ItemDict.update(self.RetrieveByTDContent(Ctd, ItemDict))
                            else:
                                try:
                                    if CF.instring(col.lower(),['appearance', 'functions', '[']):
                                        continue
                                    ItemDict[col] = " ".join(Ctd)
                                except:
                                    continue
                        self.RecordItemDict(ItemDict)
                    else:
                        nurl = response.urljoin(link)
                        yield scrapy.Request(nurl, callback=self.RetrieveByPage, cb_kwargs={"l" : l})
        except Exception as E:
            Eqlogger.critical(traceback.format_exc())

    def RetrieveByPage(self, response, l):
        if not response:
            return
        
        try:
            l.context['response'] = response
            citem = l.load_item()
            Contents = response.xpath("//div[@class='mw-parser-output'] //table[not (contains(@class, 'collapsible'))]")
            #Normal case (1 table of stat contents)
            T = []
            for table in Contents:
                EName = CF.removebr(table.xpath(".//tr//text()").getall())[0]
                if "recipe" in EName.lower():
                    break
                for row in table.xpath(".//tr")[2:]:
                    StatID = row.xpath(".//th //text()").get()
                    T.append(StatID)
                    if StatID == None:
                        continue
                    StatID = CF.replacen(StatID, ["\n"]).strip()
                    if (StatID == "Level" or 
                        StatID.lower() in self.Ignore["PageContentSkip"]):
                        #Eqlogger.debug("Skipped {0}".format(StatID))
                        continue
                    if "REQ" in StatID and "Job" not in StatID:
                        continue
                    P = set([x.lower() for x in StatID.split(" ")])
                    if any(P.intersection(set(x.lower().split(" "))) for x in self.Ignore["PageContentSkip"]):
                        #Eqlogger.debug("Skipped {0}".format(StatID))
                        continue

                    StatC = " ".join(row.xpath(" .//td //text()").getall())
                    l.add_value(StatID, StatC)

            citem = l.load_item()
            self.RecordItemDict(citem._values)
            yield citem
        except Exception:
            Eqlogger.critical(traceback.format_exc())

#        PageTitle = response.xpath("//h1[@class='page-header__title']/text()").get()
#        TableTrack = 1
#        
#        AlternateTablesTitles = response.xpath("//div[@class = 'toc'] //ul/li[contains(@class, 'toclevel-2')]/a /span[@class='toctext']/text()").getall()
#        if AlternateTablesTitles == []:
#            AlternateTablesTitles = response.xpath("//div[@class = 'toc'] //ul/li[contains(@class, 'toclevel-1')]/a /span[@class='toctext']/text()").getall()
#        
#        TableContent = response.css("div.mw-parser-output table")
#        if PageTitle.find("Genesis") != -1 and ItemDict["EquipSlot"] == "Weapon":
#            if(ItemDict["WeaponType"] == "Long Sword" or ItemDict["WeaponType"] == "Heavy Sword") == False:
#                TableContent = response.xpath("//h2/span[@id = 'Unsealed']/parent::h2/following-sibling::table")
#        TempDict = CF.DeepCopyDict(ItemDict)
#        TableCount = 0
#
#        TableContent.pop()
#        
#        for ctable in TableContent:
#            ItemDict = CF.DeepCopyDict(TempDict)
#            if AlternateTablesTitles != []:
#                TableTrack = len(AlternateTablesTitles)
#                H3 = ctable.xpath("./preceding-sibling::*[1][self::h3] //span/text()").get()
#                H2 = ctable.xpath("./preceding-sibling::*[1][self::h2] //span/text()").get()
#                try:
#                    if list(set(AlternateTablesTitles)&set([H3, H2])) == []:
#                        continue
#                except Exception as E:
#                    print(E)
#            TableCount += 1
#            if TableCount > TableTrack:
#                break
#            td = ["".join(value.css("td ::text").getall()).strip('\n') for value in ctable.css('tr') if value.css("td ::text").getall() != ['\n']]
#            th = CF.removebr(ctable.css('tr > th ::text').getall())
#            
#            ItemDict = CF.DeepCopyDict(ItemDict)
#            for key, value in zip(th, td[1:]):
#                if any(value.lower() in key.lower() for value in self.Ignore["PageContentSkip"]):
#                    continue
#                try:
#                    if "(" in value:
#                        value = CF.replacen(value, ['(',')'])
#                    nvalue = value.strip('+').rstrip(' ')
#                    if "REQ" in key:
#                        if "Level" in key:
#                            ItemDict["EquipLevel"] = nvalue
#                        elif "Job" in key:
#                            ItemDict['ClassName'] = nvalue
#                        else:
#                            continue
#                        continue
#                    if ("HP" in key or "MP" in key) and '%' in nvalue:
#                        ItemDict["Perc " + key] = nvalue.strip('%')
#                    else:
#                        ItemDict[key] = CF.replacen(nvalue,",")
#                except:
#                    continue
#            if "Equipment Set" in ItemDict.keys():
#                #Set = ItemDict['Equipment Set']
#                #Set = replacen(Set, 'Set')
#
#                #ClassFound = [i for i, j in self.rename["ClassTypes"].items() if list(set(Set.lower().split(' ')) &set(j)) != []]
#                #if ClassFound != []:
#                #    Set = Set.replace(ClassFound[0], '')
#                #ItemDict['EquipSet'] = Set.rstrip(' ')
#                
#                ItemDict['EquipSet'] = ItemDict.pop("Equipment Set")
#                
#                       
#            self.RecordItemDict(ItemDict=ItemDict)
#            yield ItemDict
    
    def RetrieveByTDContent(self, content, ItemDict = {}):
        
        for stat in content:
            if stat.find(':') != -1:
                key, value = stat.split(':')
                if value.strip(' ') == "":
                    continue
                ItemDict[key] = CF.replacen(value.strip(' +\n'), ",")
            else:
                try:                   
                    key, value = stat.split(' ')
                    ItemDict[key] = CF.replacen(value.rstrip(' \n'),",")
                except:
                    continue
        return ItemDict
    
    def CatID(self, EquipSlot):
        if EquipSlot in self.ArmorEquip:
            return "EquipArmor"
        elif EquipSlot in self.AccessoriesEquip:
            return "EquipAccessory"
        elif EquipSlot == "Shield":
            return "EquipSecondary"
        return "Equip" + EquipSlot

    def RecordItemDict(self, ItemDict):
        #Save to respective pd.DataFrames
        try:
            EquipSlot = ItemDict['EquipSlot']
            if EquipSlot is not None:
                #df = pd.DataFrame(ItemDict, index=[0])
                #ID = self.CatID(EquipSlot)
                ID = ItemDict['Destination']
                #self.FinalEquipTrack[ID] = self.FinalEquipTrack[ID].append(df, ignore_index=True)
                self.ItemCount[ID].update(1)

                Eqlogger.info("Adding {0}".format(ItemDict['EquipName']))
                self.ItemCount[ID].refresh()
        except Exception:
            Eqlogger.critical(traceback.format_exc())

    



class EquipmentSetSpider(scrapy.Spider):
    name = "EquipmentSetData"
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ['https://maplestory.fandom.com/wiki/Equipment']
    custom_settings = {
        "LOG_SCRAPED_ITEMS": False,
        "ITEM_PIPELINES" : {
            #'scrapysea.pipelines.ScrapyseaPipeline': 300,
            #'scrapysea.pipelines.MultiCSVItemPipeline': 300,
            'scrapysea.pipelines.SqliteDBItemPipeline': 100
        }
    }
    TrackEquipSets = {
        "EquipSetEffectAt" : [],
        "EquipSetEffectCul" : []
    }
    
    EquipSetTrack = [
        'Sengoku', 'Boss Accessory','Dawn Boss', 'Pitched Boss', 'Seven Days', "Ifia's Treasure",'Mystic','Ardentmill','Blazing Sun'
        ,'8th', 'Root Abyss','AbsoLab', 'Arcane', "Eternal"
        ,'Lionheart', 'Dragon Tail','Falcon Wing','Raven Horn','Shark Tooth'
        ,'Gold Parts','Pure Gold Parts'
    ]
    
    ignoreSet = [
        'Immortal' ,'Walker','Anniversary', "Eternal Hero"
        "Sweetwater", "Commerci", "Gollux", "Alien", "Blackgate", "Glona"]
    

    def parse(self, response):

        EquipmentSet =response.xpath("//span[@id = 'Equipment_Set']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1] //table[@class='wikitable']")
        ClassHeaders = response.xpath("//span[@id = 'Equipment_Set']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1] //li[contains(@class,'wds-tabs__tab')] //text()").getall()
        TempL = []
        for ix, tables in enumerate(EquipmentSet):
            ClassType = "Any" if ClassHeaders[ix] == "Common" else ClassHeaders[ix]
            for equipLinks in tables.xpath(".//a[not(descendant::img)]/@href"):
                setName = " ".join(re.split("%|_|-|#|27s", equipLinks.extract().split("/")[-1])).replace("Set", '').replace(ClassType, '').rstrip()
                TempL.append(setName)
                if CF.instring(setName, '('):
                    setName = setName.split('(')[0].rstrip()
                if any(value in setName for value in self.EquipSetTrack):
                    if any(value in setName for value in self.ignoreSet):
                        continue
                    nurl = response.urljoin(equipLinks.extract())
                    
                    yield scrapy.Request(nurl, callback=self.HandleEquipmentSet, meta = {"EquipSet" : setName, "ClassType":ClassType})
        pass

    def close(self):
        SetDF = pd.concat(self.TrackEquipSets["Set"], ignore_index=True)
        SetDF = CleanSetEffect(SetDF)
        CulDF = pd.concat(self.TrackEquipSets['Cumulative'], ignore_index=True)
        CulDF = CleanSetEffect(CulDF)
        
        SetDF.to_csv(CF.APPFOLDER + "EquipmentData\\EquipSetData.csv")
        CulDF.to_csv(CF.APPFOLDER + "EquipmentData\\EquipSetCulData.csv")

        Eslogger.info("Scraped {0} Set Effects".format(len(SetDF)))
        Eslogger.info("Scraped {0} Culmulative Set Effects".format(len(CulDF)))

        CF.TimeTaken(self)

    def HandleEquipmentSet(self, response):

        Wikitable = response.xpath("//div[@class='mw-parser-output'] //table[@class='wikitable'][1]")
        EquipSet = response.meta["EquipSet"]
        ClassType = response.meta['ClassType']
        TRRows = Wikitable.xpath(".//tr")
        Header =  CF.removebr(TRRows.xpath(".//th /text()").getall())
        try:
            for i, SetType in enumerate(Header):
                
                if "Set" in SetType:
                    SetType = "EquipSetEffectAt"
                if "Cumulative" in SetType:
                    SetType = "EquipSetEffectCul"
                for row in TRRows: 
                    
                    SetAt = row.xpath(".//td /b/text()").getall()
                    if SetAt == []:
                        continue
                    else:
                        SetAt = SetAt[0].split(' ')[0]
                    ItemDict = {
                        "EquipSet" : EquipSet,
                        "ClassType" : ClassType,
                        "Set At" : SetAt
                    }
                    l = ESLoader(item = EquipSetItem())
                    l.add_value("EquipSet", EquipSet)
                    l.add_value("ClassType", ClassType)
                    l.add_value("SetAt", SetAt)
                    l.add_value("Destination", SetType)

                    SetEffectAt  = CF.removebr(row.xpath(".//td[{0}] /text()".format(i+1)).getall())
                    self.TrackEquipSets[SetType].append(pd.DataFrame(self.RetrieveByTDContent(SetEffectAt, ItemDict), index=[0]))
                    yield l.load_item()
        except:
            Eslogger.critical(traceback.format_exc())
        Eslogger.info("Adding {0}".format(EquipSet))
        #return ItemDict


    def RetrieveByTDContent(self, content, ItemDict = {}):
        
        for stat in content:
            if CF.instring(stat, ["Max", "%"], mode="All"):
                key, value = stat.split(':')
                ItemDict["Perc " + key] = value.strip(' +%\n')
                continue
            if CF.instring(stat, ":"):
                key, value = stat.split(':')
                if CF.instring(value, "("):
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



#region Cleanup

def CleanWeaponDF(CDF, ColumnOrder):
    #CDF.drop(['Equipment Set'], axis = 1, inplace = True)
    #ColumnOrder = [
    #    "EquipSlot","EquipSet","EquipName","WeaponType",
    #    "Level","STR","DEX","INT", "LUK","Max HP","Defense",
    #    "Weapon Attack","Magic Attack","Attack Speed","Boss Damage","Ignored Enemy Defense",
    #    "Movement Speed","Knockback Chance", "Number of Upgrades"
    #]
    CDF = CDF[ColumnOrder]
    #CDF = CDF.rename(columns={
    #    'Level' : 'EquipLevel'
    #})
    CDF.loc[CDF['EquipSet'].isnull(), "EquipSet"] = "None"    

    CDF = CDF.fillna(0)
    return CDF

def CleanSecondaryDF(CDF):
    ColumnOrder =[
        "EquipSlot","ClassName","EquipName","WeaponType",
        "Level", "STR","DEX","INT", "LUK","All Stats","Max HP","Max MP","Defense",
        "Weapon Attack","Magic Attack","Attack Speed",
        "Max DF"     
    ]
    print(CDF.head(5))
    CDF = CDF[ColumnOrder]
    #CDF = CDF.rename(columns={
    #    "Level" : "EquipLevel"
    #})
    CDF.loc[CDF["WeaponType"] == 'Arrowhead', 'WeaponType'] = "Arrow Head"
    CDF.loc[CDF["WeaponType"] == 'Bladebinder', 'WeaponType'] = "Bracelet"
    CDF.loc[CDF['WeaponType'].isnull(), "WeaponType"] = CDF["EquipSlot"]
    CDF = CDF.fillna(0)
    return CDF

def CleanArmorDF(CDF):
    CDF.drop("Category", axis=1, inplace=True)
    ColumnOrder = [
        "EquipSlot","ClassName","EquipName","EquipSet",
        "Level","STR","DEX","INT","LUK","Max HP","Max MP","Defense",
        "Weapon Attack","Magic Attack", "Ignored Enemy Defense",
        "Movement Speed","Jump", 
        "Number of Upgrades"
        ]
    CDF = CDF[ColumnOrder]
    
    CDF = CDF.rename(columns={
        "Level" : "EquipLevel",
        "ClassName" : "ClassType"
    })

    CDF['EquipSet'] = CDF['EquipSet'].fillna('None')
    CDF.loc[CDF['Number of Upgrades'] == "None", "Number of Upgrades"] = 0
    #CDF['EquipSet'].fillna('None', inplace =True)
    CDF = CDF.fillna(0)

    return CDF

def CleanAccessoryDF(CDF):
    
    ColumnOrder = [
    "EquipSlot","ClassName","EquipName","Equipment Set",
    "Category",
    "Level","STR","DEX","INT","LUK","Max HP","Max MP","Perc Max HP", "Perc Max MP","Defense",
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

    CDF.drop_duplicates(keep="first", inplace=True)    

    CDF.loc[CDF['Job'].isnull(), "Job"] = "Any"    
    CDF.loc[CDF["ClassName"].isnull(), "ClassName"] = CDF['Job']
    CDF.drop("Job", axis=1, inplace=True)
    CDF.loc[CDF['Number of Upgrades'] == "None", "Number of Upgrades"] = 0

    CDF.loc[CDF['Category'].isnull(), "Category"] = "Obtainable"    
    CDF.loc[CDF['EquipSet'].isnull(), "EquipSet"] = "None"    


    CDF = CDF.fillna(0) 

    return CDF

def CleanAndroidDF(CDF):

    CDF = CDF.rename(columns={
        "Level" :"EquipLevel"
    }) 
    CDF.drop_duplicates(keep="first", inplace=True)
    return CDF

def CleanMedalDF(CDF):

    CDF.drop(["Number of Upgrades","Equipment Set"], axis=1, inplace=True)
    ColumnOrder = [
        "EquipSlot","ClassName","EquipName","EquipSet",
        "Category",
        "Level","STR","DEX","INT","LUK","Max HP","Max MP","Defense",
        "Weapon Attack","Magic Attack","Ignored Enemy Defense","Boss Damage",
        "Movement Speed","Jump"
    ]

    CDF = CDF[ColumnOrder]
    CDF = CDF.rename(columns ={
        "Level" : "EquipLevel"
    })

    CDF.loc[CDF['ClassName'].isnull(),"ClassName"] = "Any"
    CDF.loc[CDF['EquipSet'].isnull(),"EquipSet"] = "None"
    CDF.loc[CDF['Category'] == "Uncategorized", "Category"] = "Obtainable"
    CDF = CDF.fillna(0) 


    return CDF

def CleanSetEffect(CDF):
    try:
        ColumnOrder = [
            "EquipSet","ClassType","Set At",
            "STR","DEX","INT","LUK","All Stats","Max HP","Max MP","Perc Max HP","Perc Max MP","Defense",
            "Weapon Attack","Magic Attack","Ignored Enemy Defense","Boss Damage",
            "Critical Damage", "Damage","All Skills","Damage Against Normal Monsters","Abnormal Status Resistance"
        ]

        
        CDF = CDF[ColumnOrder]
        
        CDF = CDF.fillna(0) 
    except:
        Eslogger.critical(traceback.format_exc())

    return CDF

#endregion