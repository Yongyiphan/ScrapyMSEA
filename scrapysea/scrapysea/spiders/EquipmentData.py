
import CustomLogger
import scrapy
import re



Eqlogger = CustomLogger.Set_Custom_Logger("EquipmentAllSpider", logTo ="./Logs/Equipment.log", propagate=False)
Eslogger = CustomLogger.Set_Custom_Logger("EquipmentSetSpider", logTo ="./Logs/EquipmentSet.log", propagate=False)
#configure_logging(install_root_handler=False)

class TotalEquipmentSpider(scrapy.Spider):
    name = "TotalEquipmentData"
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ['https://maplestory.fandom.com/wiki/Equipment']
    custom_settings = {
        "FEEDS":
            {
                "./DefaultData/EquipmentData.json":{
                    "format":"json", 
                    "overwrite" : True
                }
            },
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
        
    EquipLevelMin = {
        "Hat" : 120, "Top" : 150, "Overall" : 120, "Bottom" :150, "Shoes" :120, "Cape" : 120, "Gloves" :120, 
        "Shoulder" :100, 
        "Face Accessory" : 100,
        "Eye Decoration" : 100, 
        "Ring" : 30, "Pendant" : 75, 
        "Earrings" : 75, 
        "Belt" : 100, 
        "Pocket Item" : 0, 
        "Android": 0,"Android Heart":30,
        "Emblem" : 100, "Badge": 100, "Medal": 0
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
        "Sweetwater", "Commerci", "Gollux", "Alien", "Blackgate", "Glona"]
    
    PageContentSkip = [
        "Extra Stats", "Notes", "Sold for", 
        "Tradability", "Bought from", "Dropped by",
        "Rewarded from", "Used In", "Used To"
        "when first equipped", "Crafted via", "EXP"]
    
    
    
    def parse(self, response):

        ArmorT = response.xpath("//span[@id = 'Armor']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1]").css('table.maplestyle-equiptable')[0]
        for Slot in ArmorT.xpath(".//a[not(descendant::img)]/@href"):
            Link = Slot.extract()
            if Link.find("Totem")  !=  -1:
                continue
            nurl = response.urljoin(Link)
            yield scrapy.Request(nurl, callback=self.HandleEquipment)
        
        weaponT = response.xpath("//span[@id = 'Weapon']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1]")
        for weaponLinks in weaponT.css("a::attr(href)"):
            weaponType = " ".join(re.split('_|-', weaponLinks.extract().split('/')[-1]))
            nurl = response.urljoin(weaponLinks.extract())
            yield scrapy.Request(nurl, callback=self.HandleWeapon, meta={"WeaponType":weaponType})
        
        secondaryT = response.xpath("//span[@id = 'Secondary_Weapon']/parent::h3/following-sibling::table[1]")
        for secondaryLinks  in secondaryT.css("a::attr(href)"):
            weaponType = " ".join(re.split('_|-', secondaryLinks.extract().split('/')[-1]))
            nurl = response.urljoin(secondaryLinks.extract())
            
            if weaponType == "Shield":
               yield scrapy.Request(nurl, callback=self.HandleShield, meta = {"Class" : "Any"})
            else:
                yield scrapy.Request(nurl, callback=self.HandleSecondary, meta={"WeaponType":weaponType})
       
    #Implemented
    def HandleWeapon(self, response):
        weaponTablesLinks = response.xpath("//div[@class='wds-tab__content wds-is-current']").css('b > a::attr(href)')
        for links in weaponTablesLinks:
            weaponName = " ".join(re.split("%|_|-|#|27s", links.extract().split("/")[-1]))
            if weaponName.find('Sealed') != -1:
                continue
            setWName = set(weaponName.split(' '))
            setTrack = set(self.WeapSetTrack)
            commonStr = list(setWName&setTrack)
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
   
    #Implemented
    def HandleSecondary(self, response):
        header = response.css('div.mw-parser-output p')
        classType = []
        CStart = False
        WeaponType = response.meta["WeaponType"]
        for i in header:
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
        
        tables = response.css('div.mw-parser-output table.wikitable')
        

        if WeaponType == "Demon Aegis":
            classType.append("Demon")

        for i, C in enumerate(classType):
            if C.lower() in list(map(lambda x:x.lower(), self.NonMseaClasses)):
                break
            for row in tables[i].css('tr'):
                
                td = [value.strip('\n') for value in row.css('td::text').getall() if value.strip(' ') != '\n']
                tda = [value.strip('\n') for value in row.css('::text').getall() if value.strip(' ') != '\n']
                if tda != []:
                    if (any(value in tda[0].lower() for value in self.ignoreSecondary) 
                        or tda[0].lower().find('picture') != -1):
                        continue
                    if WeaponType == "Katara":
                        tds = set(tda[0].split(' '))
                        W = set(self.WeapSetTrack)
                        CommonSet = list(tds&W)
                        if  CommonSet == []:
                            continue
                    if tda[0].lower().find('equipment') != -1:
                        break       
                    
                    ItemDict = {
                        "EquipSlot" : "Secondary",
                        "ClassType" : C,
                        "WeaponType" : WeaponType,
                        "EquipName" : tda[0]
                        
                    }
                    yield self.RetrieveByTDContent(td, ItemDict)
    #Implemented    
    def HandleShield(self, response):
        if not response:
            return
        classType = response.meta["Class"]
        tabsTitle = response.xpath("//div[@class='tabber wds-tabber'] //ul[@class='wds-tabs'] //li").css("::text").getall()
        Unobtainable =[i for i, j in enumerate(tabsTitle) if "Unobtainable" in j]
        tables = response.css('div.wds-is-current table.wikitable')
        tables = response.xpath("//div[contains(@class, 'wds-tab__content')] //table[@class='wikitable']")
        if Unobtainable != []:
            tables.pop(Unobtainable[0])
            tabsTitle.pop(Unobtainable[0])
        try:
            altTable = tables.xpath("./ancestor::div //a[contains(@title, '{}/')]/@href".format("Shield"))
            for altLink in altTable:
                ct = altLink.extract().split('/')[-1]
                nurl = response.urljoin(altLink.extract())
                yield scrapy.Request(nurl, callback=self.HandleShield, meta={"Class" : ct})
                
        except Exception as E:
            print(E)
            pass

        for row in tables.css('tr'):
            td = [value for value in row.css('td::text').getall() if value.strip(' ') != '\n']
            tda = [value for value in row.css('::text').getall() if value.strip(' ') != '\n']
            print(td)
            if tda != []:
                if (any(value in tda[0].lower() for value in self.ignoreSecondary) 
                    or tda[0].lower().find('picture') != -1):
                    continue
                if tda[0].lower().find('equipment') != -1:
                    break       
                
                ItemDict = {
                    "EquipSlot" : "Secondary",
                    "ClassType" : classType,
                    "WeaponType" : "Shield",
                    "EquipName" : tda[0]
                    
                }
                yield self.RetrieveByTDContent(td, ItemDict)


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
    
        
        for i, table in enumerate(Wikitable):
            Category = tabsTitle[i] if tabsTitle != [] else ""
            for row in table.xpath(".//tr"):
                
                link =  row.xpath(".//a[not(contains(@href,'png'))]/@href").get()
                td = [value.strip('\n') for value in row.xpath(".//text()").extract() if value != '\n']
                EquipName = td[0]
                if EquipName.find("Picture") != -1 or not EquipName.isascii():
                    continue

                LevelT = [value for value in td[1:] if "Level" in value or "None" in value]
                clvl = 0 if "None" in LevelT[0] else int(LevelT[0].split(" ")[1])
                if EquipSlot in self.EquipLevelMin.keys():
                    if clvl < self.EquipLevelMin[EquipSlot]:
                        continue

                if link == None:
                    Content = self.removeBRN(row.xpath(".//td[3] /text()").getall()) 
                    ItemDict = {
                        "EquipSlot" :EquipSlot,
                        "EquipName" : EquipName,
                        "Grouping" : Category,
                        "Level" : clvl 
                    }
                    yield self.RetrieveByTDContent(Content, ItemDict)
                else:
                    
                    if EquipSlot in self.EquipLevelMin.keys():
                        if clvl < self.EquipLevelMin[EquipSlot]:
                            continue
                    nurl = response.urljoin(link)
                    
                    if any(value in EquipName for value in self.ignoreSet):
                        continue
                    ItemDict = {
                        "EquipSlot" : EquipSlot,
                        "EquipName" : EquipName,
                        "Grouping" : Category
                    }
                    yield scrapy.Request(nurl, callback=self.RetrieveByPage, cb_kwargs={"ItemDict": ItemDict})
    
    
    def RetrieveByPage(self, response, ItemDict):
        if not response:
            return
        PageTitle = response.xpath("//h1[@class='page-header__title']/text()").get()
        TableTrack = 1
        AlternateTablesTitles = response.xpath("//div[@class = 'toc'] //ul/li[contains(@class, 'toclevel-2')]/a /span[@class='toctext']/text()").getall()
            
        TableContent = response.css("div.mw-parser-output table")
        if PageTitle.find("Genesis") != -1:
            TableContent = response.xpath("//h2/span[@id = 'Unsealed']/parent::h2/following-sibling::table")
        td = []
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
            for t in ctable.xpath('.//tr'):
                texts = t.css("td ::text").getall()
                if texts == ['\n']:
                    continue
                td.append("".join(texts))
            td = ["".join(value.css("td ::text").getall()).strip('\n') for value in ctable.css('tr') if value.css("td ::text").getall() != ['\n']]
            th = [value.strip('\n') for value in ctable.css('tr > th ::text').getall() if value != '\n']
            ItemDict["EquipName"] = td[0].rstrip(' \n')
            for key, value in zip(th, td[1:]):
                
                if any(value.lower() in key.lower() for value in self.PageContentSkip):
                    continue
                
                try:
                    ItemDict[key] = value.strip('+%')
                except:
                    continue
            
            Eqlogger.info(f"Adding {ItemDict['EquipName']}")
            return ItemDict
    
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
        if "EquipName" in ItemDict.keys():
            Eqlogger.info(f"Adding {ItemDict['EquipName']}")
        return ItemDict

    def removeBRN(self, List):
        return [value.strip('\n') for value in List if value.strip(' ') != '\n']

class EquipmentSetSpider(scrapy.Spider):
    name = "EquipmentSetData"
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ['https://maplestory.fandom.com/wiki/Equipment']
    custom_settings = {
        "FEEDS":
            {
                "./DefaultData/EquipmentSetData.json":{
                    "format":"json", 
                    "overwrite" : True
                }
            },
        "LOG_SCRAPED_ITEMS": False
        
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

        EquipmentSet =response.xpath("//span[@id = 'Equipment_Set']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1]").css("table.wikitable")

        for equipLinks in EquipmentSet.xpath(".//a[not(descendant::img)]/@href"):
            setName = " ".join(re.split("%|_|-|#|27s", equipLinks.extract().split("/")[-1])).replace("Set", '').rstrip(" ")
            if any(value in setName for value in self.EquipSetTrack):
                if any(value in setName for value in self.ignoreSet):
                    continue
                nurl = response.urljoin(equipLinks.extract())
                yield scrapy.Request(nurl, callback=self.HandleEquipmentSet, meta = {"EquipSet" : setName})
            
                    
        print("Donezo")
        pass

    def HandleEquipmentSet(self, response):

        Wikitable = response.xpath("//div[@class='mw-parser-output'] //table[@class='wikitable'][1]")
        EquipSet = response.meta["EquipSet"]
        ItemDict = {
            "EquipSet" : EquipSet,
        }
        TRRows = Wikitable.xpath(".//tr")
        Header =  self.removeBRN(TRRows.xpath(".//th /text()").getall())
        for i, SetType in enumerate(Header):
            for row in TRRows: 
                SetAt = row.xpath(".//td[1] /b/text()").getall()
                if SetAt == []:
                    continue
                else:
                    SetAt = SetAt[0].split(' ')[0]
                SetEffectAt  = row.xpath(f".//td[{i+1}] /text()").getall()
                if SetAt not in ItemDict.keys():
                    ItemDict[SetAt] = {}
                ItemDict[SetAt][SetType] = self.RetrieveByTDContent(SetEffectAt)
        
        Eslogger.info(f"Adding {EquipSet}")
        return ItemDict


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

    def removeBRN(self, List):
        return [value.strip('\n') for value in List if value.strip(' ') != '\n']

