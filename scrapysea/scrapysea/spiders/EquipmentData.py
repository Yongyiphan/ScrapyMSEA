
import logging
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import re
import time
EquipmentURLS = [
    "https://maplestory.fandom.com/wiki/Category:Secondary_Weapons",
        "http://maplestory.fandom.com/wiki/Category:Equipment_Sets",
        "http://maplestory.fandom.com/wiki/Sealed_Genesis_Weapon_Box",
        "http://maplestory.fandom.com/wiki/Category:Superior_Equipment",
        "http://maplestory.fandom.com/wiki/Android_Heart"
]
EquipSetTrack = [
    'Sengoku', 'Boss Accessory', 'Pitched Boss', 'Seven Days', "Ifia's Treasure",'Mystic','Ardentmill','Blazing Sun'
    ,'8th'
    ,'Lionheart', 'Dragon Tail','Falcon Wing','Raven Horn','Shark Tooth'
    ,'Root Abyss'
    ,'AbsoLab'
    , 'Arcane'
    ,'Gold Parts','Pure Gold Parts'
    ]
WeapSetTrack = [
    'Utgard', 'Lapis','Lazuli'
    ,'Fafnir','AbsoLab', 'Arcane', 'Genesis'
    ]

NonMseaClasses = ["Jett", "Beast Tamer"]

Mclasses = ['Warrior','Knight', 'Bowman', 'Archer', 'Magician','Mage','Thief', 'Pirate']

class WeaponDataSpider(scrapy.Spider):
    name = 'WeaponData'
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ["http://maplestory.fandom.com/wiki/Category:Weapons"]
    custom_settings = {
        "FEEDS":
            {"WeaponData.json":{"format":"json", "overwrite" : True}}
    }

    

    def parse(self, response):
        #for href in response.xpath('//li[@class="category-page__member"]/a/@href').extract():
        for href in response.css('a.category-page__member-link::attr(href)'):
            wtype = " ".join(re.split('_|-' , href.extract().split(':')[-1])).strip(' ')
            url = response.urljoin(href.extract())
            yield scrapy.Request(url, callback=self.WeaponType, meta = {"WeaponType" : wtype})
            ...
        
        
    def WeaponType(self, response):
        
        for href in response.css('a.category-page__member-link::attr(href)'):
            ItemDict = {
                "WeaponType" : response.meta["WeaponType"],
                "EquipSlot" : "Weapon"
                
            }
            ItemDict["WeaponType"] = response.meta.get('WeaponType')
            hrefcat = re.split('/|_|%| ', href.extract())
            match = False
            for wtrack in WeapSetTrack:
                if wtrack in hrefcat:
                    ItemDict["Set"] = wtrack
                    match = True
            if match:
                url = response.urljoin(href.extract())
                yield scrapy.Request(url, callback=self.IndividualWeapon, cb_kwargs= {'item' : ItemDict})


    
    def IndividualWeapon(self, response, item):
        tables = response.css('div.mw-parser-output > table > tbody > tr')
        for row in tables:
            th = row.xpath('th//text()').getall()
            td = row.xpath('td//text()').getall()
            if "Tradability\n" in th:
                break
            if "Extra Stats\n" in th:
                continue
            if th != []:
                key = th[0].strip('\n ')
                value = "|".join(td).strip("|\n+%")
                item[key] = value
        return item 
    

class SecondaryData(scrapy.Spider):
    name = "SecondaryData"
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ["https://maplestory.fandom.com/wiki/Category:Secondary_Weapons"]
    custom_settings = {
        "FEEDS":
            {"SecondaryData.json":{"format":"json", "overwrite" : True}}
    }
    ignoreList = ["evolving", "frozen"]
    
    def parse(self, response):
        links = response.css('a.category-page__member-link::attr(href)')
        for href in links:
            #url = response.urljoin(href.extract())
            url = response.urljoin(href.extract())
            yield scrapy.Request(url, callback=self.redirectMainArticle)
        ...
        
    def redirectMainArticle(self, response):
        mainArticleUrl = response.css('div.mw-parser-output a::attr(href)').get()
        wtype = " ".join(mainArticleUrl.split('/')[-1].split('_'))
        url = response.urljoin(mainArticleUrl)
        if wtype == "Shield":
            for classes in Mclasses: 
                Murl = url + '/' + classes
                yield scrapy.Request(Murl, callback=self.retrieveShield, meta={"Class" : classes})
        else:
            yield scrapy.Request(url, callback=self.retrieveSecondaryWeapons, meta = {"WeaponType":wtype})
        
        ...
        
    def retrieveSecondaryWeapons(self, response):
        header = response.css('div.mw-parser-output p')
        classType = []
        CStart = False
        for i in header:
            
            texts = i.xpath('//text()').getall()
            linktext = i.xpath('a//text()').getall()
            for s in texts:
                if s.find('exclusive') != -1:
                    CStart = True
                    continue
                if s.find("conjunction") != -1:
                    CStart = False
                    break
                if CStart and s.strip(' ') != '\n':
                    if s not in classType and s in linktext:
                        classType.append(s)
        
        tables = response.css('div.mw-parser-output table.wikitable')
        WeaponType = response.meta["WeaponType"]
        if WeaponType == "Demon Aegis":
            classType.append("Demon")
        for i, C in enumerate(classType):
            if C.lower() in list(map(lambda x:x.lower(), NonMseaClasses)):
                break
            for row in tables[i].css('tr'):
                
                td = [value for value in row.css('td::text').getall() if value.strip(' ') != '\n']
                tda = [value for value in row.css('::text').getall() if value.strip(' ') != '\n']
                if tda != []:
                    if (any(value in tda[0].lower() for value in self.ignoreList) 
                        or tda[0].lower().find('picture') != -1):
                        continue
                    if WeaponType == "Katara":
                        tds = set(tda[0].split(' '))
                        W = set(WeapSetTrack)
                        CommonSet = list(tds&W)
                        if  CommonSet == []:
                            continue
                    if tda[0].lower().find('equipment') != -1:
                        break       
                    
                    ItemDict = {
                        "ClassType" : C,
                        "WeaponType" : WeaponType,
                        "EquipName" : tda[0],
                        "EquipSlot" : "Secondary"
                    }
                    yield self.ReturnItemDict(ItemDict, td)
                
    def retrieveShield(self, response):
        if not response:
            return
        classType = response.meta["Class"]
        tables = response.css('div.wds-is-current table.wikitable')
        
        for row in tables.css('tr'):
            td = [value for value in row.css('td::text').getall() if value.strip(' ') != '\n']
            tda = [value for value in row.css('::text').getall() if value.strip(' ') != '\n']
            print(td)
            if tda != []:
                if (any(value in tda[0].lower() for value in self.ignoreList) 
                    or tda[0].lower().find('picture') != -1):
                    continue
                if tda[0].lower().find('equipment') != -1:
                    break       
                
                ItemDict = {
                    "ClassType" : classType,
                    "WeaponType" : "Shield",
                    "EquipName" : tda[0],
                    "EquipSlot" : "Secondary"
                }
                yield self.ReturnItemDict(ItemDict, td)

    
    
    
    def ReturnItemDict(self,ItemDict, content):
        for stat in content:
            if stat.find(':') != -1:
                key, value = stat.split(':')
                if value.strip(' \n') == "":
                    continue
                ItemDict[key] = value.strip(' \n')
            else:
                try:                   
                    key, value = stat.split(' ')
                    ItemDict[key] = value.strip(' \n')
                except:
                    continue
        
        return ItemDict
                
                
            
        ...
        
class EquipmentData(scrapy.Spider):
    
    name = 'EquipmentData'
    allowed_domains = ['maplestory.fandom.com']
    #start_urls = [
    #    "http://maplestory.fandom.com/wiki/Category:Weapons",
    #    "http://maplestory.fandom.com/wiki/Category:Superior_Equipment",
    #    "http://maplestory.fandom.com/wiki/Android_Heart"
    #    ]
    custom_settings = {
        "FEEDS":
            {"Results.json":{"format":"json", "overwrite" : True}}
    }
    
    def start_requests(self):
        yield scrapy.Request("http://maplestory.fandom.com/wiki/Category:Equipment_Sets", self.HandleEquipmentSet)
        yield scrapy.Request("http://maplestory.fandom.com/wiki/Category:Superior_Equipment", callback=self.HandleSuperiorEquipment)
        yield scrapy.Request("http://maplestory.fandom.com/wiki/Android_Heart", callback=self.HandleAndroidHeart)
        
    def HandleEquipmentSet(self, response):
        #Filters Sets to track
        div = response.css('div.')
        pass
    
    def HandleSuperiorEquipment(self, response):
        divs = [value for value in response.css('div.category-page__members-wrapper') if value.xpath('div[@class = "category-page__first-char"]/text()').get().strip('\n\t') == "T"][0].css('ul')
        hrefs = divs.css('li a::attr(href)').getall()
        for url in hrefs:
            Nurl = response.urljoin(url)
            yield scrapy.Request(Nurl, callback=self.RetrieveTyrant)    
        
    
    def RetrieveTyrant(self, response):
        table = response.css('div.mw-parser-output tbody tr')
        title = table.css('big b ::text').get().rstrip(' ').split(' ')
        ItemDict = {
            "DataSet" : "Equipment",
            "EquipSlot" : title[-1].strip(' '),
            "EquipSet" : "Tyrant",
            "EquipName" : title[1]
        }
        
        td = [value for value in table.css('td ::text').getall() if value != '\n']
        th = [value for value in table.css('th ::text').getall() if value != '\n']
        for key, value in zip(th, td[1:]):
            if key.strip('\n') == "Tradability":
                break
            try:
                ItemDict[key.strip('\n')] = value.strip('+%\n')
            except:
                continue
            
        yield ItemDict
    
    def HandleAndroidHeart(self, response):
        tables = response.css('table.wikitable')
        for row in tables.css('tr'): 
            tda = [value for value in row.css('::text').getall() if value.strip(' ') != '\n']
            if tda[0].lower().find('picture') != -1:
                continue       
            ItemDict = {
                "EquipName" : tda[0].strip('\n').replace('-', ' '),
                "EquipSlot" : "Heart",
                "ClassType" : "Any" 
            } 
            yield self.ReturnItemDict(ItemDict, tda[1:])
    
        pass
    
    def ReturnItemDict(self,ItemDict, content):
        for stat in content:
            if stat.find(':') != -1:
                key, value = stat.split(':')
                if value.strip(' \n') == "":
                    continue
                ItemDict[key] = value.strip(' \n')
            else:
                try:                   
                    key, value = stat.split(' ')
                    ItemDict[key] = value.strip(' \n')
                except:
                    continue
        
        return ItemDict
        

class TotalEquipmentData(scrapy.Spider):
    name = "TotalEquipmentData"
    allowed_domains = ['maplestory.fandom.com']
    start_urls = ['https://maplestory.fandom.com/wiki/Equipment']
    custom_settings = {
        "FEEDS":
            {"Results.json":{"format":"json", "overwrite" : True}}
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

    NonMseaClasses = ["Jett", "Beast Tamer"]

    Mclasses = ['Warrior','Knight', 'Bowman', 'Archer', 'Magician','Mage','Thief', 'Pirate']
    
    ignoreSecondary = ["evolving", "frozen"]
    
    ignoreSet = [
        'Immortal', 'Eternal','Walker','Anniversary', 
        "Sweetwater", "Commerci", "Gollux", "Alien", "Blackgate", "Glona"]
    
    PageContentSkip = [
        "Extra Stats", "Notes", "Sold for", 
        "Tradability", "Bought from", "Dropped by",
        "Rewarded from", "Used In",
        "when first equipped", "Crafted via"]
    
    EquipLevelMin = {
        "Hat" : 120,
        "Top" : 150, 
        "Overall" : 120, 
        "Bottom" :150, 
        "Shoes" :120, 
        "Cape" : 120, 
        "Gloves" :120, 
        "Shoulder" :100, 
        "Ring" : 30, 
        "Pendant" : 75, 
        "Earrings" : 75, 
        "Belt" : 100, 
        "Pocket Item" : 0,
        "Android": 0,
        "Android Heart":30,
        "Emblem" : 100,
        "Badge": 100,
        "Medal": 100
    }
    
    #Implemented Method to Retrieve:
    #All Weapons as per WeapSetTrack
    #All Secondary Including Shield
    
    def parse(self, response):
        ArmorT = response.xpath("//span[@id = 'Armor']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1]").css('table.maplestyle-equiptable')[0]
        #for Slot in ArmorT.xpath(".//a[not(descendant::img)]/@href"):
        #    Link = Slot.extract()
        #    if Link.find("Totem")  !=  -1:
        #        continue
        #    nurl = response.urljoin(Link)
        #    yield scrapy.Request(nurl, callback=self.HandleEquipment)
        
        Links = ArmorT.xpath(".//a[not(descendant::img)]/@href").getall()
        nurl = response.urljoin(Links[9])
        print(nurl)
        yield scrapy.Request(nurl, callback=self.HandleEquipment)
        
        #weaponT = response.xpath("//span[@id = 'Weapon']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1]")
        #for weaponLinks in weaponT.css("a::attr(href)"):
        #    weaponType = " ".join(re.split('_|-', weaponLinks.extract().split('/')[-1]))
        #    nurl = response.urljoin(weaponLinks.extract())
        #    yield scrapy.Request(nurl, callback=self.HandleWeapon, meta={"WeaponType":weaponType})
        #secondaryT = response.xpath("//span[@id = 'Secondary_Weapon']/parent::h3/following-sibling::table[1]")
        #for secondaryLinks  in secondaryT.css("a::attr(href)"):
        #    weaponType = " ".join(re.split('_|-', secondaryLinks.extract().split('/')[-1]))
        #    nurl = response.urljoin(secondaryLinks.extract())
        #    
        #    if weaponType == "Shield":
        #        for classes in Mclasses: 
        #            Murl = nurl + '/' + classes
        #            yield scrapy.Request(Murl, callback=self.HandleShield, meta={"Class": classes})
        #    else:
        #        yield scrapy.Request(nurl, callback=self.HandleSecondary, meta={"WeaponType":weaponType})
        
       
        
        #EquipmentSet =response.xpath("//span[@id = 'Equipment_Set']/parent::h2/following-sibling::div[@class='tabber wds-tabber'][1]").css("table.wikitable")
        
        #TrackE = []
        #for equipLinks in EquipmentSet.xpath(".//a[not(descendant::img)]/@href"):
        #    setName = " ".join(re.split("%|_|-|#|27s", equipLinks.extract().split("/")[-1])).replace("Set", '').rstrip(" ")
        #    if any(value in setName for value in self.EquipSetTrack):
        #        if any(value in setName for value in self.ignoreSet):
        #            continue
        #        TrackE.append(equipLinks.extract())
        #        pass
        
            
            
            #ESet = " ".join(re.split('_|-', cLink.extract().split('/')[-1]))
        
        print("Donezo")
        pass
    
    #Implemented
    def HandleWeapon(self, response):
        weaponTablesLinks = response.xpath("//div[@class='wds-tab__content wds-is-current']").css('b > a::attr(href)')
        for links in weaponTablesLinks:
            weaponName = " ".join(re.split("%|_|-|#|27s", links.extract().split("/")[-1]))
            if weaponName.find('Sealed') != -1:
                continue
            setWName = set(weaponName.split(' '))
            setTrack = set(WeapSetTrack)
            commonStr = list(setWName&setTrack)
            if commonStr == []:
                continue
            Nurl = response.urljoin(links.extract())
            ItemDict = {
                "EquipName" : weaponName,
                "EquipSet" : commonStr[0],
                "EquipSlot" : "Weapon",
                "WeaponType" : response.meta['WeaponType']
            }
            yield scrapy.Request(Nurl, callback=self.RetrieveByPage, cb_kwargs = {"ItemDict" : ItemDict})
    
    
    #Implemented
    def HandleSecondary(self, response):
        header = response.css('div.mw-parser-output p')
        classType = []
        CStart = False
        for i in header:
            
            texts = i.xpath('//text()').getall()
            linktext = i.xpath('a//text()').getall()
            for s in texts:
                if s.find('exclusive') != -1:
                    CStart = True
                    continue
                if s.find("conjunction") != -1:
                    CStart = False
                    break
                if CStart and s.strip(' ') != '\n':
                    if "Shade" in s:
                        continue
                    if s not in classType and s in linktext:
                        classType.append(s)
        
        tables = response.css('div.mw-parser-output table.wikitable')
        WeaponType = response.meta["WeaponType"]
        if WeaponType == "Demon Aegis":
            classType.append("Demon")
            
        for i, C in enumerate(classType):
            if C.lower() in list(map(lambda x:x.lower(), NonMseaClasses)):
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
                        W = set(WeapSetTrack)
                        CommonSet = list(tds&W)
                        if  CommonSet == []:
                            continue
                    if tda[0].lower().find('equipment') != -1:
                        break       
                    
                    ItemDict = {
                        "ClassType" : C,
                        "WeaponType" : WeaponType,
                        "EquipName" : tda[0],
                        "EquipSlot" : "Secondary"
                    }
                    yield self.RetrieveByTD(ItemDict, td)
    #Implemented    
    def HandleShield(self, response):
        if not response:
            return
        classType = response.meta["Class"]
        tables = response.css('div.wds-is-current table.wikitable')
        
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
                    "ClassType" : classType,
                    "WeaponType" : "Shield",
                    "EquipName" : tda[0],
                    "EquipSlot" : "Secondary"
                }
                yield self.RetrieveByTD(ItemDict, td)

    
    def HandleEquipmentSet(self, response):
        pass
    
    def HandleEquipment(self, response):
        
        EquipSlot = response.xpath("//h1[@id = 'firstHeading']/text()").get().strip('\n\t').split('/')[0]
        tabsTitle = response.xpath("//div[@class='tabber wds-tabber'] //ul[@class='wds-tabs'] //li").css("::text").getall()
        Unobtainable =[i for i, j in enumerate(tabsTitle) if "Unobtainable" in j]
        #Wikitable = response.xpath("//div[@class='wds-tab__content wds-is-current'] //table[@class='wikitable']")
        Wikitable = response.xpath("//div[contains(@class, 'wds-tab__content')] //table[@class='wikitable']")
        if Unobtainable != []:
            Wikitable.pop(Unobtainable[0])
            tabsTitle.pop(Unobtainable[0])
        try:
            altTable = Wikitable.xpath("./ancestor::div //a[contains(@title, '{}/')]/@href".format(EquipSlot))
            for altLink in altTable:
                nurl = response.urljoin(altLink.extract())
                yield scrapy.Request(nurl, callback=self.HandleEquipment)
                
        except Exception as E:
            print(E)
            pass
       
        
        #for row in Wikitable.xpath(".//tr //td/a[not(contains(@href, 'png'))]/@href"):
        for i, table in enumerate(Wikitable):
            Category = tabsTitle[i]
            for row in table.xpath(".//tr "):
                
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
                nurl = response.urljoin(link)
                
                if any(value in EquipName for value in self.ignoreSet):
                    continue
                ItemDict = {
                    "EquipSlot" : EquipSlot,
                    "EquipName" : EquipName,
                    "Grouping" : Category
                }
                yield scrapy.Request(nurl, callback=self.RetrieveByPage, cb_kwargs={"ItemDict": ItemDict})
        pass
    
    def SideHandleMedal(self, response):
        pass
    
    def SideHandleEmblem(self, response):
        pass
    
    def SideHandleRing(self, response):
        pass
    
    def RetrieveByPage(self, response, ItemDict):
        if not response:
            return
        PageTitle = response.xpath("//h1[@class='page-header__title']/text()").get()
        TableContent = response.css("div.mw-parser-output table")[0]
        if PageTitle.find("Genesis") != -1:
            TableContent = response.xpath("//h2/span[@id = 'Unsealed']/parent::h2/following-sibling::table[1]")
        td = []
        for t in TableContent.xpath('.//tr'):
            texts = t.css("td ::text").getall()
            if texts == ['\n']:
                continue
            td.append("".join(texts))
        td = ["".join(value.css("td ::text").getall()).strip('\n') for value in TableContent.css('tr') if value.css("td ::text").getall() != ['\n']]
        th = [value.strip('\n') for value in TableContent.css('tr > th ::text').getall() if value != '\n']
        
        for key, value in zip(th, td[1:]):
            
            if any(value in key for value in self.PageContentSkip):
                continue
            
            try:
                ItemDict[key] = value.strip('+%')
            except:
                continue
        
        print(f"Adding {ItemDict['EquipName']}")
        return ItemDict
    
    def RetrieveByTD(self, ItemDict, content):
        for stat in content:
            if stat.find(':') != -1:
                key, value = stat.split(':')
                if value.strip(' ') == "":
                    continue
                ItemDict[key] = value.strip(' +')
            else:
                try:                   
                    key, value = stat.split(' ')
                    ItemDict[key] = value
                except:
                    continue
        
        return ItemDict
    
    
process = CrawlerProcess(settings=get_project_settings())
process.crawl(TotalEquipmentData)
start = time.time()
process.start()
end = time.time()
print(f"Total TimeTaken is {end-start}")

