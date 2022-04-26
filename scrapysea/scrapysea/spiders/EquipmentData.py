from curses import meta
from select import select
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
import re

EquipmentURLS = [
    "https://maplestory.fandom.com/wiki/Category:Secondary_Weapons",
        "http://maplestory.fandom.com/wiki/Category:Equipment_Sets",
        "http://maplestory.fandom.com/wiki/Sealed_Genesis_Weapon_Box",
        "http://maplestory.fandom.com/wiki/Category:Superior_Equipment",
        "http://maplestory.fandom.com/wiki/Android_Heart"
]
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
            ItemDict = {}
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
            {"WeaponData.json":{"format":"json", "overwrite" : True}}
    }
    
    def parse(self, response):
        links = response.css('a.category-page__member-link::attr(href)')
        #for href in links:
        
            #url = response.urljoin(href.extract())
        url = response.urljoin(links[0].extract())
        yield scrapy.Request(url, callback=self.redirectMainArticle)
        ...
        
    def redirectMainArticle(self, response):
        mainArticleUrl = response.css('div.mw-parser-output a::attr(href)').get()
        wtype = " ".join(re.split('_|-' , mainArticleUrl.extract().split(':')[-1])).strip(' ')
        url = response.urljoin(mainArticleUrl)
        yield scrapy.Request(url, callback=self.retrieveSecondaryWeapons, meta = {"WeaponType":wtype})
        
        ...
        
    def retrieveSecondaryWeapons(self, response):
        header = response.css('div.mw-parser-output p')
        classType = []
        CStart = False
        for i in header:
            texts = i.css('::text').getall()
            for s in texts:
                if s.find('exclusive') != -1:
                    CStart = True
                if s.find("conjunction") != -1:
                    CStart = False
                if CStart and s.strip(' ') != '\n':
                    classType.append(s)
        
        table = response.css('div.mw-parser-output table tr')
        ignoreList = ["evolving", "frozen"]
        for C in classType:
            for row in table:
                td = row.xpath('td//text()').getall()
                if any(value in td[0].lower() for value in ignoreList):
                    continue
                if td[0].lower().find('equipment') != -1:
                    break
                ItemDict = {
                    "ClassType" : C,
                    "WeaponType" : response.meta['WeaponType']
                }
                
            
        ...
        

    

#process = CrawlerProcess(get_pondataSpider)
#process.start()project_settings())
#process.crawl(Wea
