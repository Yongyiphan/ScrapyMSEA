from time import sleep
import pandas
from pandas import DataFrame
import json
import re
import CustomLogger
import traceback

from ComFunc import *

import scrapy

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

CLogger = CustomLogger.Set_Custom_Logger("CharacterSpider", "./Logs/Character.log", propagate=False)


class CharacterSpider(scrapy.Spider):

    name = "CharacterData"
    allowed_domains = ['grandislibrary.com/']
    start_urls = ['https://grandislibrary.com/classes']
    jsonKey = "https://www.grandislibrary.com/_next/data/" #/explorers/hero.json"

    custom_settings = {
        "LOG_SCRAPED_ITEMS": False
    }

    CharacterDF = []
    UnionDF = []
    WeaponDF = []
    SecondaryDF = []
        
    IgnoreClasses = ["beast tamer", "jett"]

    def parse(self, response):
        try:
            options = webdriver.ChromeOptions()
            options.add_argument('--log-level=0')
            browser = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
            #browser = webdriver.Chrome(ChromeDriverManager().install())
            browser.get(response.url)

            browser = self.scrollDown(browser, 10)
            
            lazyLoadDiv = browser.find_elements(By.XPATH, "//div[contains(@class,'lazyload-wrapper')] //a[@class='hvr-float']")
            ClassesLinks = []
            for a in lazyLoadDiv:
                link = a.get_attribute('href').split('/')[-2:]
                if any(list(set(re.split('-| ',link[-1]))&set(value.split(' '))) != [] for value in self.IgnoreClasses):
                    continue
                link =  "/".join(link) + ".json"
                CLogger.info(link)
                ClassesLinks.append(link)
            browser.close()
            buildID = json.loads(response.xpath("//script[@id='__NEXT_DATA__'] /text()").get())['buildId']
            for i, link in enumerate(ClassesLinks):
                nurl = self.jsonKey + buildID + "/" + link
                self.logger.info(f"{i}: {nurl}")
                yield scrapy.Request(nurl, callback=self.JsonResponseData, dont_filter=True)
        except Exception:
            CLogger.warn(traceback.format_exc())

    def scrollDown(self, browser, noOfScrollDown):
        body = browser.find_element_by_tag_name("body") 
        while noOfScrollDown >=0:
            body.send_keys(Keys.PAGE_DOWN)
            sleep(0.3)
            noOfScrollDown -=1
            
        return browser

    def close(self):
        try:
            CDF = pandas.concat(self.CharacterDF, ignore_index=True)

            UDF = pandas.concat(self.UnionDF, ignore_index=True)
            UDF = CleanUnionDF(UDF)
            
            WDF = pandas.concat(self.WeaponDF, ignore_index=True)
            WDF = CleanClassMainWeaponDF(WDF)

            SDF = pandas.concat(self.SecondaryDF, ignore_index=True)
            SDF = CleanClassSecWeaponDF(SDF)

            CDF.to_csv('./DefaultData/CharacterData/CharacterData.csv')
            UDF.to_csv('./DefaultData/CharacterData/UnionEffect.csv')
            WDF.to_csv('./DefaultData/CharacterData/ClassMainWeapon.csv')
            SDF.to_csv('./DefaultData/CharacterData/ClassSecWeapon.csv')

            TimeTaken(self)
        except Exception:
            CLogger.warn(traceback.format_exc())

    def JsonResponseData(self, response):
        try:
            data = json.loads(response.text)
            PostData = data['pageProps']['post']
            PostContentData = PostData['content']

            ClassName = PostData['class']
            UnionE = PostContentData['legion']
            UnionStatType = "Flat" if 'flat' in UnionE else "Perc"
            for value in ['%', ',']:
                if if_In_String(UnionE, value):
                    UnionE = UnionE.replace(value, '')
            
            if if_In_String(UnionE, "("):
                UnionE = UnionE.split('(')[0]
            
            CharacterDict = {
                "ClassName" : ClassName,
                "Faction" : PostContentData['classGroup'].split('(')[0].rstrip(' '),
                "ClassType" : "SPECIAL" if if_In_String(PostContentData['jobGroup'],"+") else PostContentData['jobGroup'],
                "MainStat"  : "SPECIAL" if if_In_String(PostContentData['mainStat'], ',') else PostContentData['mainStat'],
                "SecStat"   : "SPECIAL" if if_In_String(PostContentData['secondaryStat'], "+") else PostContentData['secondaryStat'],
                "UnionEffect" : UnionE,
                "UnionStatType" : UnionStatType
            }
            self.CharacterDF.append(DataFrame(CharacterDict, index=[0]))
            
            LegionValues = PostContentData['legionValue'].split("/")
            FinalValue = LegionValues[4]
            if if_In_String(FinalValue, '%'):
                FinalValue = FinalValue.replace('%', '')
            for value in ['-', '[']:
                if if_In_String(FinalValue, value):
                    FinalValue = FinalValue.split(value)[0]

            UnionEffects = {
                "Effect" : UnionE,
                "EffectType" : UnionStatType,
                "B" : LegionValues[0],
                "A" : LegionValues[1],
                "S" : LegionValues[2],
                "SS" : LegionValues[3],
                "SSS" : FinalValue.rstrip()
            }
            self.UnionDF.append(DataFrame(UnionEffects, index=[0]))
            Equipments = PostContentData['equipment']
            WeaponList = Equipments[0]['weapon']
            for weap in WeaponList:
                self.WeaponDF.append(
                    DataFrame({
                        "ClassName": ClassName, 
                        "Weapon" : weap.capitalize() }, 
                        index=[0]))
            SecondaryList = Equipments[1]['secondary']
            for sec in SecondaryList:
                self.SecondaryDF.append(DataFrame({"ClassName" : ClassName, "Secondary" : sec.capitalize() }, index=[0]))

        except Exception:
            CLogger.warn(traceback.format_exc())
        finally:
            CLogger.info(f"Adding {ClassName}")
            return

        
def CleanClassMainWeaponDF(CDF):
    CDF.loc[CDF["Weapon"] == 'Ancientbow', 'Weapon']     = "Ancient Bow"
    CDF.loc[CDF["Weapon"] == 'Armcannon', 'Weapon']      = "Arm Cannon"
    CDF.loc[CDF["Weapon"] == 'Bladecaster', 'Weapon']    = "Tuner"
    CDF.loc[CDF["Weapon"] == 'Dualbowguns', 'Weapon']    = "Dual Bowguns"
    CDF.loc[CDF["Weapon"] == 'Fankanna', 'Weapon']       = "Fan"
    CDF.loc[CDF["Weapon"] == 'Handcannon', 'Weapon']     = "Hand Cannon"
    CDF.loc[CDF["Weapon"] == 'Longsword', 'Weapon']      = "Long Sword"
    CDF.loc[CDF["Weapon"] == 'Lucentgauntlet', 'Weapon'] = "Magic Gauntlet"
    CDF.loc[CDF["Weapon"] == 'Onehaxe', 'Weapon']        = "One Handed Axe"
    CDF.loc[CDF["Weapon"] == 'Onehblunt', 'Weapon']      = "One Handed Blunt Weapon"
    CDF.loc[CDF["Weapon"] == 'Onehsword', 'Weapon']      = "One Handed Sword"
    CDF.loc[CDF["Weapon"] == 'Psylimiter', 'Weapon']     = "Psy Limiter"
    CDF.loc[CDF["Weapon"] == 'Ritualfan', 'Weapon']      = "Buchae"
    CDF.loc[CDF["Weapon"] == 'Shiningrod', 'Weapon']     = "Shining Rod"
    CDF.loc[CDF["Weapon"] == 'Soulshooter', 'Weapon']    = "Soul Shooter"
    CDF.loc[CDF["Weapon"] == 'Twohaxe', 'Weapon']        = "Two Handed Axe"
    CDF.loc[CDF["Weapon"] == 'Twohblunt', 'Weapon']      = "Two Handed Blunt Weapon"
    CDF.loc[CDF["Weapon"] == 'Twohsword', 'Weapon']      = "Two Handed Sword"
    CDF.loc[CDF["Weapon"] == 'Whipblade', 'Weapon']      = "Whip Blade"
    return CDF

def CleanClassSecWeaponDF(CDF):
    CDF.loc[CDF["Secondary"] == 'Abyssalpath', 'Secondary'] = "Abyssal Path"
    CDF.loc[CDF["Secondary"] == 'Arrowfletching', 'Secondary'] = "Arrow Fletching"
    CDF.loc[CDF["Secondary"] == 'Arrowhead', 'Secondary'] = "Arrow Head"
    CDF.loc[CDF["Secondary"] == 'Bladebinder', 'Secondary'] = "Bracelet"
    CDF.loc[CDF["Secondary"] == 'Bowthimble', 'Secondary'] = "Bow Thimnble"
    CDF.loc[CDF["Secondary"] == 'Chesspiece', 'Secondary'] = "Chess Piece"
    CDF.loc[CDF["Secondary"] == 'Corecontroller', 'Secondary'] = "Core Controller"
    CDF.loc[CDF["Secondary"] == 'Demonaegis', 'Secondary'] = "Demon Aegis"
    CDF.loc[CDF["Secondary"] == 'Dragonessence', 'Secondary'] = "Dragon Essence"
    CDF.loc[CDF["Secondary"] == 'Fankanna', 'Secondary'] = "Fan"
    CDF.loc[CDF["Secondary"] == 'Fantassel', 'Secondary'] = "Fan Tassel"
    CDF.loc[CDF["Secondary"] == 'Farsight', 'Secondary'] = "Far Sight"
    CDF.loc[CDF["Secondary"] == 'Foxmarble', 'Secondary'] = "Fox Marble"
    CDF.loc[CDF["Secondary"] == 'Heavysword', 'Secondary'] = "Heavy Sword"
    CDF.loc[CDF["Secondary"] == 'Ironchain', 'Secondary'] = "Iron Chain"
    CDF.loc[CDF["Secondary"] == 'Lucentwings', 'Secondary'] = "Magic Wing"
    CDF.loc[CDF["Secondary"] == 'Mageshield', 'Secondary'] = "Shield"
    CDF.loc[CDF["Secondary"] == 'Magicarrow', 'Secondary'] = "Magic Arrow"
    CDF.loc[CDF["Secondary"] == 'Magicbookb', 'Secondary'] = "Magic Book"
    CDF.loc[CDF["Secondary"] == 'Magicbooki', 'Secondary'] = "Magic Book"
    CDF.loc[CDF["Secondary"] == 'Magicmarble', 'Secondary'] = "Magic Marble"
    CDF.loc[CDF["Secondary"] == 'Powderkeg', 'Secondary'] = "Powder Keg"
    CDF.loc[CDF["Secondary"] == 'Scabbard', 'Secondary'] = "Dagger Scabbard"
    CDF.loc[CDF["Secondary"] == 'Soulring', 'Secondary'] = "Soul Ring"
    CDF.loc[CDF["Secondary"] == 'Soulshield', 'Secondary'] = "Soul Shield"
    CDF.loc[CDF["Secondary"] == 'Thiefshield', 'Secondary'] = "Shield"
    CDF.loc[CDF["Secondary"] == 'Warpforge', 'Secondary'] = "Warp  Forge"
    CDF.loc[CDF["Secondary"] == 'Warshield', 'Secondary'] = "Shield"
    CDF.loc[CDF["Secondary"] == 'Weaponbelt', 'Secondary'] = "Weapon Belt"
    CDF.loc[CDF["Secondary"] == 'Wristband', 'Secondary'] = "Wrist Band"
    return CDF


def CleanUnionDF(CDF):

    CDF.drop_duplicates(keep="first", inplace=True)
    CDF.reset_index(drop = True)
    
    return CDF