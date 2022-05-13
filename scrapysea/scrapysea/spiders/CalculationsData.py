
from dataclasses import replace
import traceback
from pandas import DataFrame
import scrapy
import CustomLogger
import json
import requests
import re
from ComFunc import *

PLogger = CustomLogger.Set_Custom_Logger("PotentialSpider", logTo="./Logs/Potential.log", propagate=False)

class PotentialSpider(scrapy.Spider):
    
    name = "PotentialSpider"
    start_urls = ["https://strategywiki.org/wiki/MapleStory/Potential_System"]
    custom_settings = {
        "LOG_SCRAPED_ITEMS": False
        
    }
    
    PotentialTable = {
        "Potential" : [],
        "Bonus" : []
    }
    
    CubeRates = {
        "Potential" : [],
        "Bonus" : []
    }
    minLvl = 0
    maxLvl = 300
    
    def parse(self, response, **kwargs):
        #Start finding after "PotentialList" tag
        #//div[@class='mw-parser-output'] //span[@id='Potentials_List']/parent::h2/following-sibling::div 
        #  //div[contains(@class,'collapsible-content')]
        # /ancestor::div[contains(@class, 'collapsible')]/preceding-sibling::h4[1]
        try:
            mainContent = response.xpath("//div[@class='mw-parser-output'] //span[@id='Potentials_List']/parent::h2/following-sibling::div")
            PotentialGrades = mainContent.xpath(".//div[contains(@class,'collapsible-content')]")
            
            #self.TestOne(PotentialGrades)
            self.TestTwo(PotentialGrades)
            #self.Execute(PotentialGrades)
                
                
        except Exception as E:
            PLogger.critical(traceback.format_exc())
        
    def close(self, reason):
        try:
            PDF = pd.concat(self.PotentialTable['Potential'], ignore_index=True)
            CDF = pd.concat(self.CubeRates["Potential"], ignore_index=True)
            
            PDF.to_csv("TestResult.csv")
            CDF.to_csv("TestResult2.csv")
        except:
            PLogger.critical(traceback.format_exc())
            
        pass

    def TestOne(self, PotentialGrades):
        PTableGrade = PotentialGrades[8]
        divRefPoint = PTableGrade.xpath("./ancestor::div[contains(@class, 'collapsible')]")
        h2Title = divRefPoint.xpath("./preceding-sibling::h2[1]/span[contains(@class, 'headline')] /text()").get()
        PotentialType = "Bonus" if "bonus" in h2Title.lower() else "Potential"
        if PotentialType == "Bonus":
            return
        EquipSlots = divRefPoint.xpath("./preceding-sibling::h3[1]/span[contains(@class,'headline')] /text()").get()
        EList = self.formatEquipSlots(EquipSlots)
        GradeT = divRefPoint.xpath("./preceding-sibling::h4[1]/span[contains(@class,'headline')] /text()").get()
        if "/" in GradeT:
            GradeT = GradeT.split("/")[0].strip(' ')
        GradeT = GradeT.split("(")
        Grade = GradeT[0].strip(' ')
        Prime = GradeT[1].strip(')').replace('-', ' ')
        for Equip in EList:
            PDict = {}
            added = False
            Clist = self.PotentialTable[PotentialType]
            for child in PTableGrade.xpath("./child::node()"):
                childName = child.xpath("name()").get()
                if childName == None:
                    continue
                if childName == 'h5':
                    StatIncrease = child.xpath(".//span[contains(@class, 'headline')] /text()").get().encode("ascii", "ignore").decode()
                    StatT = "Perc" if if_In_String(StatIncrease, "%") else "Flat"
                    PDict = {
                            "EquipSlot" : Equip,
                            "Grade" : Grade,
                            "Prime" : Prime,
                            "DisplayStat" : StatIncrease,
                            "Stat Increase" : StatIncrease,
                            "StatT" : StatT
                        }
                    PDict = self.reformatDisplayStat(PDict)
                    added = False
                if PDict == {}:
                    continue
                if childName  == 'p':
                    PDcopy = {key: value for key, value in PDict.items()}
                    p = " ".join(child.xpath("./text()").getall())
                        #Retrieve Level Requirements
                    if if_In_String(p.lower(), "level requirement") == False:
                        continue   
                    clvl = int(p.split(":")[1].strip().split(' ')[0])
                    PDcopy["MinLvl"] = clvl
                    PDcopy["MaxLvl"] = self.maxLvl
                    Clist.append(DataFrame(PDcopy, index=[0]))
                    added = True
                    PLogger.info(f"Added {StatIncrease} by P")
                    continue
                    
                if childName == 'table':
                    PDcopy = {key: value for key, value in PDict.items()}
                    #nextEle is table selector
                    prevSibling = child.xpath("./preceding-sibling::*[1]")
                    if prevSibling.xpath("name()").get() == "dl":
                        self.CubeRates[PotentialType].append(self.HandleRates(child, PDcopy))
                        PLogger.info(f"Added Cube Rates  for {[StatIncrease]}")
                        continue
                    else:
                        if not added:
                            Clist.append(self.HandleTables(child, PDcopy))
                            added = True    
                            PLogger.info(f"Added {StatIncrease} by Table")
                            continue
                    
                        
                    
                    ... 
                
            PLogger.info(f"Added {PotentialType} for [{[Equip]}] at [{Grade} - {Prime}]")
        
        
        return
    def Execute(self, PotentialGrades):
        for PTableGrade in PotentialGrades:
            divRefPoint = PTableGrade.xpath("./ancestor::div[contains(@class, 'collapsible')]")
            h2Title = divRefPoint.xpath("./preceding-sibling::h2[1]/span[contains(@class, 'headline')] /text()").get()
            PotentialType = "Bonus" if "bonus" in h2Title.lower() else "Potential"
            if PotentialType == "Bonus":
                break
            EquipSlots = divRefPoint.xpath("./preceding-sibling::h3[1]/span[contains(@class,'headline')] /text()").get()
            EList = self.formatEquipSlots(EquipSlots)
            GradeT = divRefPoint.xpath("./preceding-sibling::h4[1]/span[contains(@class,'headline')] /text()").get()
            if "/" in GradeT:
                GradeT = GradeT.split("/")[0].strip(' ')
            GradeT = GradeT.split("(")
            Grade = GradeT[0].strip(' ')
            Prime = GradeT[1].strip(')').replace('-', ' ')
            for Equip in EList:
                PDict = {}
                added = False
                Clist = self.PotentialTable[PotentialType]
                for child in PTableGrade.xpath("./child::node()"):
                    childName = child.xpath("name()").get()
                    if childName == None:
                        continue
                    if childName == 'h5':
                        if not added and PDict != {}:
                            Clist.append(DataFrame(PDict, index=[0]))
                        StatIncrease = child.xpath(".//span[contains(@class, 'headline')] /text()").get().encode("ascii", "ignore").decode()
                        StatT = "Perc" if if_In_String(StatIncrease, "%") else "Flat"
                        PDict = {
                                "EquipSlot" : Equip,
                                "Grade" : Grade,
                                "Prime" : Prime,
                                "DisplayStat" : StatIncrease,
                                "Stat Increase" : StatIncrease,
                                "StatT" : StatT
                            }
                        PDict = self.reformatDisplayStat(PDict)
                        added = False
                    if childName  == 'p':
                        PDcopy = {key: value for key, value in PDict.items()}
                        p = " ".join(child.xpath("./text()").getall())
                        PDcopy = self.HandleP(p, PDcopy)
                        continue
                        
                    if childName == 'table':
                        PDcopy = {key: value for key, value in PDict.items()}
                        #nextEle is table selector
                        prevSibling = child.xpath("./preceding-sibling::*[1]")
                        if prevSibling.xpath("name()").get() == "dl":
                            self.CubeRates[PotentialType].append(self.HandleRates(child, PDcopy))
                            #PLogger.info(f"Added Cube Rates  for {[StatIncrease]}")
                            continue
                        else:
                            if not added:
                                Clist.append(self.HandleTables(child, PDcopy))
                                added = True    
                                #PLogger.info(f"Added {StatIncrease} by Table")
                                continue
                    
                    if childName == "div":
                        pass
                        
                            
                        
                        ... 
                    
                PLogger.info(f"Added {PotentialType} for [{[Equip]}] at [{Grade} - {Prime}]")
            
        return
    def TestTwo(self, PotentialGrades):
        for PTableGrade in PotentialGrades:
            try:
                divRefPoint = PTableGrade.xpath("./ancestor::div[contains(@class, 'collapsible')]")
                h2Title = divRefPoint.xpath("./preceding-sibling::h2[1]/span[contains(@class, 'headline')] /text()").get()
                PotentialType = "Bonus" if "bonus" in h2Title.lower() else "Potential"
                if PotentialType == "Bonus":
                    break
                EquipSlots = divRefPoint.xpath("./preceding-sibling::h3[1]/span[contains(@class,'headline')] /text()").get()
                EquipSlots = self.formatEquipSlots(EquipSlots)
                StatIncrease = ""
                GradeT = divRefPoint.xpath("./preceding-sibling::h4[1]/span[contains(@class,'headline')] /text()").get()
                if "/" in GradeT:
                    GradeT = GradeT.split("/")[0].strip(' ')
                GradeT = GradeT.split("(")
                Grade = GradeT[0].strip(' ')
                PDict = {}
                Clist = []
                Prime = GradeT[1].strip(')').replace('-', ' ')
                for child in PTableGrade.xpath("./child::node()"):
                    childName = child.xpath("name()").get()
                    if childName == None:
                        continue
                    if childName == 'h5':
                        #If dict is filled, add to list, else continue
                        if PDict != {}:
                            Clist.append(PDict)
                        DisStat = child.xpath(".//span[contains(@class, 'headline')] /text()").get().encode("ascii", "ignore").decode()
                        StatT =  "Perc" if if_In_String(DisStat, '%') else "Flat"
                            
                        StatIncrease = DisStat
                        PDict = {
                            "Slot" : EquipSlots,
                            "Grade" : Grade,
                            "Prime" : Prime,
                            "DisplayStat" : DisStat,
                            "StatIncrease" : StatIncrease,
                            "StatType" : StatT
                        }
                    if childName == "p":
                        #PLogger.info(f"{[EquipSlots]} Set: {[StatIncrease]} at {Grade}: {Prime}")
                        P = child.xpath("./text()").get()
                        PDict = self.HandleP(P, PDict)
                        
                        pass
                    if childName == "table":
                        prevSibling  =  child.xpath()
                        pass
                    if childName == "":
                        pass
                    if childName == "p":
                        pass
            except Exception as E:
                PLogger.critical(traceback.format_exc())    
                
        
        return   
    def HandleP(self, p, PDcopy):
        #PDict is a dict deepcopy
        if if_In_String(p.lower(), "level requirement"):
            clvl = int(p.split(":")[1].strip().split(' ')[0])
            PDcopy["MinLvl"] = clvl
            PDcopy["MaxLvl"] = self.maxLvl

        return PDcopy
    
    def HandleTables(self, context, PDict):
        header =  removeB(context.xpath(".//th /text()").getall())
        TableResult = []
        for row in context.xpath(".//tr"):
            text = removeB(row.xpath(".//text()").getall())
            if text == [] or text == header or if_In_String(" ".join(text), "GMS"):
                continue
            PDcopy = {key: value for key, value in PDict.items()}
            for i, h in enumerate(header):
                ctext = row.xpath(f".//td[{i+1}] /text()").get()
                if if_In_String(h.lower(), 'level'):
                    lvl = ctext.strip().split("-")
                    PDcopy["MinLvl"] = int(lvl[0].strip(' \n+%'))
                    PDcopy["MaxLvl"] = self.maxLvl if if_In_String(lvl[0], '+') else int(lvl[-1].strip(' \n+%'))
                elif if_In_String(h.lower(), 'chance'):
                    PDcopy["Chance"] = ctext.strip(" +%\n")
                else:
                    v = h.split('(')[0].strip()
                    ctext = replaceN(ctext, ['seconds'])
                    PDcopy[v] = ctext.strip(" +%\n")
                pass
            TableResult.append(DataFrame(PDcopy, index=[0]))


        Result = pd.concat(TableResult, ignore_index=True)
        print(Result)
        return Result
        
  
    def HandleRates(self, context, PDict):
        
        header =  removeB(context.xpath(".//th /text()").getall())
        
        for i, j in enumerate(header):
            v = context.xpath(f".//td[{i+1}] /text()").get()
            v = v.split("(")[0]
            PDict[j] = v.strip(' +%\n')
        
        Result = DataFrame(PDict, index=[0])
        print(Result)
        return Result

    def reformatDisplayStat(self, PDict):
        return PDict
    
    def formatEquipSlots(self, EquipStr):
        if if_In_String(EquipStr, "("):
            EquipStr =  EquipStr.split("(")[0].strip(' ')
        if "and" in EquipStr:
            EquipStr = EquipStr.replace("and", ";").split(';')
        if if_In_String(EquipStr, ","):
            EquipStr = replaceN(EquipStr, ',', ';').split(';')
        if isinstance(EquipStr, list):
            return [value.strip() for value in EquipStr]
        elif isinstance(EquipStr, str):
            return [EquipStr.strip()]
        pass
    
