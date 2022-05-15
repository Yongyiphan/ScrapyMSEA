
import traceback
from numpy import Infinity
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
        Clist = {}
        TempL = []
        StatIncrease = None
        for PTableGrade in PotentialGrades:
            try:

                divRefPoint = PTableGrade.xpath("./ancestor::div[contains(@class, 'collapsible')]")
                h2Title = divRefPoint.xpath("./preceding-sibling::h2[1]/span[contains(@class, 'headline')] /text()").get()
                PotentialType = "Bonus" if "bonus" in h2Title.lower() else "Potential"
                EquipSlots = divRefPoint.xpath("./preceding-sibling::h3[1]/span[contains(@class,'headline')] /text()").get()
                EList = self.formatEquipSlots(EquipSlots)   
                GradeT = divRefPoint.xpath("./preceding-sibling::h4[1]/span[contains(@class,'headline')] /text()").get()
                if "/" in GradeT:
                    GradeT = GradeT.split("/")[0].strip(' ')
                GradeT = GradeT.split("(")
                Grade = GradeT[0].strip(' ')
                Prime = GradeT[1].strip(')').replace('-', ' ')
                PDict = {
                    "Slot" : EList,
                    "Type" : PotentialType,
                    "Grade" : Grade,
                    "Prime" : Prime
                }
                CDict = {key: value for key, value in PDict.items()}
                for child in PTableGrade.xpath("./child::node()"):
                    childName = child.xpath("name()").get()
                    if childName == None:
                        continue
                    if childName == "h5":
                        DisplayStat = child.xpath(".//span[contains(@class, 'headline')] /text()").get().encode("ascii", "ignore").decode()
                        StatIncrease = DisplayStat
                        CDict["DisplayStat"] = DisplayStat
                        CDict["Stat"] = StatIncrease
                        CDict = self.reformatDisplayStat(CDict)
                    if childName == 'p':
                        ptext = child.xpath("./text()").get()
                        if if_In_String(ptext.lower(), 'level requirement') == False:
                            continue
                        l = ptext.split(":")[-1].strip().split(' ')[0]
                        CDict["MinLvl"] = int(l)
                        CDict["MaxLvl"] = self.maxLvl
                        CStat = CDict['Stat']
                        if not if_In_String(CStat, 'Increase') and if_In_String(CStat, '%'):
                            fv = CStat.split('%')[0].strip().split(' ')[-1]
                            CStat = ' '.join([value.strip() for value in replaceN(CStat, [f"{fv}%", "of", "+"]).split(' ') if value != ''])
                            CDict['Stat value'] = int(fv)
                            CDict['Stat'] = CStat.strip()
                            PLogger.info(CStat)

                        

                    if childName == 'div':
                        continue
                        
                    
                    if childName == "table":
                        prevSib = child.xpath("./preceding-sibling::*[1]")
                        if prevSib.xpath("name()").get() == "dl":
                            #Resets CDict
                            CDict = {key: value for key, value in PDict.items()}
                            pass
                        else:
                            pass
                    TempL.append(childName)

                    

                    
            except Exception as E:
                PLogger.critical(traceback.format_exc())    
        PLogger.info([k for k in Clist.keys()])
        for key, value in Clist.items():
            if key == "h5 table dl table":
                continue
            PLogger.info(f"Types {key}: {value['C']}")        
            for v in value["Stat"]:
                PLogger.info(f"Slot: {v[0]} at {[v[1], v[2], v[3]]} Stat: {v[4]} ")
                
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
        try:
            DS = PDict['DisplayStat']
            SI = PDict['Stat']
            ToRemove = {
                'chance to': "Chance",
                "second" : "Duration"
            }
            if if_In_String(DS, 'when'):
                SI = SI.split('when')[0].strip()  
            for R in ToRemove.keys():
                if if_In_String(DS, R):
                    idx = DS.index(R)
                    t = DS[:idx].strip().split(' ')
                    fv = t[-1]
                    if 'every' in t:
                        PDict['Tick'] = int(fv.strip(" -%"))
                        SI = replaceN(SI, 'every')
                    else:
                        PDict[ToRemove[R]] = int(fv.strip(" -%"))
                    if if_In_String(DS, 'seconds') and R == "second":
                        SI = replaceN(SI, ['seconds', fv]).strip()
                    else:
                        SI = replaceN(SI, [R, fv]).strip()
                        if R == 'chance to':
                            PDict["StatT"] = "Perc" if if_In_String(SI, "%") else "Flat"
            
            SI = replaceN(SI, ['for']).strip()
            SI = SI[0].upper() + SI[1:]
            PDict['Stat'] = SI
        except Exception as E:
            PLogger.info(traceback.format_exc())        


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
    
