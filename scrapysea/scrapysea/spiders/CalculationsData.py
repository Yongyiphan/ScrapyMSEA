
import traceback
from webbrowser import Elinks

import pandas as pd
import scrapy
import CustomLogger
import ComFunc as CF
from CompleteRun import *
from tqdm import *


PLogger  = CustomLogger.Set_Custom_Logger("PotentialSpider", logTo="./Logs/Calculation.log", propagate=False)
SFLogger = CustomLogger.Set_Custom_Logger("StarforceSpider",logTo="./Logs/Calculation.log",propagate=False )
BSLogger = CustomLogger.Set_Custom_Logger("BonusSpider",logTo="./Logs/Calculation.log",propagate=False )
HSLogger = CustomLogger.Set_Custom_Logger("HyperStatSpider",logTo="./Logs/Calculation.log",propagate=False )


class PotentialSpider(scrapy.Spider):
    
    name = "PotentialSpider"
    start_urls = ["https://strategywiki.org/wiki/MapleStory/Potential_System"]
    custom_settings = {
        "LOG_SCRAPED_ITEMS": False
        
    }
    
    PotentialTable = {
        "Main" : [],
        "Bonus" : []
    }
    
    CubeRates = {
        "Main" : [],
        "Bonus" : []
    }
    minLvl = 0
    maxLvl = 300
    
    def parse(self, response):
        try:
            mainContent = response.xpath("//div[@class='mw-parser-output'] //span[@id='Potentials_List']/parent::h2/following-sibling::div[not(contains(@class, 'nav_box'))]")
            PotentialGrades = mainContent.xpath(".//div[contains(@class,'collapsible-content')]")
            self.Execute(PotentialGrades)
                
        except Exception as E:
            PLogger.critical(traceback.format_exc())
        
    def close(self):
        try:
            PDF = pd.concat(self.PotentialTable['Potential'], ignore_index=True)
            CDF = pd.concat(self.CubeRates["Potential"], ignore_index=True)
            BPDF = pd.concat(self.PotentialTable["Bonus"], ignore_index=True)
            BCDF = pd.concat(self.CubeRates["Bonus"], ignore_index=True)            

            #CLean Potential DF
            PDF = PDF.fillna(0)
            PDF.drop(['Duration'], axis=1, inplace=True)
            PDF[['MinLvl', 'MaxLvl','Chance', 'Tick']] = PDF[['MinLvl', 'MaxLvl','Chance', 'Tick']].astype(int)
            PDF.drop_duplicates(keep="first", inplace=True)

            #Clean Potential Cube Rates 
            CDF = CDF.fillna(0)
            CDF.drop_duplicates(keep="first", inplace=True)

            #Clean Bonus Potential DF
            BPDF = BPDF.fillna(0)
            BPDF[['MinLvl', 'MaxLvl','Chance']] = BPDF[['MinLvl', 'MaxLvl','Chance']].astype(int)
            BPDF.drop_duplicates(keep="first", inplace=True)

            #Clean Bonus Potential Cube Rates
            BCDF = BCDF.fillna(0)      
            BCDF.drop_duplicates(keep="first", inplace=True)



            #Upload DF to CSV
            PDF.to_csv(CF.APPFOLDER + "CalculationData\\PotentialData.csv")
            CDF.to_csv(CF.APPFOLDER + "CalculationData\\PotentialCubeRatesData.csv")

            BPDF.to_csv(CF.APPFOLDER + "CalculationData\\BonusData.csv")
            BCDF.to_csv(CF.APPFOLDER + "CalculationData\\BonusCubeRatesData.csv")
        except:
            PLogger.critical(traceback.format_exc())
        
        CF.TimeTaken(self)
        pass
   
    def Execute(self, PotentialGrades):
        FromDiv = False
        MainBar = tqdm(total=2, desc="Main Potential: ")
        t = len(PotentialGrades.xpath("./h5"))
        CBar = tqdm(total=t)
        CurrentEListTrack = []
        CurrentPotTrack = []
        for PTableGrade in PotentialGrades:
            try:
                PotentialType = PTableGrade.xpath("./parent::div/preceding-sibling::h2[1]//text()").get()
                PotentialType = "Bonus" if CF.if_In_String(PotentialType.lower(), "bonus") else "Main"
                EList = PTableGrade.xpath("./parent::div/preceding-sibling::h3[1]//text()").get()
                if PotentialType not in CurrentPotTrack:
                    CurrentPotTrack.append(PotentialType)
                    MainBar.update( 1  if PotentialType == "Bonus" else 0)
                    MainBar.set_description = "Bonus Potential" if PotentialType == "Bonus" else "Main Potential"
                    CurrentEListTrack = []
                EList = [s.strip() for s in CF.replaceN(EList, ["and", ","], "|").split('|')]
                if EList not in CurrentEListTrack:
                    CurrentEListTrack.append(EList)
                    CBar.set_description("{0}".format(EList))
                    #CBar.total = len(PTableGrade.xpath("./child::h5"))
                    CBar.refresh()
                Grade = PTableGrade.xpath("./parent::div/preceding-sibling::h4[1]//text()").get()
                Grade, Prime =  self.ReturnGrade(Grade)
                PDict = {
                    "Slot" : EList,
                    "Grade"  : Grade,
                    "Prime"  : Prime
                }
                
                Clist = self.PotentialTable[PotentialType]
                ChanceL = self.CubeRates[PotentialType]
                CDict = {key: value for key, value in PDict.items()}
                for child in PTableGrade.xpath("./*"):
                    childName = child.xpath("name()").get()
                    if childName == None:
                        continue
                    if childName == "h5":
                        DisplayStat = child.xpath(".//span[contains(@class, 'headline')] /text()").get().encode("ascii", "ignore").decode()
                        
                        CDict["DisplayStat"] = DisplayStat
                        #CDict['Stat'] = DisplayStat
                        CDict = self.reformatDisplayStat(CDict)
                        PDict["DisplayStat"] = CDict["DisplayStat"]
                        continue
                    if childName == 'p':
                        ptext = child.xpath("./text()").get()
                        if CF.if_In_String(ptext.lower(), 'level requirement') == False:
                            continue
                        l = ptext.split(":")[-1].strip().split(' ')[0]
                        CDict["MinLvl"] = int(l)
                        CDict["MaxLvl"] = self.maxLvl
                        CStat = CDict['DisplayStat']
                        if not CF.if_In_String(CStat, 'Increase') and CF.if_In_String(CStat, '%'):
                            fv = CStat.split('%')[0].strip().split(' ')[-1]
                            CStat = ' '.join([value.strip() for value in CF.replaceN(CStat, [f"{fv}%", "of", "+"]).split(' ') if value != ''])
                            CDict['Stat value'] = int(fv)
                            #CDict['Stat'] = CStat.replace("%", "").strip()
                        continue

                    if childName == 'div':
                        FromDiv = True
                        for table in child.xpath(".//div"):
                            PDcopy = CF.DeepCopyDict(CDict)
                            for tchild in table.xpath("./child::node()"):
                                cname = tchild.xpath("name()").get()                                    
                                if cname == None:
                                    continue
                                if cname == 'table':
                                    PDcopy['StatTable'] = self.HandleTables(tchild)
                                    Clist.append(self.ConsolidateTable(PDcopy, "StatTable"))
                                    continue
                                ttext = tchild.xpath('.//text()').get()
                                if CF.if_In_String(ttext.lower(), "cubes"):
                                    ttext = CF.replaceN(ttext, ",",";")
                                    PDcopy['CubeType'] = ttext
                                    continue
                                if CF.if_In_String(ttext.lower(), "chance"):
                                    sv = ttext.split(' ')
                                    PDcopy["Chance"] = sv[-1].strip(' +%\n')
                                    continue
                        continue
                        
                    if childName == "table":
                        prevSib = child.xpath("./preceding-sibling::*[1]")
                        if prevSib.xpath("name()").get() == "dl":
                            if not FromDiv:
                                if "StatTable" in CDict.keys(): 
                                    Clist.append(self.ConsolidateTable(CDict, "StatTable"))
                                else:
                                    for s in CDict['Slot']:
                                        TempD = CF.DeepCopyDict(CDict)
                                        TempD["Slot"] = s
                                        NTempD = self.ReorgStatValue(TempD)
                                        Clist.append(pd.DataFrame(NTempD, index=[0]))
                            
                            CDict = CF.DeepCopyDict(PDict)
                            CDict["ChanceTable"] = self.HandleTables(child)
                            ChanceL.append(self.ConsolidateTable(CDict, "ChanceTable"))
                            FromDiv = False
                            CBar.update(1)
                        else:
                            CDict["StatTable"] = self.HandleTables(child)
                        continue

            except Exception as E:
                PLogger.critical(traceback.format_exc())    
        MainBar.update(1)
        return   
 
    
    def HandleTables(self, context):
        header =  CF.removeB(context.xpath(".//th /text()").getall())
        TableResult = []
        for row in context.xpath(".//tr"):
            text = CF.removeB(row.xpath(".//text()").getall())
            if text == [] or text == header or CF.if_In_String(" ".join(text), "GMS"):
                continue
            PDcopy = {}
            for i, h in enumerate(header):
                ctext = row.xpath(".//td[{0}] /text()".format(i+1)).get()
                if CF.if_In_String(h.lower(), 'level'):
                    lvl = ctext.strip().split("-")
                    PDcopy["MinLvl"] = int(lvl[0].strip(' \n+%'))
                    PDcopy["MaxLvl"] = self.maxLvl if CF.if_In_String(lvl[0], '+') else int(lvl[-1].strip(' \n+%'))
                elif CF.if_In_String(h.lower(), 'chance'):
                    PDcopy["Chance"] = ctext.strip(" +%\n")
                else:
                    v = h.split('(')[0].strip()
                    ctext = CF.replaceN(ctext, ['seconds'])
                    if CF.if_In_String(ctext, '('):
                        ctext = ctext.split("(")[0]
                    PDcopy[v] = ctext.strip(" +\n")
                pass
            TableResult.append(PDcopy)
        return TableResult
  

    def reformatDisplayStat(self, PDict):
        try:
            DS = PDict['DisplayStat']
            SI = PDict['DisplayStat']
            ToRemove = {
                'chance to': "Chance",
                "second" : "Duration"
            }
            if CF.if_In_String(DS, 'when'):
                SI = SI.split('when')[0].strip()  
            for R in ToRemove.keys():
                if CF.if_In_String(DS, R):
                    idx = DS.index(R)
                    t = DS[:idx].strip().split(' ')
                    fv = t[-1]
                    if 'every' in t:
                        PDict['Tick'] = int(fv.strip(" -%"))
                        SI = CF.replaceN(SI, 'every')
                    else:
                        PDict[ToRemove[R]] = int(fv.strip(" -%"))
                    if CF.if_In_String(DS, 'seconds') and R == "second":
                        SI = CF.replaceN(SI, ['seconds', fv]).strip()
                    else:
                        SI = CF.replaceN(SI, [R, fv]).strip()
                        
            SI = CF.replaceN(SI, ['for']).strip()
            SI = SI[0].upper() + SI[1:]
            PDict["StatT"] = "Perc" if CF.if_In_String(SI, "%") else "Flat"
            #PDict['Stat'] = SI.strip()
        except Exception as E:
            PLogger.critical(traceback.format_exc())        
        return PDict
    
    def ReturnGrade(self, Grade):
        Temp = Grade.split(" ")
        Grade = Temp[0]
        Prime = CF.replaceN(Temp[1], ["(", ")"])
        return (Grade, Prime)

    
    def UpdateTableDict(self, Base, AddOn):
        for k, v in AddOn.items():
            Base[k] = v
        return Base

    def ConsolidateTable(self, Base, key):
        try:
            StatLists = []
            if "Slot" in Base.keys():
                for s in Base['Slot']:
                    for row in Base[key]:
                        TempD = CF.DeepCopyDict(Base)
                        TempD['Slot'] = s
                        TempD = self.UpdateTableDict(TempD, row)
                        del TempD[key]
                        if key == "StatTable":
                            TempD = self.ReorgStatValue(TempD)
                        
                        if "Stat value" in TempD.keys() and "StatT" in TempD.keys():
                            if CF.if_In_String(TempD["Stat value"], "%"):
                                TempD['Stat value'] = CF.replaceN(TempD["Stat value"],'%').strip()
                                if TempD['StatT'] != "Perc":
                                    TempD['StatT'] = "Perc"
                        
                        StatLists.append(pd.DataFrame(TempD, index=[0]))   
                try:
                    #PLogger.info(f"Grabbing {s}: {Base['DisplayStat']} => {Base['Stat']}")
                    PLogger.info("Grabbing {0} - {1} for : {2}".format(Base['Slot'], key, Base["DisplayStat"]))
                except KeyError:
                    pass
            R = pd.concat(StatLists, ignore_index=True)
            return R
        except Exception as E:
            PLogger.critical(traceback.format_exc())

    def ReorgStatValue(self, TempD):
        KeyToD = []
        if "Stat value" not in TempD.keys() or TempD["Stat value"] == 0:
            IgnoreCol = ['Slot', 'Grade', 'Prime', 'DisplayStat', 'StatT', 'MinLvl', 'MaxLvl','Chance','Tick', 'CubeType']
            for k in list(TempD.keys()):
                if k in IgnoreCol:
                    continue
                if TempD[k] != 0:
                    TempD["Stat value"] = TempD[k]
                    if k not in KeyToD:
                        KeyToD.append(k)
            for d in KeyToD:
                    del TempD[d]
        return TempD
    
import re
class TestSpider(scrapy.Spider):
    name = "TestSpider"
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
    
    def parse(self, response):
        try:
            mainContent = response.xpath("//div[@class='mw-parser-output'] //span[@id='Potentials_List']/parent::h2/following-sibling::div[not(contains(@class, 'nav_box'))]")
            PotentialGrades = mainContent.xpath(".//div[contains(@class,'collapsible-content')]")
            self.Execute(PotentialGrades)
                
        except Exception as E:
            PLogger.critical(traceback.format_exc())
    
    def Execute(self, Contents):
        #Contents Contains tables at potential grade
        
        
        for GradeTable in Contents:
            try:
                PotentialType = GradeTable.xpath("./parent::div/preceding-sibling::h2[1]//text()").get()
                PotentialType = "Bonus" if CF.if_In_String(PotentialType.lower(), "bonus") else "Main"
                EqList = GradeTable.xpath("./parent::div/preceding-sibling::h3[1]//text()").get()
                EqList = [s.strip() for s in CF.replaceN(EqList, ["and", ","], "|").split('|')]
                Grade = GradeTable.xpath("./parent::div/preceding-sibling::h4[1]//text()").get()
                Grade, Prime =  self.ReturnGrade(Grade)
                BlankEntry = {
                    "PotentialType" : PotentialType,
                    "EqList" : EqList,
                    "Grade"  : Grade,
                    "Prime"  : Prime
                }
                IsChance = False
                for element in Contents.xpath("./*"):
                    eName = element.xpath("name()").get()
                    if eName == "h5":
                        IsChance = False
                        ##Retrieve DisplayStat
                        DisplayStat = element.xpath(".//span[contains(@class, 'headline')] /text()").get().encode("ascii", "ignore").decode()
                        print(DisplayStat)
                        ... 
                        
                    elif eName == "dl":
                        IsChance =  True
                    
                    if eName == "table":
                        if IsChance:
                            ...
                        else:
                            ...        
                    
                    ... 
            except Exception as E:
                PLogger.warning(traceback.format_exc())
            
            print(Grade)
        
    def ReturnGrade(self, Grade):
        Temp = Grade.split(" ")
        Grade = Temp[0]
        Prime = CF.replaceN(Temp[1], ["(", ")"])
        return (Grade, Prime)
    
    
        

class StarforceSpider(scrapy.Spider):
    name = "StarforceSpider"
    start_urls = ["https://strategywiki.org/wiki/MapleStory/Spell_Trace_and_Star_Force"]
    custom_settings = {
        "LOG_SCRAPED_ITEMS" : False
    }

    FinalDict = {
        "StarLimit" : [],
        "SuccessRates":[],
        #StatsBoost
        "Normal_Equips" : None,
        "Superior_Items" : None
        
    }
    MaxLvl = 300
    def parse(self, response):
        try:
            StartRecords = response.xpath("//span[@id='Normal_Equip']/parent::h3/following-sibling::*")
            yield self.GetStarforce(StartRecords)
        except Exception as E:
            SFLogger.critical(traceback.format_exc())
    
    def close(self):
        StarLimitDF = pd.concat(self.FinalDict["StarLimit"],ignore_index=True)
        SuccessDF = pd.concat(self.FinalDict["SuccessRates"],ignore_index=True)
        NormalDF = self.FinalDict["Normal_Equips"].fillna(0)
        SuperiorDF = self.FinalDict['Superior_Items'].fillna(0)
        
        NormalDF = NormalDF.astype(int)
        SuperiorDF = SuperiorDF.astype(int)
        
        StarLimitDF.to_csv(CF.APPFOLDER + "CalculationData\\StarLimit.csv")
        SuccessDF.to_csv(  CF.APPFOLDER + "CalculationData\\SFSuccessRates.csv")
        NormalDF.to_csv(   CF.APPFOLDER + "CalculationData\\NormalEquipSF.csv")
        SuperiorDF.to_csv( CF.APPFOLDER + "CalculationData\\SuperiorItemsSF.csv")

        SFLogger.info("Completed CSV export for Starforce tables")
        CF.TimeTaken(self)
        pass

    def GetStarforce(self, content):
        CurrentKey = None
        SkipValue = False
        IgnoreTable = ['Total_Stats', 'Meso_Cost']
        CurrentRecord = "Normal_Equips"
        print("\nGathering {0}:".format(CurrentRecord))
        for element in content:
            cName = element.xpath("name()").get()
            cid = element.xpath(".//span[@class='mw-headline']/@id").get()
            if cName == "h3":
                CurrentRecord = cid
                print("\nGathering {0}:".format(CurrentRecord))
                continue
            if cName == "h4":
                CurrentKey = "".join(cid.split('_')[:2])
                if CF.if_In_String(cid, IgnoreTable):
                    SkipValue = True
                    continue
                SkipValue = False
            if SkipValue:
                continue
            #Skip Condition
            if cName == 'h5':
                SkipValue = True
                if CF.if_In_String(cid, "Total_Stats") and CurrentRecord == "Superior_Items":
                    break
                continue
            if cName == "table":
                if CurrentKey == "StarLimit":
                    self.FinalDict[CurrentKey].append(self.StarLimitTable(CurrentRecord, element))
                elif CurrentKey == "SuccessRates":
                    self.FinalDict[CurrentKey].append(self.SuccessRatesTable(CurrentRecord, element))
                elif CurrentKey == "StatsBoost":
                    self.FinalDict[CurrentRecord]= self.StatsBoostsTable(CurrentRecord, element)
                
        return

    def StarLimitTable(self, title, content):
        Header = CF.removeB(content.xpath(".//tr/th/text()").getall())
        ConsolTable = []
        StarLimitBar = tqdm(total=len(content.xpath(".//tr")), desc="Star Limits")
        for row in content.xpath(".//tr"):
            if CF.removeB(row.xpath(".//text()").getall()) == Header:
                StarLimitBar.total -= 1
                continue
            CRow = {
                "Title" : title
            }
            for i, key in enumerate(Header):
                ctext = row.xpath(".//td[{0}]/text()".format(i+1)).get()
                if CF.if_In_String(key, "Level"):
                    splitv = CF.removeB(CF.replaceN(ctext, ['~', " "], ";").split(";"))
                    if CF.if_In_String(ctext, "and above"):
                        CRow["MinLvl"] =splitv[0]
                        CRow["MaxLvl"] =self.MaxLvl
                        continue
                    CRow["MinLvl"] = splitv[0]
                    CRow["MaxLvl"] = splitv[-1]
                    continue
                CRow[key] = ctext.strip(" \n")
            StarLimitBar.update(1)
            ConsolTable.append(pd.DataFrame(CRow, index=[0]))
        SFLogger.info("Adding StarLimit Table for {0}".format(title))
        StarLimitBar.close()
        return pd.concat(ConsolTable, ignore_index=True)

    def SuccessRatesTable(self, CurrentRecord, content):
        Header = CF.removeB(content.xpath(".//tr/th/text()").getall())
        ConsolTable = []
        SuccessRatesBar = tqdm(total=len(content.xpath(".//tr")), desc="Success Rates")
        for row in content.xpath(".//tr"):
            if CF.removeB(row.xpath(".//text()").getall()) == Header:
                SuccessRatesBar.total -= 1
                continue
            CRow = {
                "Title" :  CurrentRecord
            }
            for i, key in enumerate(Header):
                if CF.if_In_String(key, "("):
                    key = CF.replaceN(key.split('(')[1], ")").strip()
                ctext = row.xpath(".//td[{0}]/text()".format(i+1)).get()
                if ctext == None:
                    CRow[key] = "0"
                    continue
                value = ctext
                if CF.if_In_String(ctext, '★'):
                    splitv = ctext.split('★')
                    value = splitv[0].strip()
                elif CF.if_In_String(ctext, "%"):
                    value = ctext.split("%")[0].strip()
                
                CRow[key] = value
        
            SuccessRatesBar.update(1)
            ConsolTable.append(pd.DataFrame(CRow, index=[0]))

        SuccessRatesBar.close()
        SFLogger.info("Adding Success Rates for {0}".format(CurrentRecord))
        return pd.concat(ConsolTable, ignore_index=True)

    def StatsBoostsTable(self,CurrentRecord, content):

        
        Header = CF.removeB(content.xpath(".//tr/th/text()").getall())
        ConsolTable = []
        StatsPBar = tqdm(total=1, desc="Starforce Stats: ")
        for row in content.xpath("./tbody/tr"):
            if CF.removeB(row.xpath(".//text()").getall()) == Header:
                continue      
            Attempt = row.xpath(".//td[1]/text()").get()
            if Attempt == None:
                continue
            SFIDs = CF.replaceN(Attempt, ['★','→'])
            try:
                if CF.if_In_String(SFIDs, ','):
                    splitv = SFIDs.split(',')
                    SFIDs = [max([int(value.strip()) for value in pair.split(' ') if value != '']) for pair in splitv]
                else:
                    SFIDs = [max(int(value.strip()) for value in SFIDs.split(' ') if value != '')]
            except:
                SFLogger.critical(traceback.format_exc())
            for ids in SFIDs:
                StatsPBar.total += 1
                SFat = {
                    "SFID" : ids,
                    "MinLvl" : 0,
                    "MaxLvl" : self.MaxLvl
                }
                if ids <=15 and CurrentRecord == "Normal_Equips":
                    Stats = CF.removeB(row.xpath(".//td[2] //ul //text()").getall())
                    SFat.update(self.ReturnStatDict(Stats))   
                    ConsolTable.append(pd.DataFrame(SFat, index=[0]))
                else:
                    NewPoint = row.xpath(".//td[2]")
                    CommonStats = CF.removeB(NewPoint.xpath("./child::*[2] //text()").getall())
                    SFat.update(self.ReturnStatDict(CommonStats))
                    for subrow in NewPoint.xpath("./child::*[1] //tr"):
                        DCopy = CF.DeepCopyDict(SFat)
                        MinMaxLvl = subrow.xpath(".//td[1] //text()").get()
                        if MinMaxLvl == None:
                            continue
                        if CF.if_In_String(MinMaxLvl, "~"):
                            Min, Max = MinMaxLvl.split("~")
                            DCopy["MinLvl"] = int(Min)
                            DCopy["MaxLvl"] = int(Max)
                        else:
                            DCopy["MinLvl"] = int(MinMaxLvl.strip(" +"))
                            DCopy["MaxLvl"] = self.MaxLvl
                        Stats = CF.removeB(subrow.xpath(".//td[2] //text()").getall())
                        DCopy.update(self.ReturnStatDict(Stats))
                        ConsolTable.append(pd.DataFrame(DCopy, index=[0]))
                StatsPBar.update(1)
                SFLogger.info("Adding Starforce stat at {0} for {1}".format(ids, CurrentRecord))
        StatsPBar.total -= 1
        SFLogger.info("Adding SF Stats for {0}".format(CurrentRecord))
        StatsPBar.close()
        return pd.concat(ConsolTable, ignore_index=True)

    def ReturnStatDict(self, StatList):
        RDict = {}
        for s in StatList:
            key, value = s.split("+")
            key = CF.replaceN(key, ["'s", "'"]).strip()
            RDict[key] =  int(value.strip(" %"))
        
        return RDict
    
        

#Unuser whether implementation is necessary
class SpellTraceSpider(scrapy.Spider):
    name = "SpellTraceSpider"
    start_urls = ["https://strategywiki.org/wiki/MapleStory/Spell_Trace_and_Star_Force"]
    custom_settings = {
        "LOG_SCRAPED_ITEMS" : False
    }
    def parse(self, response):
        pass
    def close(self):
        pass

class BonusStatSpider(scrapy.Spider):
    name = "BonusStatSpider"
    start_urls = ["https://strategywiki.org/wiki/MapleStory/Bonus_Stats"]
    custom_settings = {
        "LOG_SCRAPED_ITEMS" : False
    }

    def parse(self, response):
        StatTablesContent = response.xpath("//h2//span[@id='Stats']/parent::*/following-sibling::table")
        yield self.GetFlames(StatTablesContent)
        pass

    def close(self):
        CF.TimeTaken(self)
        pass

    def GetFlames(self, content):
        try:
            TrackKey = []
            SpecialConsideration = ["Attack Power", "Magic Attack", "Special Bonus Stats"]
            ConsolTable = []
            for tables in content:
                CDict = {}
                Stat, CDict["EquipGrp"] = self.CleanStat(tables.xpath("./preceding-sibling::h3[1] /span[@class='mw-headline']/text()").get())
                if (Stat, CDict["EquipGrp"]) in TrackKey:
                    continue
                CDict["EquipType"] = "Common"
                if Stat in SpecialConsideration:
                    if CF.if_In_String(Stat, "Attack"):
                        dl = tables.xpath("./preceding-sibling::dl[1] //text()").get()
                        if CDict["EquipGrp"] == "Weapons":
                            if CF.if_In_String(dl, "Normal"):
                                CDict["EquipType"] = "Normal"
                            else:
                                CDict["EquipType"] = "Special"
                    else:
                        Stat, CDict["EquipGrp"] = self.CleanStat(tables.xpath("./preceding-sibling::h4[1] /span[@class='mw-headline']/text()").get())
                CDict['Stat'] = Stat
                ConsolTable.append(self.ReturnTable(tables, CDict))
                
                if CDict["EquipType"] != "Normal":
                    TrackKey.append((Stat, CDict["EquipGrp"]))
                
            Result = pd.concat(ConsolTable, ignore_index=True)
            Result.to_csv("./DefaultData/CalculationData/FlameData.csv")
        except:
            BSLogger.critical(traceback.format_exc())

    
    def ReturnTable(self, tables, CDict):
        ConsolRow = []
        Header = CF.removeB(tables.xpath(".//th/text()").getall())
        if "Equip level" not in Header:
            p = tables.xpath("./preceding-sibling::p[1]/text()").get()
            splitv = p.split('at least level')[-1].strip().split(' ')[0]
            if CF.if_In_String(p, "Not affected") and CF.if_In_String(p, "at least") == False:
                CDict["MinLvl"] = 0
            else:
                CDict["MinLvl"] = int(splitv.strip(" .\n"))
            CDict["MaxLvl"] = MAXLVL

        for row in tables.xpath(".//tr"):
            CRow = CF.DeepCopyDict(CDict)
            CText = CF.removeB(row.xpath(".//text()").getall())
            if CText == Header or CText is None:
                continue

            for i, th in enumerate(Header):
                textAt = row.xpath("./td[{0}]/text()".format(i+1)).get()
                if textAt is None:
                    continue
                if CF.if_In_String(th, "level"):
                    CRow["MinLvl"], CRow["MaxLvl"] = textAt.split("-")
                    continue
                
                CRow[th] = CF.replaceN(textAt, ",").strip(" %\n")
            ConsolRow.append(pd.pd.DataFrame(CRow, index=[0]))
        
        return pd.concat(ConsolRow, ignore_index=True)



    def CleanStat(self, Stat):
        MainStat = ["STR", "DEX", "INT", "LUK"]
        Stat = Stat.encode("ASCII", "ignore").decode()
        Stat = " ".join([ele.strip() for ele in Stat.split("increase")]).strip()
                
        if CF.if_In_String(Stat, "and"):
            Stat = "Mixed Stats"
        if CF.if_In_String(Stat, MainStat):
            Stat = "Main Stats"

        if CF.if_In_String(Stat, "("):
            splitv = Stat.split("(")
            EquipGrp = CF.replaceN(splitv[-1], ")").strip()
            Stat = splitv[0].strip()
        else:
            EquipGrp = "Common"
        Stat = CF.replaceN(Stat, ["%"])

        return Stat, EquipGrp

class HyperStatSpider(scrapy.Spider):
    name = "HyperStatSpider"
    start_urls = ["https://strategywiki.org/wiki/MapleStory/Hyper_Stats"]
    custom_settings = {
        "LOG_SCRAPED_ITEMS" : False
    }
    FinalDict = {
        "Distribution" : None,
        "HyperStats" : [],
        "Cost" : None
    }

    
    def parse(self, response):

        HyperStatDistContent =  response.xpath("//span[@id='Hyper_Stats_Points_Distribution']/parent::*/following-sibling::div[1]//table")
        yield self.HyperStatDistribution(HyperStatDistContent)
        HyperStatContent = response.xpath("//span[@id='Hyper_Stats']/parent::*/following-sibling::table")
        yield self.HyperStat(HyperStatContent)
        
    def close(self):
        DisDF = self.FinalDict["Distribution"]
        HDF = pd.concat(self.FinalDict["HyperStats"], ignore_index=True)
        CDF = self.FinalDict["Cost"]

        DisDF.to_csv(CF.APPFOLDER + "CalculationData\\HyperStatDistribution.csv")
        HDF.to_csv(  CF.APPFOLDER + "CalculationData\\HyperStat.csv")
        CDF.to_csv(  CF.APPFOLDER + "CalculationData\\HyperStatCost.csv")
        CF.TimeTaken(self)
        pass
    
    def HyperStatDistribution(self, content):
        Header = sorted(list(set(CF.removeB(content.xpath(".//tr //th/text()").getall()))))
        ConsolTable = []
        HSDBar = tqdm(total=len(content.xpath(".//tr")),desc="HyperStats Distribution: ")
        for row in content.xpath(".//tr"):
            t = CF.removeB(row.xpath(".//text()").getall())
            if sorted(t) == Header:
                HSDBar.total -= 1
                continue
            CDict = {}
            for i, key in enumerate(Header):
                if CF.if_In_String(key, "Level"):
                    key = key.split('(')[0].strip()
                CDict[key] = CF.replaceN(t[i], ",")
            ConsolTable.append(pd.DataFrame(CDict, index=[0]))
            HSDBar.update(1)
        HSDBar.close()
        self.FinalDict["Distribution"] = pd.concat(ConsolTable, ignore_index=True)
        return

    def HyperStat(self, content):
        try:
            HSBar = tqdm(total=len(content), desc="HyperStats: ")
            for i, table in enumerate(content):
                StatType = table.xpath("./preceding-sibling::h3[1]/span[@class='mw-headline']/text()").get()
                CStat = {}
                if StatType is not None:
                    StatType = CF.replaceN(StatType.encode("ascii","ignore").decode(), ["%", "Weapon and Magic"]).strip()
                    CStat["StatIncrease"] = CF.replaceN(StatType, "Increase").strip()

                Header = CF.removeB(table.xpath(".//tr/th/text()").getall())
                ConsolTable = []
                for row in table.xpath(".//tr"):
                    DCopy = CF.DeepCopyDict(CStat)
                    Ctext = CF.removeB(row.xpath(".//text()").getall())
                    if Ctext == Header or Ctext is None:
                        continue
                    for i, td in enumerate(Header):
                        if CF.if_In_String(td, 'Cost'):
                            td = td.split("Cost")[0].strip() + " Cost"
                        t = row.xpath("./td[{0}]/text()".format(i+1)).get()
                        if CF.if_In_String(t, "+"):
                            t = t.split("+")[-1]
                        if td == "Overall Stat":
                            td = "Overall Effect"
                        DCopy[td] = t.strip(" %\n")
                    ConsolTable.append(pd.DataFrame(DCopy, index=[0]))
                Result = pd.concat(ConsolTable, ignore_index=True)
                if StatType is None:
                    self.FinalDict["Cost"] = Result
                else:
                    self.FinalDict["HyperStats"].append(Result)
                HSBar.update(1)
            HSBar.close()
                    
        except:
            HSLogger.critical(traceback.format_exc())
            
        return

#class FormulaSpider(scrapy.Spider):
