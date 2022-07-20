
from tempfile import tempdir
import traceback

from pandas import DataFrame
import scrapy
import CustomLogger
from ComFunc import *

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
            PDF.to_csv( "./DefaultData/CalculationData/PotentialData.csv")
            CDF.to_csv( "./DefaultData/CalculationData/PotentialCubeRatesData.csv")

            BPDF.to_csv("./DefaultData/CalculationData/BonusData.csv")
            BCDF.to_csv("./DefaultData/calculationData/BonusCubeRatesData.csv")
        except:
            PLogger.critical(traceback.format_exc())
        
        TimeTaken(self)
        pass
   
    def Execute(self, PotentialGrades):
        FromDiv = False
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
                    "Grade" : Grade,
                    "Prime" : Prime
                }
                Clist = self.PotentialTable[PotentialType]
                ChanceL = self.CubeRates[PotentialType]
                CDict = {key: value for key, value in PDict.items()}
                for child in PTableGrade.xpath("./child::node()"):
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
                        if if_In_String(ptext.lower(), 'level requirement') == False:
                            continue
                        l = ptext.split(":")[-1].strip().split(' ')[0]
                        CDict["MinLvl"] = int(l)
                        CDict["MaxLvl"] = self.maxLvl
                        CStat = CDict['DisplayStat']
                        if not if_In_String(CStat, 'Increase') and if_In_String(CStat, '%'):
                            fv = CStat.split('%')[0].strip().split(' ')[-1]
                            CStat = ' '.join([value.strip() for value in replaceN(CStat, [f"{fv}%", "of", "+"]).split(' ') if value != ''])
                            CDict['Stat value'] = int(fv)
                            #CDict['Stat'] = CStat.replace("%", "").strip()
                            
                        continue

                    if childName == 'div':
                        FromDiv = True
                        for table in child.xpath(".//div"):
                            PDcopy = DeepCopyDict(CDict)
                            for tchild in table.xpath("./child::node()"):
                                cname = tchild.xpath("name()").get()                                    
                                if cname == None:
                                    continue
                                if cname == 'table':
                                    PDcopy['StatTable'] = self.HandleTables(tchild)
                                    Clist.append(self.ConsolidateTable(PDcopy, "StatTable"))
                                    continue
                                ttext = tchild.xpath('.//text()').get()
                                if if_In_String(ttext.lower(), "cubes"):
                                    ttext = replaceN(ttext, ",",";")
                                    PDcopy['CubeType'] = ttext
                                    continue
                                if if_In_String(ttext.lower(), "chance"):
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
                                        TempD = DeepCopyDict(CDict)
                                        TempD["Slot"] = s
                                        NTempD = self.ReorgStatValue(TempD)
                                        Clist.append(DataFrame(NTempD, index=[0]))
                                        #PLogger.info(f"Grabbing {s}: {CDict['DisplayStat']} => {CDict['Stat']}")
                                        #PLogger.info(f"Grabbing {s}: {CDict['DisplayStat']}")
                            
                            CDict = DeepCopyDict(PDict)
                            CDict["ChanceTable"] = self.HandleTables(child)
                            ChanceL.append(self.ConsolidateTable(CDict, "ChanceTable"))

                            #PLogger.info(f"Adding Chance for {EList}: {CDict['DisplayStat']}")
                            #Resets CDict
                            CDict = DeepCopyDict(PDict)
                            FromDiv = False
                        else:
                            CDict["StatTable"] = self.HandleTables(child)
                        continue

            except Exception as E:
                PLogger.critical(traceback.format_exc())    
        return   
 
    
    def HandleTables(self, context):
        header =  removeB(context.xpath(".//th /text()").getall())
        TableResult = []
        for row in context.xpath(".//tr"):
            text = removeB(row.xpath(".//text()").getall())
            if text == [] or text == header or if_In_String(" ".join(text), "GMS"):
                continue
            PDcopy = {}
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
                    if if_In_String(ctext, '('):
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
                        
            SI = replaceN(SI, ['for']).strip()
            SI = SI[0].upper() + SI[1:]
            PDict["StatT"] = "Perc" if if_In_String(SI, "%") else "Flat"
            #PDict['Stat'] = SI.strip()
        except Exception as E:
            PLogger.critical(traceback.format_exc())        
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
                        TempD = DeepCopyDict(Base)
                        TempD['Slot'] = s
                        TempD = self.UpdateTableDict(TempD, row)
                        del TempD[key]
                        if key == "StatTable":
                            TempD = self.ReorgStatValue(TempD)
                        
                        if "Stat value" in TempD.keys() and "StatT" in TempD.keys():
                            if if_In_String(TempD["Stat value"], "%"):
                                TempD['Stat value'] = replaceN(TempD["Stat value"],'%').strip()
                                if TempD['StatT'] != "Perc":
                                    TempD['StatT'] = "Perc"
                        
                        StatLists.append(DataFrame(TempD, index=[0]))   
                try:
                    #PLogger.info(f"Grabbing {s}: {Base['DisplayStat']} => {Base['Stat']}")
                    PLogger.info(f"Grabbing {Base['Slot']} - {key} for : {Base['DisplayStat']}")
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
        
        StarLimitDF.to_csv( "./DefaultData/CalculationData/StarLimit.csv")
        SuccessDF.to_csv( "./DefaultData/CalculationData/SFSuccessRates.csv")
        NormalDF.to_csv( "./DefaultData/CalculationData/NormalEquipSF.csv")
        SuperiorDF.to_csv( "./DefaultData/CalculationData/SuperiorItemsSF.csv")

        SFLogger.info("Completed CSV export for Starforce tables")
        TimeTaken(self)
        pass

    def GetStarforce(self, content):
        CurrentKey = None
        SkipValue = False
        IgnoreTable = ['Total_Stats', 'Meso_Cost']
        CurrentRecord = "Normal_Equips"
        for element in content:
            cName = element.xpath("name()").get()
            cid = element.xpath(".//span[@class='mw-headline']/@id").get()
            if cName == "h3":
                CurrentRecord = cid
                continue
            if cName == "h4":
                CurrentKey = "".join(cid.split('_')[:2])
                if if_In_String(cid, IgnoreTable):
                    SkipValue = True
                    continue
                SkipValue = False
            if SkipValue:
                continue
            #Skip Condition
            if cName == 'h5':
                SkipValue = True
                if if_In_String(cid, "Total_Stats") and CurrentRecord == "Superior_Items":
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
        Header = removeB(content.xpath(".//tr/th/text()").getall())
        ConsolTable = []
        for row in content.xpath(".//tr"):
            if removeB(row.xpath(".//text()").getall()) == Header:
                continue
            CRow = {
                "Title" : title
            }
            for i, key in enumerate(Header):
                ctext = row.xpath(f".//td[{i+1}]/text()").get()
                if if_In_String(key, "Level"):
                    splitv = removeB(replaceN(ctext, ['~', " "], ";").split(";"))
                    if if_In_String(ctext, "and above"):
                        CRow["MinLvl"] =splitv[0]
                        CRow["MaxLvl"] =self.MaxLvl
                        continue
                    CRow["MinLvl"] = splitv[0]
                    CRow["MaxLvl"] = splitv[-1]
                    continue
                CRow[key] = ctext.strip(" \n")
            ConsolTable.append(DataFrame(CRow, index=[0]))
        SFLogger.info(f"Adding StarLimit Table for {title}")
        return pd.concat(ConsolTable, ignore_index=True)

    def SuccessRatesTable(self, CurrentRecord, content):
        Header = removeB(content.xpath(".//tr/th/text()").getall())
        ConsolTable = []
        for row in content.xpath(".//tr"):
            if removeB(row.xpath(".//text()").getall()) == Header:
                continue
            CRow = {
                "Title" :  CurrentRecord
            }
            for i, key in enumerate(Header):
                if if_In_String(key, "("):
                    key = replaceN(key.split('(')[1], ")").strip()
                ctext = row.xpath(f".//td[{i+1}]/text()").get()
                if ctext == None:
                    CRow[key] = "0"
                    continue
                value = ctext
                if if_In_String(ctext, '★'):
                    splitv = ctext.split('★')
                    value = splitv[0].strip()
                elif if_In_String(ctext, "%"):
                    value = ctext.split("%")[0].strip()
                
                CRow[key] = value
        
            ConsolTable.append(DataFrame(CRow, index=[0]))

        SFLogger.info(f"Adding Success Rates for {CurrentRecord}")
        return pd.concat(ConsolTable, ignore_index=True)

    def StatsBoostsTable(self,CurrentRecord, content):

        
        Header = removeB(content.xpath(".//tr/th/text()").getall())
        ConsolTable = []
        for row in content.xpath("./tbody/tr"):
            if removeB(row.xpath(".//text()").getall()) == Header:
                continue      
            Attempt = row.xpath(".//td[1]/text()").get()
            if Attempt == None:
                continue
            SFIDs = replaceN(Attempt, ['★','→'])
            try:
                if if_In_String(SFIDs, ','):
                    splitv = SFIDs.split(',')
                    SFIDs = [max([int(value.strip()) for value in pair.split(' ') if value != '']) for pair in splitv]
                else:
                    SFIDs = [max(int(value.strip()) for value in SFIDs.split(' ') if value != '')]
            except:
                SFLogger.critical(traceback.format_exc())
            for ids in SFIDs:
                SFat = {
                    "SFID" : ids,
                    "MinLvl" : 0,
                    "MaxLvl" : self.MaxLvl
                }
                if ids <=15 and CurrentRecord == "Normal_Equips":
                    Stats = removeB(row.xpath(".//td[2] //ul //text()").getall())
                    SFat.update(self.ReturnStatDict(Stats))   
                    ConsolTable.append(DataFrame(SFat, index=[0]))
                else:
                    NewPoint = row.xpath(".//td[2]")
                    CommonStats = removeB(NewPoint.xpath("./child::*[2] //text()").getall())
                    SFat.update(self.ReturnStatDict(CommonStats))
                    for subrow in NewPoint.xpath("./child::*[1] //tr"):
                        DCopy = DeepCopyDict(SFat)
                        MinMaxLvl = subrow.xpath(".//td[1] //text()").get()
                        if MinMaxLvl == None:
                            continue
                        if if_In_String(MinMaxLvl, "~"):
                            Min, Max = MinMaxLvl.split("~")
                            DCopy["MinLvl"] = int(Min)
                            DCopy["MaxLvl"] = int(Max)
                        else:
                            DCopy["MinLvl"] = int(MinMaxLvl.strip(" +"))
                            DCopy["MaxLvl"] = self.MaxLvl
                        Stats = removeB(subrow.xpath(".//td[2] //text()").getall())
                        DCopy.update(self.ReturnStatDict(Stats))
                        ConsolTable.append(DataFrame(DCopy, index=[0]))
                SFLogger.info(f"Adding Starforce stat at {ids} for {CurrentRecord}")

        SFLogger.info(f"Adding SF Stats for {CurrentRecord}")
        return pd.concat(ConsolTable, ignore_index=True)

    def ReturnStatDict(self, StatList):
        RDict = {}
        for s in StatList:
            key, value = s.split("+")
            key = replaceN(key, ["'s", "'"]).strip()
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
        TimeTaken(self)
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
                    if if_In_String(Stat, "Attack"):
                        dl = tables.xpath("./preceding-sibling::dl[1] //text()").get()
                        if CDict["EquipGrp"] == "Weapons":
                            if if_In_String(dl, "Normal"):
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
        Header = removeB(tables.xpath(".//th/text()").getall())
        if "Equip level" not in Header:
            p = tables.xpath("./preceding-sibling::p[1]/text()").get()
            splitv = p.split('at least level')[-1].strip().split(' ')[0]
            if if_In_String(p, "Not affected") and if_In_String(p, "at least") == False:
                CDict["MinLvl"] = 0
            else:
                CDict["MinLvl"] = int(splitv.strip(" .\n"))
            CDict["MaxLvl"] = MAXLVL

        for row in tables.xpath(".//tr"):
            CRow = DeepCopyDict(CDict)
            CText = removeB(row.xpath(".//text()").getall())
            if CText == Header or CText is None:
                continue

            for i, th in enumerate(Header):
                textAt = row.xpath(f"./td[{1+i}]/text()").get()
                if textAt is None:
                    continue
                if if_In_String(th, "level"):
                    CRow["MinLvl"], CRow["MaxLvl"] = textAt.split("-")
                    continue
                
                CRow[th] = replaceN(textAt, ",").strip(" %\n")
            ConsolRow.append(DataFrame(CRow, index=[0]))
        
        return pd.concat(ConsolRow, ignore_index=True)



    def CleanStat(self, Stat):
        MainStat = ["STR", "DEX", "INT", "LUK"]
        Stat = Stat.encode("ASCII", "ignore").decode()
        Stat = " ".join([ele.strip() for ele in Stat.split("increase")]).strip()
                
        if if_In_String(Stat, "and"):
            Stat = "Mixed Stats"
        if if_In_String(Stat, MainStat):
            Stat = "Main Stats"

        if if_In_String(Stat, "("):
            splitv = Stat.split("(")
            EquipGrp = replaceN(splitv[-1], ")").strip()
            Stat = splitv[0].strip()
        else:
            EquipGrp = "Common"
        Stat = replaceN(Stat, ["%"])

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

        DisDF.to_csv( "./DefaultData/CalculationData/HyperStatDistribution.csv")
        HDF.to_csv( "./DefaultData/CalculationData/HyperStat.csv")
        CDF.to_csv( "./DefaultData/CalculationData/HyperStatCost.csv")
        TimeTaken(self)
        pass
    
    def HyperStatDistribution(self, content):
        Header = sorted(list(set(removeB(content.xpath(".//tr //th/text()").getall()))))
        ConsolTable = []
        for row in content.xpath(".//tr"):
            t = removeB(row.xpath(".//text()").getall())
            if sorted(t) == Header:
                continue
            CDict = {}
            for i, key in enumerate(Header):
                if if_In_String(key, "Level"):
                    key = key.split('(')[0].strip()
                CDict[key] = replaceN(t[i], ",")
            ConsolTable.append(DataFrame(CDict, index=[0]))
        self.FinalDict["Distribution"] = pd.concat(ConsolTable, ignore_index=True)
        return

    def HyperStat(self, content):
        try:
            for i, table in enumerate(content):
                StatType = table.xpath("./preceding-sibling::h3[1]/span[@class='mw-headline']/text()").get()
                CStat = {}
                if StatType is not None:
                    StatType = replaceN(StatType.encode("ascii","ignore").decode(), ["%", "Weapon and Magic"]).strip()
                    CStat["StatIncrease"] = replaceN(StatType, "Increase").strip()

                Header = removeB(table.xpath(".//tr/th/text()").getall())
                ConsolTable = []
                for row in table.xpath(".//tr"):
                    DCopy = DeepCopyDict(CStat)
                    Ctext = removeB(row.xpath(".//text()").getall())
                    if Ctext == Header or Ctext is None:
                        continue
                    for i, td in enumerate(Header):
                        if if_In_String(td, 'Cost'):
                            td = td.split("Cost")[0].strip() + " Cost"
                        t = row.xpath(f"./td[{1+i}]/text()").get()
                        if if_In_String(t, "+"):
                            t = t.split("+")[-1]
                        if td == "Overall Stat":
                            td = "Overall Effect"
                        DCopy[td] = t.strip(" %\n")
                    ConsolTable.append(DataFrame(DCopy, index=[0]))
                Result = pd.concat(ConsolTable, ignore_index=True)
                if StatType is None:
                    self.FinalDict["Cost"] = Result
                else:
                    self.FinalDict["HyperStats"].append(Result)
                
                    
        except:
            HSLogger.critical(traceback.format_exc())
            
        return

#class FormulaSpider(scrapy.Spider):
