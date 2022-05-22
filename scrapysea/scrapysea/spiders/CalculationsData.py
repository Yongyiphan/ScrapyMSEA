import traceback
from pandas import DataFrame
import scrapy
import CustomLogger
from ComFunc import *
from ComFunc import DeepCopyDict

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
    
    def parse(self, response):
        try:
            mainContent = response.xpath("//div[@class='mw-parser-output'] //span[@id='Potentials_List']/parent::h2/following-sibling::div[not(contains(@class, 'nav_box'))]")
            PotentialGrades = mainContent.xpath(".//div[contains(@class,'collapsible-content')]")
            self.Execute(PotentialGrades)
                
        except Exception as E:
            PLogger.critical(traceback.format_exc())
        
    def close(self, reason):
        try:
            PDF = pd.concat(self.PotentialTable['Potential'], ignore_index=True)
            CDF = pd.concat(self.CubeRates["Potential"], ignore_index=True)

            BPDF = pd.concat(self.PotentialTable["Bonus"], ignore_index=True)
            BCDF = pd.concat(self.CubeRates["Bonus"], ignore_index=True)            


            PDF = PDF.fillna(0)
            CDF = CDF.fillna(0)

            BPDF = BPDF.fillna(0)
            BCDF = BCDF.fillna(0)


            PDF.to_csv( "./DefaultData/CalculationData/PotentialData.csv")
            CDF.to_csv( "./DefaultData/CalculationData/PotentialCubeRatesData.csv")
            BPDF.to_csv("./DefaultData/CalculationData/BonusData.csv")
            BCDF.to_csv("./DefaultData/calculationData/BonusCubeRatesData.csv")
        except:
            PLogger.critical(traceback.format_exc())
            
        pass
   
    def Execute(self, PotentialGrades):
        FromDiv = False
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
                Clist = self.PotentialTable[PotentialType]
                ChanceL = self.CubeRates[PotentialType]
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
                        continue
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
                            CDict['Stat'] = CStat.replace("%", "").strip()
                            
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
                                        Clist.append(DataFrame(TempD, index=[0]))
                                        PLogger.info(f"Grabbing {s}: {CDict['DisplayStat']} => {CDict['Stat']}")
                            
                            CDict = DeepCopyDict(PDict)
                            CDict["ChanceTable"] = self.HandleTables(child)
                            ChanceL.append(self.ConsolidateTable(CDict, "ChanceTable"))

                            PLogger.info(f"Adding Chance for {EList}")
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
                    PDcopy[v] = ctext.strip(" +%\n")
                pass
            TableResult.append(PDcopy)
        return TableResult
  
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
                        
            SI = replaceN(SI, ['for']).strip()
            SI = SI[0].upper() + SI[1:]
            PDict["StatT"] = "Perc" if if_In_String(SI, "%") else "Flat"
            PDict['Stat'] = SI.strip()
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
                        StatLists.append(DataFrame(TempD, index=[0]))   
                    try:
                        PLogger.info(f"Grabbing {s}: {Base['DisplayStat']} => {Base['Stat']}")
                    except KeyError:
                        continue
            R = pd.concat(StatLists, ignore_index=True)
            return R
        except Exception as E:
            PLogger.critical(traceback.format_exc())

#class StarforceSpider(scrapy.Spider):

#class HyperStatSpider(scrapy.Spider):

#class FormulaSpider(scrapy.Spider):
