
import sys
import time
import traceback
import pandas as pd
import CustomLogger
DATABASENAME = "MapleSeaDB"
MAXLVL = 300
MINLVL = 0

#APPFOLDER = "C:\\Users\\edgar\\AppData\\Local\\Packages\\MSEA-000f7318-a33f-4024-b59c-7eafe27b8831_h8rqv0gxgvjbt\\LocalState\\ScrapedData\\"

#LOADER

def setPath(destination):
    global APPFOLDER
    APPFOLDER = destination 

def setMseaModule(status):
    global MseaModule
    MseaModule = bool(status)

import json
import os
def LoadRenameJson():
    t = os.listdir()
    with open("./scrapysea/ReName.json", "r") as file:
        global REJSON
        REJSON = json.load(file)
    
    print(REJSON.keys())



#STRING FORMATERS

def instring(string, parameter, mode="Any"):
    if isinstance(parameter, str):
        if parameter in string:
            return True
    
    if isinstance(parameter, list):
        if mode == "Any":
            if any(para in string for para in parameter):
                return True
        if mode == "All":
            if all(para in string for para in parameter):
                return True
    
    return False

def replacen(string, parameter, replacement = ''):
    if string == None:
        return ""
    if isinstance(parameter, list):
        for p in parameter:
            string = string.replace(p,replacement)
    if isinstance(parameter, str):
        string = string.replace(parameter, replacement)

    
    return string

def removebr(list):
    return [value.strip('\n') for value in list if value.strip(' ') != '\n']

def DeepCopyDict(Base):
        return {key: value for key, value in Base.items()}




    
def TimeTaken(self):
    start_time = self.crawler.stats.get_value('start_time')
    finish_time = self.crawler.stats.get_value('finish_time')
    print("{0} crawled in: {1}".format(self.name, finish_time-start_time))
    if(MseaModule):
        sys.stdout.flush()
        time.sleep(1)



def main():
    Rlog = CustomLogger.Set_Custom_Logger("ReformatInfo", "./Logs/Reformat.log", propagate=False)

    try:
        pd.set_option("display.max_rows", None, "display.max_columns", 50)
        CDF = pd.read_csv('./DefaultData/EquipmentData/WeaponData.csv')
        
        
        CDF.loc[CDF["EquipSet"] == "Genesis", "Equipment Set"] = "Lucky"
        CDF.loc[CDF["Equipment Set"] == "0", "Equipment Set"] = "None"
        CDF.to_csv("TestResult.csv")

    except:
        Rlog.warn(traceback.format_exc())        



