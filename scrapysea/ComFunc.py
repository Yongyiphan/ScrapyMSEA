

from operator import mod
import traceback
import pandas as pd
import CustomLogger
DATABASENAME = "MapleSeaDB"
MAXLVL = 300
MINLVL = 0
APPFOLDER = "C:\\Users\\edgar\\AppData\\Local\\Packages\\MSEA-000f7318-a33f-4024-b59c-7eafe27b8831_h8rqv0gxgvjbt\\LocalState\\ScrapedData\\"


def if_In_String(string, parameter, mode="Any"):
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

def replaceN(string, parameter, replacement = ''):
    if isinstance(parameter, list):
        for p in parameter:
            string = string.replace(p,replacement)
    if isinstance(parameter, str):
        string = string.replace(parameter, replacement)

    
    return string

def removeB(list):
    return [value.strip('\n') for value in list if value.strip(' ') != '\n']

def DeepCopyDict(Base):
        return {key: value for key, value in Base.items()}

def returnPotLevel(lvl):
    lvl = int(lvl)
    if lvl in range(0, 21):
        return 1
    elif lvl in range(21, 41):
        return 2
    elif lvl in range(41, 51):
        return 3
    elif lvl in range(51,71):
        return 4
    elif lvl in range(71, 91):
        return 5
    elif lvl >= 91:
        return 6

def returnSFLevelRank(mode, level):
    
    level = int(level)
    if mode != "Tyrant" :
        if level >= 128 and level <= 137:
            return 5
        elif level >= 138 and level <= 149:
            return 4
        elif level >= 150 and level <= 159:
            return 3
        elif level >= 160 and level <= 199:
            return 2
        elif level >= 200:
            return 1
    else:
        if level >= 0 and level <= 77:
            return 9
        elif level >= 78 and level <= 87:
            return 8
        elif level >= 88 and level <= 97:	
            return 7
        elif level >= 98 and level <= 107:
            return 6
        elif level >= 108 and level <= 117:	
            return 5
        elif level >= 118 and level <= 127:	
            return 4
        elif level >= 128 and level <= 137:
            return 3
        elif level >= 138 and level <= 149:
            return 2
        elif level >= 150:
            return 1
    
def TimeTaken(self):
    start_time = self.crawler.stats.get_value('start_time')
    finish_time = self.crawler.stats.get_value('finish_time')
    print(f"{self.name} crawled in: {finish_time - start_time}")


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



