

import traceback
import pandas as pd
import CustomLogger


def if_In_String(string, paramater):
    if isinstance(paramater, str):
        if paramater in string:
            return True
    
    if isinstance(paramater, list):
        if any(para in string for para in paramater):
            return True
    
    return False



def main():
    Rlog = CustomLogger.Set_Custom_Logger("ReformatInfo", "./Logs/Reformat.log", propagate=False)

    try:
        pd.set_option("display.max_rows", None, "display.max_columns", 50)
        CDF = pd.read_csv('./DefaultData/EquipmentData/AccessoryData.csv')
        #ColumnOrder = [
        #    "EquipSlot","ClassName","EquipName","Equipment Set",
        #    "Category",
        #    "Level","STR","DEX","LUK","INT","All Stats","Max HP","Max MP","Defense",
        #    "Weapon Attack","Magic Attack","Ignored Enemy Defense","Boss Damage",
        #    "Movement Speed","Jump","Number of Upgrades"
        #]

        #CDF = CDF[ColumnOrder]
        #CDF.rename({
        #    "Equipment Set" :"EquipSet",
        #    "Level" : "EquipLevel"
        #}) 
        #CDF['ClassName'].fillna("Any", inplace=True)
        #CDF['Equipment Set'].fillna("None",inplace=True)
        #CDF.loc[CDF['Category'] == "Uncategorized", "Category"] = "Obtainable"
        #CDF.drop("Number of Upgrades", axis=1, inplace=True)
        #CDF.fillna(0, inplace=True)        

        ColumnOrder = [
        "EquipSlot","ClassName","EquipName","Equipment Set",
        "Category",
        "Level","STR","DEX","LUK","INT","All Stats","Max HP","Max MP","Defense",
        "Weapon Attack","Magic Attack","Ignored Enemy Defense",
        "Movement Speed","Jump",
        "Number of Upgrades",
        "Job",
        "Rank"]
        CDF = CDF[ColumnOrder]
        print(CDF)
        #print(CDF.to_string())
        CDF['ClassName'] = CDF['ClassName'].fillna(CDF['Job'])
        #CDF.loc[CDF["ClassName"].isnull(), "ClassName"] = CDF['Job']
        CDF.drop("Job", axis=1, inplace=True)
        CDF.loc[CDF['Number of Upgrades'] == "None", "Number of Upgrades"] = 0

        
        CDF.rename(columns={
            "Level" : "EquipLevel",
            "Equipment Set" : "EquipSet"
        }, inplace = True)

        
        CDF['Category'].fillna("Obtainable", inplace=True)
        CDF['ClassName'].fillna("Any", inplace=True)
        CDF['EquipSet'].fillna("None",inplace=True)
        
        CDF.fillna(0, inplace=True) 
        CDF.to_csv("TestResult.csv")

    except:
        Rlog.warn(traceback.format_exc())        
    

