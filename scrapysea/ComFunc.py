

from operator import mod
import traceback
import pandas as pd
import CustomLogger


def if_In_String(string, paramater, mode="Any"):
    if isinstance(paramater, str):
        if paramater in string:
            return True
    
    if isinstance(paramater, list):
        if mode == "Any":
            if any(para in string for para in paramater):
                return True
        if mode == "All":
            if all(para in string for para in paramater):
                return True
    
    return False



def main():
    Rlog = CustomLogger.Set_Custom_Logger("ReformatInfo", "./Logs/Reformat.log", propagate=False)

    try:
        pd.set_option("display.max_rows", None, "display.max_columns", 50)
        CDF = pd.read_csv('./DefaultData/EquipmentData/EquipSetData.csv')
        ColumnOrder = [
            "EquipSet","ClassType","Set At",
            "STR","DEX","LUK","INT","All Stats","Max HP","Max MP","Perc Max HP","Perc Max MP","Defense",
            "Weapon Attack","Magic Attack","Ignored Enemy Defense","Boss Damage",
            "Critical Damage", "Damage","All Skills","Damage Against Normal Monsters","Abnormal Status Resistance"]

        
        
        CDF = CDF[ColumnOrder]
        
        CDF.fillna(0, inplace=True) 
        CDF.to_csv("TestResult.csv")

    except:
        Rlog.warn(traceback.format_exc())        
    

