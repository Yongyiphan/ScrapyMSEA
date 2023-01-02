# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import traceback
from itemadapter import ItemAdapter
import sqlite3
import ComFunc as CF
import os



class ScrapyseaPipeline:
    def process_item(self, item, spider):
        return item


import scrapysea.items as sitems
from scrapy.exporters import CsvItemExporter

def item_type(item):
    return type(item).__name__
import CustomLogger
"""
Pipeline Flow
    - Scrapy scrape
    -> Scrapy Item Loader
    -> Pipeline to DB
        -> TableN ID get from "Destination" key

Fresh Init
-> Create New DB
-> On Exit
    -> IF tableN exist
    -> ELSE Create new empty table --> Insert item

Existing DB / Tables prev filled
__init__:
    Retreive Tables
        TableNames : [(Col Name, Col Type)]


"""
import pandas as pd

class BasePipeline:
    def __init__(self) -> None:
        self.DBLog = CustomLogger.Set_Custom_Logger("DBLogger", logTo ="./Logs/DBLogger.log", propagate=False)
        
class SqliteDBItemPipeline(BasePipeline):
    SqliteDataType = ["INTEGER", "REAL", "TEXT", "BLOB"]
    Mode = "Read"
    #Mode = "Write"
    def __init__(self) -> None:
        try:
            self.con = sqlite3.connect(os.path.join(CF.DBPATH, "Maplestory.db"))
            self.connected = True
        except sqlite3.Error as Error:
            self.connected = False
            print(Error)
        self.cur = self.con.cursor()
        
        #self.DBLog = CustomLogger.Set_Custom_Logger("DBLogger", logTo ="./Logs/DBLogger.log", propagate=False)
        #Containers for scraped data
        self.NewTable = {}
        #Containers for pre-existing data
        self.ExistTable = {}
        try:
            #Get Existing table info
            self.cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            for name in self.cur.fetchall():
                name = name[0]
                #Get Table info
                self.cur.execute("PRAGMA table_info({0})".format(name))
                result = self.cur.fetchall()
                self.TableContainer(self.ExistTable, name)
                temp = self.ExistTable[name]
                for x in result:
                    temp["CT"][x[1]] = x[2]
                    temp["CO"].append(x[1])
                    if x[-1] >= 1:
                        temp["PK"].append(x[1])
                        ...
                    ...
                ...
        except Exception:
            self.DBLog.critical(traceback.format_exc())
        pass

    def close_spider(self, spider):
        #SqliteDB Insert Flow
        """
        Two Types of containers
        Pre-Existing  data Containers:
        Newly scraped data Containers:
        """
        try:
            #Iterate thru all tables
            for k, v in self.NewTable.items():
                DataDF = pd.concat(v["Data"], axis=0, ignore_index=True)
                #DataDF.to_csv(f"{k}.csv")
                self.sqlitetablebuilder(k)
                self.sqlitetableinserter(k, v, DataDF)
                ...
        except Exception:
            self.DBLog.critical(traceback.format_exc())

        self.cur.close()
        self.con.close()
        self.connected = False
        print("Closing: {0}".format(spider.name))
        pass
    def process_item(self, item, spider):
        try:
            TableN = item['Destination']
            del item['Destination']
            if TableN not in self.NewTable:
                self.TableContainer(self.NewTable, TableN)
                self.NewTable[TableN]["Data"] = []
                self.NewTable[TableN]["PK"]   = item.PrimaryKeys

            for k, v in item._values.items():
                if k not in self.NewTable[TableN]["CT"]:
                    self.NewTable[TableN]["CT"][k] = self.sqliteTypeSort(v)
                ...
            cdf = pd.DataFrame(item._values, index=[0])
            self.NewTable[TableN]["Data"].append(cdf)
        except Exception as E:
            self.DBLog.critical(traceback.format_exc())
        return item
    
    def sqlitetablebuilder(self,TableN):
        try:
            if self.connected:
                CTinfo = self.NewTable[TableN]
                if TableN in self.ExistTable:
                    #Update/ Insert
                    #Check if row exists
                    DiffCol = set(CTinfo["CT"]).difference(set(self.ExistTable[TableN]["CT"]))
                    if DiffCol:
                        self.ExistTable[TableN]["CT"].update({k : self.sqliteTypeSort(k) for k in DiffCol})
                        self.ExistTable[TableN]["CO"] = self.ColumnOrganiser(self.ExistTable[TableN]["CT"])
                        ...
                    ...
                else:
                    CTinfo["CO"] = self.ColumnOrganiser(CTinfo["CT"])

                    TCreateStr = "CREATE Table {0} (".format(TableN)
                    TCreateStr += ",".join(CTinfo["CO"])

                    TCreateStr += ", PRIMARY KEY ("
                    TCreateStr += ",".join(CTinfo["PK"])
                    TCreateStr += ")"

                    TCreateStr += ");"
                    self.cur.execute(TCreateStr)
                    self.con.commit()
                    ...
        except Exception as E:
            self.DBLog.critical(traceback.format_exc())
            ...
        ...
    
    def ColumnOrganiser(self, CT):
        CO = sum(CF.REJSON["Dataframe ReCol"].values(),[])
        Parameters = []
        for DCol in CO:
            if len(CT) == len(Parameters):
                return Parameters
            if DCol in CT:
                Parameters.append(DCol)
            ...
        self.DBLog.critical("\n\tColumn Organiser Error\n")
        return []

    def sqlitetableinserter(self, TN, CT, DF):
        #Sort out auto conversion of int to float
        try:
            if self.Mode ==  "Write":
                ColOrder = self.ExistTable[TN]["CO"] if CT["CO"] == [] else CT["CO"]
                #DF = DF[ColOrder]
                InsertStr = "REPLACE INTO {0} (".format(TN)
                for c, t in CT["CT"].items():
                    if t == "INTEGER":
                        DF[c] = DF[c].fillna(0)
                        DF[c] = DF[c].astype(int)
                        continue
                    if c in CF.REJSON["DefaultValues"]:
                        D = CF.REJSON["DefaultValues"][c]
                        DF[c] = DF[c].fillna(CF.REJSON["DefaultValues"][c])
                
                InsertStr += ",".join(ColOrder) + ") "
                for row in DF.to_dict("records"):
                    w = [str(row[x]) for x in ColOrder]
                    ValueStr = InsertStr + "VALUES (" + ", ".join(['"%s"' % str(row[x]) for x in ColOrder]) + ");"
                    try:
                        self.cur.execute(ValueStr)
                        self.con.commit()
                    except Exception as e:
                        self.DBLog.warning(f"Failed to add: {row}")
                        continue
        except Exception:
            self.DBLog.critical(traceback.format_exc())
        return
        ...

    def sqliteTypeSort(self, value):
        if isinstance(value, str):
            return "TEXT"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "REAL"
        return "BLOB"
    
    def TableContainer(self, holder, name):
        if name not in holder:
            holder[name] = {
                "CT"       : {}, #Column name : type
                "CO"       : [], #Column Order
                "PK"       : [], #Primary Key
                "Data"     : None
            }
        ...


    
    
class ItemRenamePipeline(BasePipeline):
    """
    Should only be called from
        TotalEquipment, EquipmentSet

    """
    def __init__(self) -> None:
        pass
        
    def process_item(self, item, spider):
        try:
            ItemDict = item._values
            self.ComplicatedRenamer(ItemDict, spider.name)
        
        except Exception as E:
            self.DBLog.critical(traceback.format_exc())
            self.DBLog.critical(f"Location: {spider.name}")            
        print("Here")
        return item

    def ComplicatedRenamer(self, ItemDict, spidername):
    
        if "ClassType" in ItemDict:
            if "EquipSet" in ItemDict:
                Kek = ItemDict["EquipSet"]
                for k, v in CF.REJSON["Equipment"]["Set"].items():
                    if Kek.lower() in v:
                        Kek = k
                        ...
                    ...
                ...
            if "EquipName" in ItemDict:
                Kekw = ItemDict["EquipName"]
                Kekw = CF.replacen(Kekw, [ItemDict["ClassType"]])
                ...
            ...
        ...
    

