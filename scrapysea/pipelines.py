# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
from itemadapter import ItemAdapter, adapter
import utils as utils
import io


class ExportPipeline:
    Jsons = {}

    def close_spider(self, spider):
        self.file.close()
        for j in self.Jsons:
            j.close()
        ...

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get("Path") and adapter["Category"] not in self.Jsons:
            self.Jsons[adapter["Category"]] = open(adapter["Path"], "w")
        Cat = adapter["Category"]
        del item["Path"]
        del item["Category"]
        line = json.dumps(ItemAdapter(item).asdict()) + "\n"
        self.Jsons[Cat].write(line)
        return item

    ...


class ExpPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get("Level"):
            adapter["Level"] = int(adapter["Level"])

        if adapter.get("Multiplier"):
            cvalue = adapter["Multiplier"]
            if "N/A" in cvalue:
                cvalue = "0.0"
            cvalue = utils.RemoveString(cvalue, ["N/A", "\n"])
            adapter["Multiplier"] = cvalue

        return item

    ...
