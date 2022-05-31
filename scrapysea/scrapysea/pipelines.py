# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import traceback
from itemadapter import ItemAdapter
import sqlite3
import CustomLogger



class ScrapyseaPipeline:
    def process_item(self, item, spider):
        return item




