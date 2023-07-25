import scrapy
import utils
from utils import CustomLogger

DLog = CustomLogger.Set_Custom_Logger("Dev Spider", logTo="Dev.log", propagate=False)


class DevSpider(scrapy.Spider):
    name = "DevSpider"
    ID = "Potential"
    start_urls = [utils.Source_Links[ID]]

    def parse(self, response):
        print(response)
        ...

    ...
