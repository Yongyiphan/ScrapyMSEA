import scrapy
import scrapysea.utils as utils


DLog = utils.Set_Custom_Logger("Dev Spider", logTo="./Logs/Dev.log", propagate=False)


class DevSpider(scrapy.Spider):
    name = "DevSpider"
    ID = "Potential"
    start_urls = [utils.Source_Links[ID]]

    ...
