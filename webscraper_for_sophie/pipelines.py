# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import logging

from webscraper_for_sophie.database_manager import DatabaseManager


class WebscraperForSophiePipeline:

    def open_spider(self, spider):
        """ This method is called when the spider is opened. """
        self.db_manager = DatabaseManager()
        self.db_manager.connect()
        self.db_manager.prep_table()
        spider.store_known_items(self.db_manager.get_known_items())

    def close_spider(self, spider):
        """ This method is called when the spider is closed. """
        # print stats

        stats = spider.stats
        for key in stats.keys():
            s = stats[key]
            logging.info(key.capitalize())
            logging.info("seen: %d, crawled: %d, new: %d, price changed: %d" % (s["seen"], s["crawled"], s["new"], s["price_changed"]))

        self.db_manager.close()

    def process_item(self, item, spider):
        """ This method is called for every item pipeline component. """
        self.db_manager.store_item(item)
        return item
