# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from webscraper_for_sophie.database_manager import DatabaseManager


class WebscraperForSophiePipeline:

    def open_spider(self, spider):
        """ This method is called when the spider is opened. """
        self.db_manager = DatabaseManager()
        self.db_manager.connect()
        self.db_manager.prep_table()
        spider.load_known_items(self.db_manager.get_known_items())
        spider.last_timestamp = self.db_manager.load_timestamp()
        spider.load_due_for_expiration(self.db_manager.get_due_for_expiration())
        spider.logger.info("Timestamp of last run: %s" % spider.last_timestamp)

    def close_spider(self, spider):
        """ This method is called when the spider is closed. """
        # print stats

        stats = spider.stats
        for key in stats.keys():
            s = stats[key]
            spider.logger.info(key.capitalize())
            spider.logger.info("seen: %d, crawled: %d, new: %d, price changed: %d" % (s["seen"], s["crawled"], s["new"], s["price_changed"]))

        if spider.interesting:
            spider.logger.info("Found %d new interesting items: " % len(spider.interesting))

        # print interesting items
        for item in spider.interesting:
            spider.logger.info("%s\n > [%dEUR / %d m² / #%d] %s" % (item['title'], item['price'], item['size'], item['room_count'], item['url']))

        self.db_manager.store_timestamp()
        self.db_manager.close()

    def process_item(self, item, spider):
        """ This method is called for every item pipeline component. """
        self.db_manager.store_item(item)
        return item
