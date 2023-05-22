#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This class defines how the willhaben website will be crawled
"""

# default python packages
import datetime
from datetime import datetime as dt
import re

# from IPython import embed

# installed packages
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
# project modules
from webscraper_for_sophie.items import CondoItem

import json


class WillhabenSpider(scrapy.Spider):
    """
    Spider‘s are classes which define how a certain site (or a group of sites)
    will be scraped, including how to perform the crawl (i.e. follow links) and
    how to extract structured data from their pages (i.e. scraping items)
    Here is a summary of the most important spider attributes. A detailed
    documentation can be found in the `official Scrapy documentation
    """
    
    CRUMBS = { "mietwohnungen" : ["rent", "flat"],
               "haus-mieten" : ["rent", "house"],
               "eigentumswohnung" : ["sale", "flat"],
               "haus-kaufen" : ["sale", "house"] }

    # Graz flats for rent
    START_URL_TEMPLATE = 'https://www.willhaben.at/iad/immobilien/%s/steiermark/graz/?rows=90'
    ITEM_URL_REGEX_TEMPLATE = r"/immobilien/d/%s/steiermark/graz/"

    WILLHABEN_CODE_REGEX = re.compile(r"""\d{6,}""")

    ITEM_PRICE_DATA_CLASS = "search-result-entry-price-%s"

    PRICE_REGEX = re.compile(r"""\d+""")

    DATE_FORMAT_STRING = "%d.%m.%Y, %H:%M Uhr"
    DATETIME_OFFSET_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S%z"

    COMMISSION_REGEX = re.compile(r"""provi([a-z]+)frei|privatperson|bezahlt der abgeber""", flags=re.I)

    NO_DURATION_REGEX = re.compile(r"""keine|unbefristet""", flags=re.I)

    DURATION_SUFFIX_REGEX = re.compile(r""" Jahr\(e\)$| Jahre$| Jahr$""")

    ALTBAU_REGEX = re.compile(r"""altbau|neubau""", flags=re.I)

    ITEM_IMG_REGEX = r'"referenceImageUrl":"(https:\/\/cache.willhaben.at[-a-zA-Z0-9@:%._\+~#=/]+)"'

    BASE_URL = "https://www.willhaben.at"
    name = 'willhaben'
    allowed_domains = ['willhaben.at']

    def __init__(self):
        super()
        self.start_urls = [ self.START_URL_TEMPLATE % crumb for crumb in self.CRUMBS.keys() ]
        self.stats = {"rent flats":  {"seen": 0, "crawled": 0, "new": 0, "price_changed": 0, "stop": False},
                      "sale flats":  {"seen": 0, "crawled": 0, "new": 0, "price_changed": 0, "stop": False},
                      "rent houses": {"seen": 0, "crawled": 0, "new": 0, "price_changed": 0, "stop": False},
                      "sale houses": {"seen": 0, "crawled": 0, "new": 0, "price_changed": 0, "stop": False}}
        self.interesting = []
        self.due_for_expiration = []
        self.last_timestamp = dt(1970, 1, 1)

    def get_type(self, response):
        url = response.url

        crumb = url.split("/")[5]
        value = self.CRUMBS.get(crumb)
        if value:
            rent_sale, type = value
            return re.compile(self.ITEM_URL_REGEX_TEMPLATE % crumb), rent_sale, type
        raise ValueError("Could not determine item from for url %s" % url)

    def load_known_items(self, known_items):
        self.known_items = known_items
        self.logger.debug("Got %d known items" % len(known_items))

    def load_due_for_expiration(self, due_items):
        self.due_for_expiration = due_items

        if len(due_items):
            self.logger.info("Got %d items due for expiration" % len(known_items))

    def parse(self, response):
        """
        This is the default callback used by Scrapy to process downloaded
        responses, when their requests don’t specify a callback like the
        `start_urls`
        """

        # inject items which are due for expiration, this is DIRTY
        for item in self.due_for_expiration:
            yield scrapy.Request(item['url'], self.check_due_item, meta={"id":item['id']})

        self.due_for_expiration = []

        # get the next page of the list
        soup = BeautifulSoup(response.text, 'lxml')

        item_url_regex, rent_sale, item_type = self.get_type(response)
        stats_key = "%s %ss" % (rent_sale, item_type)

        # get item urls and yield a request for each item
        relative_item_urls = re.findall(item_url_regex, response.text)

        data_script_tag = soup.find(id="__NEXT_DATA__")
        if data_script_tag:
            self.logger.debug("Found NEXT_DATA script tag")
            data_script_data = data_script_tag.string
            try:
                full_data = json.loads(data_script_data)
            except:
                self.logger.error("Failed to parse NEXT_DATA json data")
                raise

            page_props = full_data["props"]["pageProps"]

            if page_props.get("advertDetails") is not None:
                item['has_json_details'] = True


            ad_list = page_props["searchResult"]["advertSummaryList"]["advertSummary"]

            for (k, ad) in enumerate(ad_list):

                if self.stats[stats_key]["stop"]:
                    self.logger.info("Reached end of last run for %s" % stats_key)
                    return

                self.stats[stats_key]["seen"] += 1

                willhaben_code = str(ad["id"])

                attrs = ad["attributes"]["attribute"]

                # get price and publication date from the attributes
                url = None
                pub_date = None
                current_price = -1

                skip = False
                missing_attr = 3

                for attr in attrs:
                    value = attr["values"][0]

                    if attr["name"] == "SEO_URL":
                        url = "%s/iad/%s" % (self.BASE_URL, value)
                        if not item_url_regex.search(url):
                            skip = True
                            break
                        missing_attr -= 1
                    if attr["name"] == "PUBLISHED_String":
                        pub_date = dt.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                        missing_attr -= 1
                    if attr["name"] == "PRICE":
                        try:
                            current_price = int(float(value))
                        except:
                            current_price = -1
                        missing_attr -= 1

                    if missing_attr == 0:
                        break

                if skip:
                    continue

                collected_attrs = [url, pub_date, current_price]
                if None in collected_attrs:
                    self.logger.error("Failed to extract data from JSON for item %s, %s", willhaben_code, collected_attrs)
                    continue

                # Check if the item was published or updated after our last visit

                if pub_date < self.last_timestamp:

                    if k == len(ad_list)-1:
                        # This is the last item on the page,
                        # we can assume that items on the next pages are older
                        # and stop the scraping here
                        self.logger.info("Reached last known item for '%s'" % stats_key)
                        self.stats[stats_key]["stop"] = True
                        return

                    continue

                # check for matching item in the database
                known_price = self.known_items.get(willhaben_code, None)
                if known_price is not None:

                    if known_price == current_price:
                        # self.logger.info("price for item %s (%d EUR) did not change, skipping" % (willhaben_code, current_price_int))
                        continue
                    else:
                        self.logger.info("price for item %s (%d → %d EUR) changed!" % (willhaben_code, known_price, current_price))
                        self.stats[stats_key]["price_changed"] += 1
                else:
                    self.stats[stats_key]["new"] += 1
                    self.logger.info("item '%s' is unknown" % (willhaben_code))

                self.stats[stats_key]["crawled"] += 1
                self.logger.debug("queueing %s" % (url))
                yield scrapy.Request(url, self.parse_item, meta={"willhaben_code": willhaben_code, "item_type":item_type, "rent_sale": rent_sale, "stats_key": stats_key})

        else:
            self.logger.error("Did not find NEXT_DATA script tag!")

        pagination_btn = soup.find(
            'a', attrs={"data-testid": "pagination-top-next-button"})
        try:
            next_page_url = self.BASE_URL + pagination_btn['href']
        except:
            return
        yield scrapy.Request(next_page_url, self.parse)

    def check_due_item(self, response):
        """returns/yields a :py:class:`WillhabenItem`.

        This is the callback used by Scrapy to parse downloaded item pages.
        """
        item = CondoItem()
        item.set_default_values()
        item['url'] = response.url

        now = dt.now()

        item['discovery_date'] = now.strftime("%Y-%m-%d")
        item['discovery_timestamp'] = int(now.timestamp())
        item['sql_id'] = response.meta['id']
        item['willhaben_code'] = response.meta['willhaben_code']

        if response.status == 301 and '/d/' not in item['url']:
            # The request has been redirected to the list page,
            # this means that the item has expired.
            item['expiry_date'] = item['discovery_date']
            yield item

    def parse_item(self, response):
        """returns/yields a :py:class:`WillhabenItem`.

        This is the callback used by Scrapy to parse downloaded item pages.
        """
        item = CondoItem()
        item.set_default_values()
        item['url'] = response.url

        now = dt.now()

        item['discovery_date'] = now.strftime("%Y-%m-%d")
        item['discovery_timestamp'] = int(now.timestamp())

        if response.status == 301 and '/d/' not in item['url']:
            # The request has been redirected to the list page,
            # this means that the item has expired.
            item['expiry_date'] = item['discovery_date']
            yield item

        item['type'] = response.meta["item_type"]
        item['rent_sale'] = response.meta["rent_sale"]

        stats_key = response.meta["stats_key"]

        # time could also be added if needed: "%Y-%m-%d %H:%M:%S"

        soup = BeautifulSoup(response.text, 'lxml')
        # # remove all script tags from soup
        # for s in soup('script'):
        #     s.clear()

        data_script_tag = soup.find(id="__NEXT_DATA__")

        if data_script_tag:
            self.logger.debug("Found NEXT_DATA script tag")
            data_script_data = data_script_tag.string

            full_data = json.loads(data_script_data)
            page_props = full_data["props"]["pageProps"]

            if page_props.get("advertDetails") is not None:

                self.logger.debug("Item has JSON ad details")

                item['has_json_details'] = True

                details = page_props["advertDetails"]
                for d in details["attributes"]["attribute"]:
                    if d["name"] == "CONTACT/EMAIL":
                        self.logger.info("Found e-mail: %s" % d["values"][0])

                start_date = dt.strptime(details["startDate"], self.DATETIME_OFFSET_FORMAT_STRING)
                item['discovery_date'] = start_date.strftime("%Y-%m-%d %H:%M:%S")
                item['discovery_timestamp'] = int(now.timestamp())

            # try:
            # except:
            #     self.logger.error("Failed to parse NEXT_DATA json data for item %s" % item['url'])

        # title
        title_tag = soup.find('h1')
        if title_tag:
            item['title'] = title_tag.get_text()
        else:
            self.logger.error("title element not found on page " + item['url'])

        body_tag = soup.find('article')

        # price
        price_tag = soup.find(
            'span', attrs={"data-testid": "contact-box-price-box-price-value-0"})
        main_price_missing = False
        while True:
            if price_tag:
                visible_price_text = price_tag.get_text()
                item.parse_price(visible_price_text)

                # if parsing failed, check the label, which contains the price
                # if the item is currently reserved
                if item['current_price'] == -1 and not main_price_missing:
                    main_price_missing = True
                    self.logger.warn("main price missing for item %s" % item['url'])
                    price_tag = soup.find(
                        'span', attrs={"data-testid": "contact-box-price-box-price-label-0"})
                else:
                    break
            else:
                self.logger.error("price element not found on page " + item['url'])
                break

        # size
        size_tag = soup.find(
            'div', attrs={"data-testid": "ad-detail-teaser-attribute-0"})
        if size_tag:
            visible_size_text = size_tag.get_text()
            item.parse_size(visible_size_text)
        else:
            self.logger.error("size element not found on page " + item['url'])

        # room_count
        room_count_tag = soup.find(
            'div', attrs={"data-testid": "ad-detail-teaser-attribute-1"})
        if room_count_tag:
            room_count_text = room_count_tag.get_text()
            item.parse_room_count(room_count_text)
        else:
            self.logger.error(
                "room_count element not found on page " + item['url'])

        # alternative size and room count parsing (from attributes)
        attribute_tags = soup.findAll(
            'li', attrs={"data-testid": "attribute-item"})
        if attribute_tags:
            for attribute_tag in attribute_tags:
                attribute_text = attribute_tag.get_text()
                # parse size again if zero
                if item['size'] == 0:
                    item.parse_size_2(attribute_text)
                # parse room_count again if zero
                if item['room_count'] == 0:
                    item.parse_room_count_2(attribute_text)
        else:
            self.logger.error(
                "attribute elements not found on page " + item['url'])

        # energy info
        energy_tag = soup.find(
            'div', attrs={"data-testid": "energy-pass-box"})
        if energy_tag:
            info = ""
            i = 0
            while True:
                label_tag = energy_tag.find(
                        'span', attrs={"data-testid": "energy-pass-attribute-label-%d" % (i)})
                value_tag = energy_tag.find(
                        'span', attrs={"data-testid": "energy-pass-attribute-value-%d" % (i)})
                if label_tag and value_tag:
                    if len(info) > 0:
                        info += ", "
                    label = label_tag.get_text().replace(" ", "").replace(":", "")
                    value = value_tag.get_text()
                    info += '"%s": "%s"' % (label, value)

                    if "HWB(kWh" in label:
                        item['heating_consumption'] = float(value.replace(',', '.'))

                    i += 1
                else:
                    break
            item['energy_info'] = info
        else:
            self.logger.warning(
                "energy info element not found on page " + item['url'])

        # features info
        features_title_tag = soup.find('h2', string="Objektinformationen")
        if features_title_tag:
            features_tags = features_title_tag.next_sibling.findAll('li', attrs={"data-testid": "attribute-item"})
            info = ""
            for ft in features_tags:
                label_tag = ft.find("div", attrs={"data-testid":"attribute-title"}).text
                value_tag = ft.find("div", attrs={"data-testid":"attribute-value"}).text
                if label_tag and value_tag:
                    if len(info) > 0:
                        info += ", "
                    info += '"%s": "%s"' % (label_tag, value_tag)
                    if label_tag == "Befristung":
                        if self.NO_DURATION_REGEX.search(value_tag):
                            item['contract_duration'] = '0'
                        else:
                            item['contract_duration'] = self.DURATION_SUFFIX_REGEX.sub("", value_tag)
                    elif label_tag == "Bautyp":
                        item['construction_type'] = value_tag.lower()
            item['features_info'] = info
            if (not item['construction_type']) and body_tag:
                m = self.ALTBAU_REGEX.search(body_tag.get_text())
                if m:
                    item['construction_type'] = m[0].lower()
        else:
            self.logger.warning(
                "features info element not found on page " + item['url'])

        # address, postal_code and district
        location_address_tag = soup.find(
            'div', attrs={"data-testid": "object-location-address"})
        if location_address_tag:
            location_address_text = location_address_tag.get_text()
            # parse address
            item['address'] = location_address_text
            # parse postal_code
            match = re.search(r'8\d\d\d', location_address_text)
            if match:
                item['postal_code'] = match[0]  # The entire match
            else:
                self.logger.error(
                    "postal_code parsing failed on page " + item['url'])
            # parse district
            match = re.search(r'8\d\d\d ([^,]+)', location_address_text)
            if match:
                item['district'] = match[1]  # The first group
            else:
                self.logger.error(
                    "district parsing failed on page " + item['url'])
        else:
            self.logger.error("element for address, postal_code and district " +
                          "not found on page " + item['url'])

        # willhaben_code
        willhaben_code_tag = soup.find(
            'span', attrs={"data-testid": "ad-detail-ad-id"})
        if willhaben_code_tag:
            willhaben_code_text = willhaben_code_tag.get_text()
            match = re.search(r'\d+', willhaben_code_text)
            if match:
                item['willhaben_code'] = match[0]  # The first group
            else:
                self.logger.error(
                    "willhaben_code parsing failed on page " + item['url'])
        else:
            self.logger.error(
                "willhaben_code element not found on page " + item['url'])

        # edit_date
        edit_date_tag = soup.find(
            'span', attrs={"data-testid": "ad-detail-ad-edit-date"})
        if edit_date_tag:
            item['edit_date'] = dt.strptime(edit_date_tag.get_text(), self.DATE_FORMAT_STRING)
            if item['edit_date'] < self.last_timestamp:
                self.stats[stats_key]["stop"] = True

        else:
            self.logger.error("edit_date element not found on page " + item['url'])

        # private seller
        if body_tag:
            item['seller_is_private'] = body_tag.find('div', attrs={"data-testid": "ad-detail-contact-box-private-top"}) is not None

        # commission_fee
        if body_tag:
            body_text = body_tag.get_text()
            if self.COMMISSION_REGEX.search(body_text):
                item['commission_fee'] = 0
            else:
                item['commission_fee'] = 1
        else:
            self.logger.error(
                "commission_fee element not found on page " + item['url'])

        # price_per_m2
        item.calc_price_per_m2()

        if item['rent_sale'] == 'rent' and item['room_count'] > 3 and item['current_price'] < 1700 and not item['commission_fee']:
            self.interesting.append({"title": item['title'], "url": item['url'], "price": item['current_price'], "size": item['size'], "room_count": item['room_count']})

        # futher item processing is done in the item pipeline
        yield item
