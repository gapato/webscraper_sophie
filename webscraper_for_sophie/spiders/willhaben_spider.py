#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This class defines how the willhaben website will be crawled
"""

# default python packages
import datetime
import re
import logging
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
    
    # crumbs = ["mietwohnungen", "haus-mieten", "eigentumswohnung", "haus-kaufen"]

    # Graz flats for rent
    START_URL_FLATS = 'https://www.willhaben.at/iad/immobilien/mietwohnungen/steiermark/graz/?rows=90'
    ITEM_URL_REGEX_SHORT_FLATS = 'https://www.willhaben.at/iad/immobilien/d/mietwohnungen/steiermark/graz/'
    ITEM_URL_REGEX_FLATS = r"\"url\":\"(\/iad\/immobilien\/d\/mietwohnungen\/steiermark\/graz\/[a-z,A-Z,0-9,-]+\/)\""

    # # Graz houses for rent
    START_URL_HOUSES = 'https://www.willhaben.at/iad/immobilien/haus-mieten/steiermark/graz/?rows=90'
    ITEM_URL_REGEX_SHORT_HOUSES = 'https://www.willhaben.at/iad/immobilien/d/haus-mieten/steiermark/graz/'
    ITEM_URL_REGEX_HOUSES = r"\"url\":\"(\/iad\/immobilien\/d\/haus-mieten\/steiermark\/graz\/[a-z,A-Z,0-9,-]+\/)\""

    WILLHABEN_CODE_REGEX = re.compile(r"""\d{6,}""")

    ITEM_PRICE_DATA_CLASS = "search-result-entry-price-%s"

    # Graz flats for sale
    # START_URL = 'https://www.willhaben.at/iad/immobilien/eigentumswohnung/steiermark/graz/'
    # ITEM_URL_REGEX = r"\"url\":\"(\/iad\/immobilien\/d\/eigentumswohnung\/steiermark\/graz\/[a-z,A-Z,0-9,-]+\/)\""

    # # Graz-Umgebung
    # START_URL = 'https://www.willhaben.at/iad/immobilien/eigentumswohnung/steiermark/graz-umgebung/'
    # ITEM_URL_REGEX = r"\"url\":\"(\/iad\/immobilien\/d\/eigentumswohnung\/steiermark\/graz-umgebung\/[a-z,A-Z,0-9,-]+\/)\""

    PRICE_REGEX = re.compile(r"""\d+""")

    DATE_FORMAT_STRING = "%d.%m.%Y, %H:%M Uhr"

    COMMISSION_REGEX = re.compile(r"""provi([a-z]+)frei|privatperson|bezahlt der abgeber""", flags=re.I)

    NO_DURATION_REGEX = re.compile(r"""keine|unbefristet""", flags=re.I)

    DURATION_SUFFIX_REGEX = re.compile(r""" Jahr\(e\)$| Jahre$| Jahr$""")

    ALTBAU_REGEX = re.compile(r"""altbau|neubau""", flags=re.I)

    ITEM_IMG_REGEX = r'"referenceImageUrl":"(https:\/\/cache.willhaben.at[-a-zA-Z0-9@:%._\+~#=/]+)"'
    BASE_URL = "https://www.willhaben.at"
    name = 'willhaben'
    allowed_domains = ['willhaben.at']
    start_urls = [
        START_URL_FLATS, START_URL_HOUSES
    ]

    def __init__(self):
        super()
        self.stats = {"flats": {"seen": 0, "crawled": 0, "new": 0, "price_changed": 0},
                      "houses": {"seen": 0, "crawled": 0, "new": 0, "price_changed": 0}}

    def get_type(self, response):
        url = response.url

        if url.startswith(self.START_URL_FLATS):
            return "flat", self.ITEM_URL_REGEX_FLATS
        if url.startswith(self.START_URL_HOUSES):
            return "house", self.ITEM_URL_REGEX_HOUSES
        raise ValueError("Could not determine item from for url %s" % url)

    def load_known_items(self, known_items):
        self.known_items = known_items
        logging.info("Got %d known items" % len(known_items))

    def parse(self, response):
        """
        This is the default callback used by Scrapy to process downloaded
        responses, when their requests don’t specify a callback like the
        `start_urls`
        """

        # get the next page of the list
        soup = BeautifulSoup(response.text, 'lxml')

        item_type, item_url_regex = self.get_type(response)
        stats_key = item_type + "s"

        # get item urls and yield a request for each item
        relative_item_urls = re.findall(item_url_regex, response.text)
        item_count = len(relative_item_urls)
        if item_count == 25:
            logging.info("Found {} items on page {}".format(
                item_count, response.url))
        elif item_count >= 20:
            logging.warning("Found only {} items on page {}".format(
                item_count, response.url))
        else:
            logging.error("Found only {} items on page {}".format(
                item_count, response.url))

        data_script_tag = soup.find(id="__NEXT_DATA__")
        if data_script_tag:
            logging.info("Found NEXT_DATA script tag")
            data_script_data = data_script_tag.string
            full_data = json.loads(data_script_data)
            ad_data_string = full_data["props"]["pageProps"]["searchResult"]["taggingData"]["tmsDataValues"]["tmsData"]["search_results"]
            ad_data = json.loads(ad_data_string)

            price_data = { ad["adId"]:ad.get("price") for ad in ad_data if ad.get("price") }
        else:
            logging.error("Did not find NEXT_DATA script tag!")
            price_data = {}

        for relative_item_url in relative_item_urls:

            self.stats[stats_key]["seen"] += 1

            # this should always match
            willhaben_code = self.WILLHABEN_CODE_REGEX.match(relative_item_url.split("-")[-1])
            if willhaben_code:
                willhaben_code = willhaben_code[0]
                # check for matching item in the database
                known_price = self.known_items.get(willhaben_code, None)
                if known_price:
                    # logging.info("item with code '%s' is known, price: %dEUR" % (willhaben_code, known_price))
                    current_price_int = int(float(price_data.get(willhaben_code, -1)))
                    if known_price == current_price_int:
                        logging.info("price for item %s (%d EUR) did not change, skipping" % (willhaben_code, current_price_int))
                        continue
                    else:
                        logging.info("price for item %s (%d → %d EUR) changed!" % (willhaben_code, known_price, current_price_int))
                        self.stats[stats_key]["price_changed"] += 1
                else:
                    self.stats[stats_key]["new"] += 1
                    logging.info("item '%s' is unknown" % (willhaben_code))

            self.stats[stats_key]["crawled"] += 1
            logging.info("queueing %s" % (relative_item_url))
            full_item_url = self.BASE_URL + relative_item_url
            yield scrapy.Request(full_item_url, self.parse_item, meta={"item_type":item_type})

        pagination_btn = soup.find(
            'a', attrs={"data-testid": "pagination-top-next-button"})
        try:
            next_page_url = self.BASE_URL + pagination_btn['href']
        except:
            return
        yield scrapy.Request(next_page_url, self.parse)

    def parse_item(self, response):
        """returns/yields a :py:class:`WillhabenItem`.

        This is the callback used by Scrapy to parse downloaded item pages.
        """
        item = CondoItem()
        item.set_default_values()
        item['url'] = response.url
        item['discovery_date'] = datetime.datetime.now().strftime("%Y-%m-%d")

        item['type'] = response.meta["item_type"]

        # time could also be added if needed: "%Y-%m-%d %H:%M:%S"

        soup = BeautifulSoup(response.text, 'lxml')
        # remove all script tags from soup
        for s in soup('script'):
            s.clear()

        # title
        title_tag = soup.find('h1')
        if title_tag:
            item['title'] = title_tag.get_text()
        else:
            logging.error("title element not found on page " + item['url'])

        body_tag = soup.find('article')

        # price
        price_tag = soup.find(
            'span', attrs={"data-testid": "contact-box-price-box-price-value-0"})
        if price_tag:
            visible_price_text = price_tag.get_text()
            item.parse_price(visible_price_text)
        else:
            logging.error("price element not found on page " + item['url'])

        # size
        size_tag = soup.find(
            'div', attrs={"data-testid": "ad-detail-teaser-attribute-0"})
        if size_tag:
            visible_size_text = size_tag.get_text()
            item.parse_size(visible_size_text)
        else:
            logging.error("size element not found on page " + item['url'])

        # room_count
        room_count_tag = soup.find(
            'div', attrs={"data-testid": "ad-detail-teaser-attribute-1"})
        if room_count_tag:
            room_count_text = room_count_tag.get_text()
            item.parse_room_count(room_count_text)
        else:
            logging.error(
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
            logging.error(
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
            logging.warning(
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
            logging.warning(
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
                logging.error(
                    "postal_code parsing failed on page " + item['url'])
            # parse district
            match = re.search(r'8\d\d\d ([^,]+)', location_address_text)
            if match:
                item['district'] = match[1]  # The first group
            else:
                logging.error(
                    "district parsing failed on page " + item['url'])
        else:
            logging.error("element for address, postal_code and district " +
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
                logging.error(
                    "willhaben_code parsing failed on page " + item['url'])
        else:
            logging.error(
                "willhaben_code element not found on page " + item['url'])

        # edit_date
        edit_date_tag = soup.find(
            'span', attrs={"data-testid": "ad-detail-ad-edit-date"})
        if edit_date_tag:
            item['edit_date'] = datetime.datetime.strptime(edit_date_tag.get_text(), self.DATE_FORMAT_STRING)
        else:
            logging.error("edit_date element not found on page " + item['url'])

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
            logging.error(
                "commission_fee element not found on page " + item['url'])

        # price_per_m2
        item.calc_price_per_m2()

        # futher item processing is done in the item pipeline
        yield item
