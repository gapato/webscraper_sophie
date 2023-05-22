import requests
import bs4
import sys
import logging
import json
import datetime
import arrow

logger = logging.getLogger("willhaben")

URL_BASE = "https://www.willhaben.at/iad/immobilien/d/mietwohnungen/steiermark/graz/-%s"

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

def main():

    for code in sys.argv[1:]:

        r = get_page(code)
        if r is not None:
            parse_page(r)
        else:
            logger.error("Failed to retrieve page for ad %s", code)

def get_page(code):

    r = requests.get(URL_BASE % code)
    if r.ok:
        return r.text
    else:
        return None

def get_attribute_value(attr):
    if len(attr["values"]) == 1:
        return attr["values"][0]
    else:
        return attr["values"]

def parse_page(text):

    soup = bs4.BeautifulSoup(text, 'lxml')

    data_script_tag = soup.find(id="__NEXT_DATA__")
    if data_script_tag:
        logger.debug("Found NEXT_DATA script tag")

        try:
            full_data = json.loads(data_script_tag.string)
        except:
            self.logger.error("Failed to parse NEXT_DATA json data")
            raise

        page_props = full_data["props"]["pageProps"]

        if page_props.get("advertDetails") is None:
            logger.error("This ad does not have 'advertDetails' property")
            return
        else:
            data = page_props["advertDetails"]

        first_published_date = datetime.datetime.strptime(data["firstPublishedDate"], DATE_FORMAT)
        a = arrow.get(first_published_date)

        attributes = { attr["name"]:get_attribute_value(attr) for attr in data["attributes"]["attribute"] }

        def get_a(key):
            return attributes.get(key, "")
            
        print(data["description"])
        print()
        print("First published: %s (%s)" % (data["firstPublishedDate"], a.humanize()))
        print("        Changed: %s" % data["changedDate"])
        print()
        print("%s / %sm² / %s rooms / %s" % (get_a("PRICE_FOR_DISPLAY"), get_a("ESTATE_SIZE/LIVING_AREA"), get_a("NO_OF_ROOMS"), get_a("BUILDING_TYPE")))
        print()
        print("Available: %s" % get_a("AVAILABLE_DATE"))
        print()
        print("Features: ", end="")
        for p in get_a("ESTATE_PREFERENCE"):
            print("%s, " % p, end="")
        print()


        

main()
