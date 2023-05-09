# import default packages
import logging
import time
# import installed packages
import environs
import mysql.connector
from mysql.connector import errorcode
# import project modules
from webscraper_for_sophie.items import CondoItem


env = environs.Env()
USER = env("MYSQL_USER")
PASSWORD = env("MYSQL_PASSWORD")
DATABASE = env("MYSQL_DATABASE")
TABLENAME = env("MYSQL_TABLENAME")
HOST = 'db'     # name of the docker container

# Settings for connection error handling
NUM_ATTEMPTS = 30
DELAY_BTW_ATTEMPTS = 1     # in seconds
RETRY_MSG = ("Waiting for MySQL container to start gracefully " +
             "(Attempt {} of {}) failed")


class DatabaseManager():
    """
    Simplies our database operations
    """

    def connect(self):
        """ Connect to the database """
        for attempt_no in range(1, NUM_ATTEMPTS+1):
            try:
                self.connection = mysql.connector.connect(host=HOST,
                                                          database=DATABASE,
                                                          user=USER,
                                                          password=PASSWORD)
                self.cursor = self.connection.cursor(dictionary=True)
                logging.debug("Database connection opened")
                return
            except mysql.connector.Error as err:
                logging.debug(RETRY_MSG.format(attempt_no, NUM_ATTEMPTS))
                if attempt_no < NUM_ATTEMPTS:
                    time.sleep(DELAY_BTW_ATTEMPTS)
                else:
                    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                        logging.error(
                            "Something is wrong with your user name or password")
                    elif err.errno == errorcode.ER_BAD_DB_ERROR:
                        logging.error("Database does not exist")
                    else:
                        logging.error(err)

    def close(self):
        """ Close the database connection """
        self.connection.close()
        logging.debug("Database connection closed")

    def is_connected(self):
        """
        Returns:
            bool: True if connected. False otherwise
        """
        self.connection.is_connected()

    def prep_table(self):

        """ create a new table if the provided table name does not exist. """
        sql_command = "SHOW TABLES LIKE '{0}'".format(TABLENAME)
        self.cursor.execute(sql_command)
        result = self.cursor.fetchone()  # fetch will return a python tuple

        drop = False

        if result and drop:
            # """ drop the table (testing) """
            logging.debug("Dropping table")
            sql_command = "DROP TABLE {0};".format(TABLENAME)
            self.cursor.execute(sql_command)
            self.connection.commit()

        if drop or not result:
            logging.debug("Database table does not exist")

            # create table
            sql_command = """
            CREATE TABLE {0} (
            id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
            willhaben_code VARCHAR(10) COLLATE utf8_bin,
            postal_code VARCHAR(10) COLLATE utf8_bin,
            district VARCHAR(100) COLLATE utf8_bin,
            type VARCHAR(10) COLLATE utf8_bin,
            current_price INTEGER,
            min_price INTEGER,
            max_price INTEGER,
            previous_price INTEGER,
            min_price_date DATETIME,
            max_price_date DATETIME,
            previous_price_date DATETIME,
            energy_info TEXT COLLATE utf8_bin,
            heating_consumption FLOAT,
            seller_is_private BIT,
            features_info TEXT COLLATE utf8_bin,
            commission_fee FLOAT,
            construction_type VARCHAR(100) COLLATE utf8_bin,
            contract_duration VARCHAR(100) COLLATE utf8_bin,
            size INTEGER,
            room_count INTEGER,
            price_per_m2 FLOAT,
            discovery_date DATE,
            title TEXT COLLATE utf8_bin,
            url TEXT COLLATE utf8_bin,
            edit_date DATETIME,
            address VARCHAR(100) COLLATE utf8_bin);""".format(TABLENAME)
            self.cursor.execute(sql_command)
            self.connection.commit()
            logging.debug("New database table has been created")

    def get_known_items(self):
        sql_command = """SELECT willhaben_code, current_price FROM {0};""".format(TABLENAME)
        self.cursor.execute(sql_command)
        result = self.cursor.fetchall()
        return { i["willhaben_code"]:i["current_price"] for i in result }

    def store_item(self, item):
        """
        Store a new item in the database

        Args:
            item: the CondoItem that should be inserted in the database.
        """

        # check for existing item (with identical willhaben code)
        sql_command = """SELECT id, current_price, min_price, max_price, previous_price, min_price_date, max_price_date, previous_price_date, edit_date FROM {0} where willhaben_code = %s LIMIT 1""".format(TABLENAME)
        sql_command_args = (item['willhaben_code'], )

        self.cursor.execute(sql_command, sql_command_args)
        result = self.cursor.fetchone()

        if result is not None:
            # update price info
            if item['current_price'] != result['max_price']:
                result['previous_price'] = result['current_price']
                result['previous_price_date'] = result['edit_date']
                result['current_price'] = item['current_price']

                if item['current_price'] > result['max_price']:
                    result['max_price'] = item['current_price']
                    result['max_price_date'] = item['edit_date']
                if item['current_price'] < result['min_price']:
                    result['min_price'] = item['current_price']
                    result['min_price_date'] = item['edit_date']
            else:
                return

            sql_command = """UPDATE {0} SET current_price=%s, price_per_m2=%s, min_price=%s, max_price=%s, previous_price=%s, min_price_date=%s, max_price_date=%s, previous_price_date=%s WHERE id = %s;""".format(TABLENAME)

            update_tuple = (result['current_price'], item['price_per_m2'],
                                result['min_price'], result['max_price'], result['previous_price'],
                                result['min_price_date'], result['max_price_date'], result['previous_price_date'],
                                result['id'])
            try:
                self.cursor.execute(sql_command, update_tuple)
            except:
                logging.error(self.cursor.statement)
                raise
        

        else:
            # fill table of database with data
            sql_command = """INSERT INTO {0}
                                (id, willhaben_code, postal_code, district, type, current_price,
                                min_price, max_price, previous_price,
                                min_price_date, max_price_date, previous_price_date,
                                energy_info, features_info,
                                heating_consumption, seller_is_private, contract_duration,
                                construction_type,
                                commission_fee, size, room_count, price_per_m2,
                                discovery_date, title, url, edit_date, address)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """.format(TABLENAME)

            insert_tuple = (None, item['willhaben_code'], item['postal_code'],
                            item['district'], item['type'], item['current_price'],
                            item['current_price'], item['current_price'], item['current_price'],
                            item['edit_date'], item['edit_date'], item['edit_date'],
                            item['energy_info'], item['features_info'],
                            item['heating_consumption'], item['seller_is_private'], item['contract_duration'],
                            item['construction_type'], item['commission_fee'],
                            item['size'], item['room_count'], item['price_per_m2'],
                            item['discovery_date'], item['title'], item['url'],
                            item['edit_date'], item['address'])
            # use parameterized input to avoid SQL injection
            self.cursor.execute(sql_command, insert_tuple)
            # never forget this, if you want the changes to be saved:
        self.connection.commit()
