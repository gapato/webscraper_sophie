# import default packages
import logging
import time
import datetime
# import installed packages
import environs
# import project modules
from webscraper_for_sophie.items import CondoItem

env = environs.Env()

DB_DRIVER = env("DB_DRIVER")

if DB_DRIVER == "mysql":
    import mysql.connector
    from mysql.connector import errorcode

    MYSQL_USER = env("MYSQL_USER")
    MYSQL_PASSWORD = env("MYSQL_PASSWORD")
    MYSQL_DATABASE = env("MYSQL_DATABASE")
    MYSQL_HOST = 'db'     # name of the docker container

    # Settings for connection error handling
    NUM_ATTEMPTS = 30
    DELAY_BTW_ATTEMPTS = 1     # in seconds
    RETRY_MSG = ("Waiting for MySQL container to start gracefully " +
                 "(Attempt {} of {}) failed")

    COLLATION = " COLLATE utf8_bin"
    SQL_PLACEHOLDER = "%s"
    SQL_AUTOINCREMENT_STR = "AUTO_INCREMENT"
elif DB_DRIVER == "sqlite":
    import sqlite3
    SQLITE_DB_FILE = env("SQLITE_DB_FILE")
    COLLATION = ""
    SQL_PLACEHOLDER = "?"
    SQL_AUTOINCREMENT_STR = ""
else:
    raise ValueError("DB_DRIVER must be one of 'mysql' or 'sqlite'")

TABLENAME = env("TABLENAME")

# SQL COMMANDS
SQL_CMD_INIT_META = """INSERT INTO META (txtkey, value) VALUES ({0}, {0});""".format(SQL_PLACEHOLDER)
SQL_CMD_UPDATE_ITEM = """UPDATE {0} SET current_price={1}, price_per_m2={1}, edit_date={1}, min_price={1}, max_price={1}, previous_price={1}, min_price_date={1}, max_price_date={1}, previous_price_date={1} WHERE id = {1};""".format(TABLENAME, SQL_PLACEHOLDER)
SQL_CMD_UPDATE_ITEM_EXPIRED = """UPDATE {0} SET expiry_date={1}, expiry_last_check_timestamp={1} WHERE id = {1};""".format(TABLENAME, SQL_PLACEHOLDER)
SQL_CMD_SELECT_ITEM = """SELECT id, current_price, min_price, max_price, previous_price, min_price_date, max_price_date, previous_price_date, edit_date FROM {0} WHERE willhaben_code = {1} LIMIT 1""".format(TABLENAME, SQL_PLACEHOLDER)
SQL_CMD_SELECT_ITEM_ID = """SELECT id, current_price, min_price, max_price, previous_price, min_price_date, max_price_date, previous_price_date, edit_date FROM {0} WHERE id = {1} LIMIT 1""".format(TABLENAME, SQL_PLACEHOLDER)
SQL_CMD_INSERT_ITEM = """INSERT INTO {0}
                    (id, willhaben_code, postal_code, district, type, current_price,
                    min_price, max_price, previous_price,
                    min_price_date, max_price_date, previous_price_date,
                    energy_info, features_info,
                    heating_consumption, seller_is_private, contract_duration,
                    construction_type, rent_sale,
                    commission_fee, size, room_count, price_per_m2,
                    discovery_date, discovery_timestamp, expiry_last_check_timestamp, title, url, edit_date, address,
                    has_json_details)
                VALUES ({1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1},
                {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1});
                """.format(TABLENAME, SQL_PLACEHOLDER)
SQL_CMD_UPDATE_TIMESTAMP = """UPDATE META SET value={0} WHERE txtkey={0};""".format(SQL_PLACEHOLDER)
SQL_CMD_DUE_EXPIRY = """SELECT id, willhaben_code, url FROM {0} WHERE expiry_last_check_timestamp < {1} AND expiry_date IS NULL;""".format(TABLENAME, SQL_PLACEHOLDER)

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

class DatabaseManager():
    """
    Simplies our database operations
    """

    def connect(self):
        """ Connect to the database """
        if DB_DRIVER == "mysql":
            logging.debug("Connecting to MySQL database")
            for attempt_no in range(1, NUM_ATTEMPTS+1):
                try:
                    self.connection = mysql.connector.connect(host=MYSQL_HOST,
                                                              database=MYSQL_DATABASE,
                                                              user=MYSQL_USER,
                                                              password=MYSQL_PASSWORD)
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
        elif DB_DRIVER == "sqlite":
            logging.debug("Opening sqlite database")
            self.connection = sqlite3.connect(SQLITE_DB_FILE)
            self.connection.row_factory = sqlite3.Row
            self.cursor = self.connection.cursor()

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

    def has_table(self, tablename):

        if DB_DRIVER == "mysql":
            sql_command = "SHOW TABLES LIKE '{0}'".format(tablename)
        elif DB_DRIVER == "sqlite":
            sql_command = "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE '{0}'".format(tablename)
        self.cursor.execute(sql_command)
        result = self.cursor.fetchone()  # fetch will return a python tuple

        return result is not None

    def prep_table(self):

        """ create a new table if the provided table name does not exist. """
        has_main_table = self.has_table(TABLENAME)
        has_meta_table = self.has_table('META')

        drop = False

        if drop and has_main_table:
            # """ drop the table (testing) """
            logging.debug("Dropping table")
            sql_command = "DROP TABLE {0};".format(TABLENAME)
            self.cursor.execute(sql_command)

            sql_command = "DROP TABLE META;"
            self.cursor.execute(sql_command)
            self.connection.commit()

        if drop or not has_meta_table:
            logging.debug("Database META table does not exist")
            sql_command = """
            CREATE TABLE META (
            txtkey VARCHAR(100) NOT NULL PRIMARY KEY{0},
            value VARCHAR(100){0}
            );""".format(COLLATION)
            self.cursor.execute(sql_command)
            self.connection.commit()

            sql_command = SQL_CMD_INIT_META
            insert_tuple = ('last_timestamp', datetime.datetime(1970, 1, 1))

            self.cursor.execute(sql_command, insert_tuple)
            self.connection.commit()

            logging.debug("New META database table has been created")

        if drop or not has_main_table:
            logging.debug("Database table does not exist")

            # create table
            sql_command = """
            CREATE TABLE {0} (
            id INTEGER NOT NULL {2} PRIMARY KEY,
            willhaben_code VARCHAR(10){1},
            postal_code VARCHAR(10){1},
            district VARCHAR(100){1},
            type VARCHAR(10){1},
            current_price INTEGER,
            min_price INTEGER,
            max_price INTEGER,
            previous_price INTEGER,
            min_price_date DATETIME,
            max_price_date DATETIME,
            previous_price_date DATETIME,
            energy_info TEXT{1},
            heating_consumption FLOAT,
            seller_is_private BIT,
            features_info TEXT{1},
            commission_fee FLOAT,
            construction_type VARCHAR(100){1},
            contract_duration VARCHAR(100){1},
            rent_sale VARCHAR(10){1},
            size INTEGER,
            room_count INTEGER,
            price_per_m2 FLOAT,
            discovery_date DATE,
            discovery_timestamp INTEGER,
            title TEXT{1},
            url TEXT{1},
            edit_date DATETIME,
            expiry_date DATETIME,
            expiry_last_check_timestamp INTEGER,
            address VARCHAR(100){1},
            has_json_details BIT);""".format(TABLENAME, COLLATION, SQL_AUTOINCREMENT_STR)
            self.cursor.execute(sql_command)
            self.connection.commit()
            logging.debug("New database table has been created")

    def get_due_for_expiration(self):
        threshold_ts = int((datetime.datetime.now() - datetime.timedelta(days=31)).timestamp())

        sql_command = SQL_CMD_DUE_EXPIRY
        sql_tuple = (threshold_ts, )

        self.cursor.execute(sql_command, sql_tuple)

        return self.cursor.fetchall()

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

        if item.get('expiry_date') is not None:
            now_ts = int(datetime.datetime.now().timestamp())
            sql_command = SQL_CMD_UPDATE_ITEM_EXPIRED
            update_tuple = (item['expiry_date'], now_ts, item['sql_id'])
            self.cursor.execute(sql_command, update_tuple)
            self.connection.commit()
            return

        # check for existing item (with identical willhaben code)
        sql_command = SQL_CMD_SELECT_ITEM
        sql_command_args = (item['willhaben_code'], )

        self.cursor.execute(sql_command, sql_command_args)
        result = self.cursor.fetchone()

        keys = ['current_price', 'min_price', 'max_price',
                'min_price_date', 'max_price_date',
                'id']

        if result is not None:

            # update price info
            if item['current_price'] == result['current_price'] and item['expiry_date'] is None:
                return

            merged_item = { k:result[k] for k in keys }

            merged_item['previous_price'] = result['current_price']
            merged_item['previous_price_date'] = result['edit_date']
            merged_item['current_price'] = item['current_price']

            merged_item['edit_date'] = item['edit_date']
            merged_item['price_per_m2'] = item['price_per_m2']

            if item['current_price'] > result['max_price']:
                merged_item['max_price'] = item['current_price']
                merged_item['max_price_date'] = item['edit_date']
            if item['current_price'] < result['min_price']:
                merged_item['min_price'] = item['current_price']
                merged_item['min_price_date'] = item['edit_date']

            sql_command = SQL_CMD_UPDATE_ITEM

            update_tuple = (merged_item['current_price'], merged_item['price_per_m2'], merged_item['edit_date'],
                                merged_item['min_price'], merged_item['max_price'], merged_item['previous_price'],
                                merged_item['min_price_date'], merged_item['max_price_date'], merged_item['previous_price_date'],
                                merged_item['id'])

            self.cursor.execute(sql_command, update_tuple)
        

        else:
            # fill table of database with data
            sql_command = SQL_CMD_INSERT_ITEM

            insert_tuple = (None, item['willhaben_code'], item['postal_code'],
                            item['district'], item['type'], item['current_price'],
                            item['current_price'], item['current_price'], item['current_price'],
                            item['edit_date'], item['edit_date'], item['edit_date'],
                            item['energy_info'], item['features_info'],
                            item['heating_consumption'], item['seller_is_private'], item['contract_duration'],
                            item['construction_type'], item['rent_sale'], item['commission_fee'],
                            item['size'], item['room_count'], item['price_per_m2'],
                            item['discovery_date'], item['discovery_timestamp'], item['discovery_timestamp'], item['title'], item['url'],
                            item['edit_date'], item['address'], item['has_json_details'])
            # use parameterized input to avoid SQL injection
            self.cursor.execute(sql_command, insert_tuple)
            # never forget this, if you want the changes to be saved:
        self.connection.commit()

    def store_timestamp(self):
        now = datetime.datetime.now().strftime(DATETIME_FORMAT)

        sql_command = SQL_CMD_UPDATE_TIMESTAMP
        update_tuple = (now, 'last_timestamp')

        self.cursor.execute(sql_command, update_tuple)
        self.connection.commit()

        logging.info("Stored timestamp %s" % now)

    def load_timestamp(self):
        sql_command = "SELECT value FROM META WHERE txtkey = 'last_timestamp';"
        self.cursor.execute(sql_command)

        result = self.cursor.fetchone()
        if result:
            return datetime.datetime.strptime(result['value'], DATETIME_FORMAT)
        else:
            return datetime.datetime(1970, 1, 1)

