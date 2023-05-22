import sqlite3

import datetime
from datetime import datetime as dt

con = sqlite3.connect("housing_graz.sqlite")
con.row_factory = sqlite3.Row
cursor = con.cursor()

statement = "select value from META where txtkey = 'last_timestamp';"
row = cursor.execute(statement).fetchone()

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

ts = dt.strptime(row['value'], DATE_FORMAT)

new_ts = (ts - datetime.timedelta(seconds=3*3600)).strftime(DATE_FORMAT)

print("Old timestamp: %s" % ts)
print("New timestamp: %s" % new_ts)

s = "update META set value = ? where txtkey = 'last_timestamp';"

cursor.execute(s, (new_ts,))
con.commit()

con.close()
