import mysql.connector
from mysql.connector import errorcode

import time

import sys, re
import mysqlutils
from mysqlutils import SQL_runner

f = open('scripts/schema.sql')
script = f.read()
f.close()

print("Creating Schema....")

try:
    err = SQL_runner().run(script)
except:
    print(err)
    print("Error Creating Schema")

print("OK")

config_file = 'connection_data.conf'

print("Populating Tables...")

try:
    with mysqlutils.MySQLConnectionManager(config_file) as con:
        with mysqlutils.MySQLCursorManager( con ) as cursor:
            statements = ["LOAD DATA INFILE '/var/lib/mysql-files/VESSEL.csv' INTO TABLE AISDraft.VESSEL COLUMNS TERMINATED BY ',' IGNORE 1 ROWS;",
            "LOAD DATA INFILE '/var/lib/mysql-files/MAP_VIEW.csv' INTO TABLE AISDraft.MAP_VIEW COLUMNS TERMINATED BY ',' IGNORE 1 ROWS;",
            "LOAD DATA INFILE '/var/lib/mysql-files/PORT.csv' INTO TABLE AISDraft.PORT COLUMNS TERMINATED BY ';' IGNORE 1 ROWS;",
            "LOAD DATA INFILE '/var/lib/mysql-files/AIS_MESSAGE_1000000_rows.csv' INTO TABLE AISDraft.AIS_MESSAGE COLUMNS TERMINATED BY ';' IGNORE 1 ROWS;",
            "LOAD DATA INFILE '/var/lib/mysql-files/STATIC_DATA_1000000_rows.csv' INTO TABLE AISDraft.STATIC_DATA COLUMNS TERMINATED BY ';' IGNORE 1 ROWS;",
            "LOAD DATA INFILE '/var/lib/mysql-files/POSITION_REPORT_1000000_rows.csv' INTO TABLE AISDraft.POSITION_REPORT COLUMNS TERMINATED BY ';' IGNORE 1 ROWS;"
            ]
            
            for sttmt in statements:
                cursor.execute( sttmt )
                if cursor.with_rows:
                    rs = cursor.fetchall()
                else:
                    rs = [(cursor.rowcount,)]
except mysql.connector.Error as err:
    if  err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
except Exception as e:
    print( e )

print("OK")