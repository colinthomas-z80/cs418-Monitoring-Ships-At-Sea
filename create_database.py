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

print("INSERTING VALUES....")

vessels = open("scripts/VESSEL_VALUES.sql").read()
map_views = open("scripts/MAP_VIEW_VALUES2.sql").read()
ports = open("scripts/PORT_VALUES.sql").read()

print("VESSEL....")
try:
    err = SQL_runner().run("INSERT INTO AISDraft.VESSEL VALUES " + vessels)
except:
    print("Error inserting vessel values")

print("MAP VIEW....")
try:
    err = SQL_runner().run("INSERT INTO AISDraft.MAP_VIEW VALUES " + map_views)
except:
    print("Error inserting map view values")

print("PORT....")
try:
    err = SQL_runner().run("INSERT INTO AISDraft.PORT VALUES " + ports)
except:
    print("Error inserting port values")