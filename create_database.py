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


