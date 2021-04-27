import mysql.connector
from mysql.connector import errorcode

import time

import sys, re
from mysqlutils import SQL_runner

f = open('schema.sql')
script = f.read()
f.close()

try:
    err = SQL_runner().run(script)
except:
    print(err)
    print("Error Creating Schema")

