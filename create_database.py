import mysql.connector
from mysql.connector import errorcode

import sys, re
from mysqlutils import SQL_runner

f = open('new.mysql')
script = f.read()
f.close()

queries = script.split(';')

try:
    for q in queries:
        err = SQL_runner().run(q)
except:
    print(err)
