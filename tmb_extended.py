import tmb_dao
from mysql.connector import errorcode
from mysqlutils import SQL_runner

print(SQL_runner().run("SHOW DATABASES;"))