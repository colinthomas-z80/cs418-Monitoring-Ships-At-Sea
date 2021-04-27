#!/usr/bin/python3

# CS418, Spring 2019
# Nicolas Renet

# Exercises in SQL (covering Week #8)

import mysql.connector
from mysql.connector import errorcode

import configparser, sys, re


class MySQLConnectionManager:


	def __init__(self, cfg):
		self.config_file = cfg

	def __enter__(self):
		config = configparser.ConfigParser()
		success = config.read(self.config_file)
		if not success:
			raise configparser.Error("Could not read file {}".format( self.config_file))
		self.cnx = mysql.connector.connect( 
			user=config['SQL']['user'],
			password=config['SQL']['password'], 
			database=config['SQL']['database'])

		return self.cnx

	def __exit__(self, *ignore):
		#print("Closing connexion")
		self.cnx.commit()
		self.cnx.close()

class MySQLCursorManager:
	
	def __init__(self, cnx, options={}): 
		"""
		Initialize a new cursor, with the provided, named parameters.

		:param options: a dictionary of pairs (key, value) that can be passed to the cursor initializer functions, to be unpacked as named parameters. E.g. if ``options`` is the dictionary ``{'named_tuple: True'``, the cursor will be initiazed with the named parameter ``named_tuple=True``.
		:type options: dict
		"""
		if len(options.items())>0:
			print("Initializing cursor with following options:")
		for key, value in options.items():
			print("\t{}={}".format( key, value ))
				
		self.cursor=cnx.cursor( **options )
	
	def __enter__(self):
		return self.cursor

	def __exit__(self, *ignore):
		#print("Closing cursor")
		self.cursor.close()


class SQL_runner():

	config_file = 'connection_data.conf'
		
	def run(self,  query):
		rs = []

		try:
			with MySQLConnectionManager(self.config_file) as con:
				with MySQLCursorManager( con ) as cursor:
					statements = []
					if ';' in query:
						for maybe in query.split(";"): 
							if not re.match(r'^\s*$', maybe ):
								statements.append( maybe )
					else:
						statements.append(query)

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
		finally: 
			return rs
