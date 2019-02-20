"""Contains Database connection and CRUD operations."""

import os
import time
import neo4j
import MySQLdb
from parse_config import ParseConfig
from logger import Logger
from transform import Transform
from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime, timedelta


__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"
__updatedDate__ = "2018-07-19"

slash = os.sep
log_file = 'database.py'

class Database(Logger):
	"""Database connectivity."""

	def __init__(self):
		Logger.__init__(self)
		"""Initialize database connection details."""

		base_path = slash.join(os.path.abspath('').split(slash)[:-1])
		conn_file_path = '{base}{sep}{folder}{sep}{file}'.format(
			base=base_path,sep=slash,folder='resources',file='connection.cfg'
			)
		
		connection_dict = ParseConfig.get_config_dict(conn_file_path)
		conn_type = connection_dict.get("devLocalConnection") #localConnection / devConnection / UATConnection
		self.host = conn_type.get("host")
		self.user = conn_type.get("user")
		self.password = conn_type.get("pass")
		self.database = conn_type.get("database")
		self.neo4j_user = conn_type.get('neo4j_user')
		self.neo4j_password = conn_type.get('neo4j_pass')
		self.neo4j_url = conn_type.get('neo4j_url')
		self.connect_neo4j()

	def connect(self):
		"""."""
		
		try:
			connection = MySQLdb.connect(
				host=self.host, user = self.user, passwd = self.password, db = self.database, charset='utf8'
			)
			cursor = connection.cursor()
		
		except Exception as e:
			self.logg(debug_msg = 'The progam cannot continue without database connection.', 
				info_msg = 'Function = connect()', 
				warning_msg = 'Mysql connection failed.', 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))
		else:
			self.connection = connection
			self.cursor = cursor

	def disconnect(self):
		"""."""

		try:
			self.connection.close()
		
		except Exception as e:
			self.logg(debug_msg = 'The progam cannot continue without database connection.', 
				info_msg = 'Function = disconnect()', 
				warning_msg = 'Mysql connection failed.', 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

	def get_connection(self):
		"""Creates database connection."""

		try:
			connection = MySQLdb.connect(
				host=self.host, user = self.user, passwd = self.password, db = self.database, charset='utf8'
				)
			cursor = connection.cursor()
		except Exception as e:
			self.logg(debug_msg = 'The progam cannot continue without database connection.', 
				info_msg = 'Function = get_connection()', 
				warning_msg = 'Mysql connection failed.', 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))
		else:
			return connection,cursor

	def connect_neo4j(self):
		"""."""

		while True:
			try:
				driver = GraphDatabase.driver(self.neo4j_url, auth=basic_auth(self.neo4j_user, self.neo4j_password))

			except neo4j.exceptions.ServiceUnavailable as e:
				time.sleep(120)
				continue

			except Exception as e:
				self.logg(debug_msg = 'The progam cannot continue without database connection.', 
					info_msg = 'Function = connect_neo4j()', 
					warning_msg = 'Neo4j connection failed.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))
				time.sleep(30)

			else:
				 self.driver = driver
				 return


	def get_neo4j_connection(self):
		"""."""

		while True:
			try:
				driver = GraphDatabase.driver(self.neo4j_url, auth=basic_auth(self.neo4j_user, self.neo4j_password))

			except neo4j.exceptions.ServiceUnavailable as e:
				time.sleep(120)
				continue

			except Exception as e:
				self.logg(debug_msg = 'The progam cannot continue without database connection.', 
					info_msg = 'Function = get_neo4j_connection()', 
					warning_msg = 'Neo4j connection failed.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))
				time.sleep(30)

			else:
				return driver

	@staticmethod
	def get_query_dictonary(results, cursor):
		"""Formats data into key,value pair(dictionary)."""

		total_result = []
		columns = [column[0] for column in cursor.description]
		for row in results:
			total_result.append(dict(zip(columns, row)))
		return total_result

	def get_neo4j_queries(self, neo4j_label, query_type, neo4j_entity):
		"""."""

		connection, cursor = self.get_connection()

		select_query = 'SELECT neo4j_query FROM neo4j_queries '\
		'WHERE neo4j_label = "{}" AND query_type  = "{}" AND neo4j_entity = "{}"'\
		.format(neo4j_label, query_type, neo4j_entity)

		try:
			cursor.execute(select_query)
			query_result = cursor.fetchall()

		except Exception as e:
			self.logg(debug_msg = 'Unable to fetch neo4j Query.', 
				info_msg = 'Function = get_neo4j_queries()', 
				warning_msg = 'Unable to query columns of {}.'.format('neo4j_queries'), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			if query_result:
				neo4j_query = Transform.to_str(query_result[0][0])
				return neo4j_query

		finally:
			connection.close()


	def get_neo4j_properties(self, status, neo4j_label, neo4j_entity):
		"""."""

		select_query = 'SELECT property, null FROM neo4j_properties '\
		'WHERE active_status = {flag} AND neo4j_label = "{label}" AND neo4j_entity = "{entity}"'\
		.format(flag=status, label=neo4j_label, entity=neo4j_entity)

		connection, cursor = self.get_connection()

		try:
			cursor.execute(select_query)
			query_result = cursor.fetchall()

		except Exception as e:
			self.logg(debug_msg = 'Unable to fetch table columns.', 
				info_msg = 'Function = get_mysql_queries()', 
				warning_msg = 'Unable to query columns of {}.'.format("mysql_data_queries"), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			if query_result:
				properties = dict(query_result)
				return properties

		finally:
			connection.close()

	def get_neo4j_mapping(self, label):
		"""."""

		select_query = 'SELECT mysql_properties, neo4j_properties FROM mysql_neo4j_mapping WHERE neo4j_label = "{}"'\
		.format(label)

		connection, cursor = self.get_connection()

		try:
			cursor.execute(select_query)
			query_result = cursor.fetchall()

		except Exception as e:
			self.logg(debug_msg = 'Unable to fetch table columns.', 
				info_msg = 'Function = get_mysql_queries()', 
				warning_msg = 'Unable to query columns of {}.'.format("mysql_data_queries"), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			if query_result:
				mysql_query = dict(query_result)
				return mysql_query

		finally:
			connection.close()

	def get_mysql_queries(self, label, query_type, entity):
		"""."""

		connection, cursor = self.get_connection()

		select_query = 'SELECT mysql_query FROM mysql_queries '\
		'WHERE neo4j_label = "{}" and query_type = "{}" and neo4j_entity = "{}"'\
		.format(label, query_type, entity)

		try:
			cursor.execute(select_query)
			query_result = cursor.fetchall()

		except Exception as e:
			self.logg(debug_msg = 'Unable to fetch table columns.', 
				info_msg = 'Function = get_mysql_queries()', 
				warning_msg = 'Unable to query columns of {}.'.format("mysql_data_queries"), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			if query_result: 
				mysql_query = query_result[0][0]
				return mysql_query

		finally:
			connection.close()

	def fetch_all_mysql_data(self, data_type, query_type, neo4j_entity, match=None):
		"""."""
		
		query = self.get_mysql_queries(data_type, query_type, neo4j_entity)
		if match:
			select_query = query.format(match)
		else:
			select_query = query

		connection, cursor = self.get_connection()

		try:
			cursor.execute(select_query)
			query_result = cursor.fetchall()

		except Exception as e:
			self.logg(debug_msg = 'Unable to fetch table columns.', 
				info_msg = 'Function = fetch_mysql_data()', 
				warning_msg = 'Unable to query columns of {}.'.format(select_query), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			if query_result:
				data = self.get_query_dictonary(query_result, cursor)
				return data

		finally:
			connection.close()

	def get_organization_permalink(self, organization_id):
		"""."""

		connection, cursor = self.get_connection()

		select_query = 'SELECT Permalink, PrimaryRole FROM CB_OrganizationMaster WHERE OrganizationID = {}'\
		.format(organization_id)

		try:
			cursor.execute(select_query)
			query_result = cursor.fetchall()

		except Exception as e:
			self.logg(debug_msg = 'Unable to fetch table columns.', 
				info_msg = 'Function = get_organization_permalink()', 
				warning_msg = 'Unable to query columns of {}.'.format("CB_OrganizationMaster"), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))
			return None, None

		else:
			if query_result: 
				org_permalink = query_result[0][0]
				org_primary_role = query_result[0][1]
				return org_permalink, org_primary_role
			else:
				return None, None

		finally:
			connection.close()

	def get_organization_primary_role(self, permalink):
		"""."""

		connection, cursor = self.get_connection()

		select_query = 'SELECT Permalink, PrimaryRole FROM CB_OrganizationMaster WHERE Permalink = "{}"'\
		.format(permalink)

		try:
			cursor.execute(select_query)
			query_result = cursor.fetchall()

		except Exception as e:
			self.logg(debug_msg = 'Unable to fetch table columns.', 
				info_msg = 'Function = get_organization_primary_role()', 
				warning_msg = 'Unable to query columns of {}.'.format("CB_OrganizationMaster"), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))
			return None, None

		else:
			if query_result: 
				org_permalink = query_result[0][0]
				org_primary_role = query_result[0][1]
				return org_permalink, org_primary_role
			else:
				return None, None

		finally:
			connection.close()

	def execute_neo4j_merge_query(self, query):
		"""."""
		
		while True:
			with self.driver.session() as session:
				try:
					session.run(query)
					return True

				except neo4j.exceptions.ConstraintError as e:
					query = query.replace('Source:"CB"','Source:"CB, MS"')
					session.run(query)
					return True

				except neo4j.exceptions.ServiceUnavailable as e:
					self.connect_neo4j()

				except Exception as e:
					self.logg(debug_msg = 'Error while executing cypher query.',
						info_msg = 'Function = execute_neo4j_merge_query()',
						warning_msg = 'Error in QUERY: {}.'.format(query),
						error_msg = 'Module = '+log_file,
						critical_msg = str(Transform.to_str(e)))
					return True

	def execute_neo4j_match_query(self, query):
		"""."""

		while True:
			with self.driver.session() as session:
				try:
					result = session.run(query)

				except neo4j.exceptions.ServiceUnavailable as e:
					self.connect_neo4j()

				except Exception as e:
					self.logg(debug_msg = 'Error while executing cypher query.',
						info_msg = 'Function = execute_neo4j_match_query()',
						warning_msg = 'Error in QUERY: {}.'.format(query),
						error_msg = 'Module = '+log_file,
						critical_msg = str(Transform.to_str(e)))
					return None

				else:
					return result


if __name__ == "__main__":
	"""module testing."""

	database = Database()
	database.get_connection()



