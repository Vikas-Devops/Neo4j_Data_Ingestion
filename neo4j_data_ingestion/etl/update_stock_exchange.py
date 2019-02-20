"""Organization Module for following nodes in neo4j:
* Company
* Investor
* AcademicInstitute
* Group
"""

import neo4j
from database import Database
from transform import Transform
from collections import OrderedDict
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'update_stock_exchange.py'

class UpdateStockExchange(Database):
	"""Creates and updates Organization nodes in neo4j."""

	def __init__(self):
		"""Initialize object variables."""

		Database.__init__(self)

		self.label = 'UpdateStockExchange'
		self.active_status = 1
		self.entity_type = 'node'
		self.mysql_all_data_query = 'select_all'
		self.merge_query = 'merge'
		self.unique_keys = ['Permalink']
		self.to_upper_case = ['StockExchange']
		self.to_integer = []

		self.neo4j_mapping = self.get_neo4j_mapping("Company")
		self.neo4jQuery = self.get_neo4j_queries(self.label, self.merge_query, self.entity_type)
		self.neo4j_properties = self.get_neo4j_properties(
			self.active_status, self.label, self.entity_type
			)

	def create_query_data(self, data):
		"""Creates dynamic string for node properties and match condition."""
		
		try:
			properties = []
			unique_properties = []

			for key, value in data.iteritems():
				key = Transform.to_str(key)
				value = Transform.to_str(value)
		
				if key in self.unique_keys:
					item = '{}:"{}"'.format(key,value)
					unique_properties.append(item)

				elif key in self.to_upper_case:
					if value:
						item = '{}:"{}"'.format(key,value.upper())
						properties.append(item)

				elif value != 0 and not value:
					continue

				else:
					if isinstance(value, str) and '"' in value:
						value = value.replace('"','') 

					item = '{}:"{}"'.format(key,value)
					properties.append(item)

			property_string = ','.join(properties)
			unique_string = ','.join(unique_properties)

		except Exception as e:
			self.logg(debug_msg = 'Error while perparing properties.', 
				info_msg = 'Function = create_query_data()', 
				warning_msg = 'Data to properties string failed.', 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			return unique_string, property_string


	def create_data_dictionary(self, row):
		"""Converts mysql data into dictionary of neo4j properties."""
		
		properties = OrderedDict()
		
		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(self.neo4j_mapping, row)

		for key, value in self.neo4j_properties.iteritems():
			try:
				val = data.get(key)
				properties.setdefault(key,val)

			except Exception as e:
				self.logg(debug_msg = 'Error while perparing data dictionary.', 
					info_msg = 'Function = create_data_dictionary()', 
					warning_msg = 'Data transformation to key, value pair failed.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))
				continue

		return properties

	def update_organization_node(self, data):
		"""Process raw data from mysql and executes neo4j queries."""
		
		if data:
			for row in data:
				try:
					properties_data = self.create_data_dictionary(row)
					key, properties =  self.create_query_data(properties_data)
					if key and properties:
						query = self.neo4jQuery.format(key, properties)
						self.execute_neo4j_merge_query(query)
					else:
						pass

				except Exception as e:
					self.logg(debug_msg = 'Error while creating neo4j node query.',
						info_msg = 'Function = update_organization_node()',
						warning_msg = 'Error in creating dynamic node query.',
						error_msg = 'Module = '+log_file,
						critical_msg = str(e))
					continue

	def get_data(self, data_set=None, permalink=None):
		"""Fetch data from mysql w.r.t node."""

		if data_set == 'all':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_all_data_query, self.entity_type, match=None
				)
			return data

	@classmethod
	def run(cls):
		"""."""

		try:
			updateStockExchange = cls()
			data = updateStockExchange.get_data(data_set='all')
			if data:
				updateStockExchange.update_organization_node(data)
		
		except Exception as e:
			return False
		
		else:
			return True

if __name__ == '__main__':
	"""."""

	UpdateStockExchange.run()


