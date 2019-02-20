"""Fund Module for Fund nodes in neo4j"""

from database import Database
from transform import Transform
from collections import OrderedDict
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'fund.py'

class Fund(Database):
	"""Creates and updates Fund nodes in neo4j."""

	def __init__(self):
		"""Initialize object variables."""

		Database.__init__(self)

		self.label = 'Fund'
		self.active_status = 1
		self.entity_type = 'node'
		self.mysql_delta_query = 'select_delta'
		self.mysql_filter_query = 'select_filtered'
		self.mysql_all_data_query = 'select_all'
		self.date = (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d")
		self.merge_query = 'merge'
		self.delete_query = 'delete'
		self.unique_keys = ['FundsId']
		self.to_integer = [
							'MoneyRaisedUSD',
							'AnnouncedOnMonth',
							'AnnouncedOnYear'
							]

		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.neo4jQuery = self.get_neo4j_queries(self.label, self.merge_query, self.entity_type)
		self.neo4jDeleteQuery = self.get_neo4j_queries(self.label, self.delete_query, self.entity_type)
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
					continue

				if key in self.to_integer and (value or value == 0):
					value = Transform.to_integer(value)
					item = '{}:{}'.format(key,value)
					properties.append(item)
					continue
				
				if value != 0 and not value:
					continue

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
				if key == 'UpdatedDate':
					properties.update({'UpdatedDate':today_date})

				elif key == 'AnnouncedOnMonth':
					AnnouncedOn = data.get('AnnouncedOn')
					if AnnouncedOn:
						month = str(AnnouncedOn.month)
						properties.update({'AnnouncedOnMonth':month})

				elif key == 'AnnouncedOnYear':
					AnnouncedOn = data.get('AnnouncedOn')
					if AnnouncedOn:
						year = str(AnnouncedOn.year)
						properties.update({'AnnouncedOnYear':year})

				else:
					val = data.get(key)
					properties.update({key:val})

			except Exception as e:
				self.logg(debug_msg = 'Error while perparing data dictionary.', 
					info_msg = 'Function = create_data_dictionary()', 
					warning_msg = 'Data transformation to key, value pair failed.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))
				continue

		return properties

	def create_update_fund_node(self, data):
		"""Process raw data from mysql and executes neo4j queries."""
	
		if data:
			for row in data:
				try:
					properties_data = self.create_data_dictionary(row)
					key, properties =  self.create_query_data(properties_data)
					if key and properties:
						IsDeleted = row.get('IsDeleted')
						if IsDeleted:
							query = self.neo4jDeleteQuery.format(key)
						else:
							query = self.neo4jQuery.format(key, properties)
						
						self.execute_neo4j_merge_query(query)
					else:
						pass

				except Exception as e:
					self.logg(debug_msg = 'Error while creating neo4j node query.', 
						info_msg = 'Function = create_update_fund_node()',
						warning_msg = 'Error in creating dynamic node query.', 
						error_msg = 'Module = '+log_file,
						critical_msg = str(e))
					continue

	def check_create_fund_node(self, fund_id):
		"""Check if the fund exists in neo4j.
		   If node does not exists creates a new node.
		"""

		query = 'MATCH (f:Fund) where f.FundsId = "{}" RETURN f.FundsId'\
		.format(fund_id)

		result = self.execute_neo4j_match_query(query)

		if result:
			items = [item for item in result]
			try:
				Fund_ID = items[0]['f.FundsId']

			except IndexError as e:
				data = self.get_data(data_set='selected', fund_id=fund_id)
				if data:
					self.create_update_fund_node(data)
					return True

			except Exception as e:
				pass

	def get_data(self, data_set=None, fund_id=None):
		"""Fetch data from mysql w.r.t node."""

		if data_set == 'delta':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_delta_query, self.entity_type, match=self.date
				)
			return data

		elif data_set == 'selected':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_filter_query, self.entity_type, match=fund_id
				)
			return data

		elif data_set == 'select_all':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_all_data_query, self.entity_type, match=None
				)
			return data

	@classmethod
	def run(cls):
		"""."""

		try:
			fund = cls()
			data = fund.get_data(data_set='delta')
			if data:
				fund.create_update_fund_node(data)

		except Exception as e:
			return False

		else:
			return True

if __name__ == '__main__':
	"""."""

	Fund.run()





