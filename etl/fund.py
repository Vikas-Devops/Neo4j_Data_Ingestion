

from database import Database
from transform import Transform
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'fund.py'

class Fund(Database):
	"""."""

	def __init__(self):
		"""."""

		Database.__init__(self)

		self.active_status = 1
		self.query_type = 'merge'
		self.entity_type = 'node'
		self.label = 'fund'
		self.date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
		self.unique_keys = ['FundsId']
		self.to_integer = ['MoneyRaisedUSD']


	def create_query_data(self, data):
		"""."""
		
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


	def create_data_dictionary(self, properties, neo4j_mapping, row):
		"""."""

		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(neo4j_mapping, row)

		for key, value in properties.iteritems():
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

	def initialize_variables(self):
		"""."""

		self.neo4jQuery = self.get_neo4j_queries('Fund', 'merge', 'node')
		self.neo4j_properties = self.get_neo4j_properties(self.active_status, self.label, self.entity_type)
		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.driver = self.get_neo4j_connection()

	def create_update_fund_node(self, data):
		"""."""
	
		self.initialize_variables()

		for row in data:
			try:
				properties_data = self.create_data_dictionary(self.neo4j_properties, self.neo4j_mapping, row)
				key, properties =  self.create_query_data(properties_data)
				if key and properties:
					query = self.neo4jQuery.format(key, properties)
					try:
						with self.driver.session() as session:
							session.run(query)

					except Exception as e:
						self.logg(debug_msg = 'Error while merging new node in neo4j.', 
							info_msg = 'Function = create_update_fund_node()', 
							warning_msg = 'Error in node {}.'.format(key), 
							error_msg = 'Module = '+log_file, 
							critical_msg = str(Transform.to_str(e)))
						continue
					
					else:
						pass

			except Exception as e:
				self.logg(debug_msg = 'Error while creating neo4j node query.', 
					info_msg = 'Function = create_update_fund_node()', 
					warning_msg = 'Neo4j node query error.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))

	def get_data(self, data_set, fund_id=None):
		"""."""

		if data_set == 'delta':
			data = self.fetch_all_mysql_data(self.date, 'fund', 'select', 'node')
			return data

		elif data_set == 'on_demand':
			data = self.fetch_all_mysql_data(fund_id, 'fund', 'select_ondemand', 'node')
			return data
		
		return data
	
	@classmethod
	def run(cls, data_set='delta'):
		"""."""

		fund = cls()
		data = fund.get_data(data_set)
		fund.create_update_fund_node(data)


if __name__ == '__main__':
	"""."""

	Fund.run()





