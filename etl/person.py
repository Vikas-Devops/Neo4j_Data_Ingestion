

from database import Database
from transform import Transform
from datetime import datetime, timedelta

log_file = 'person.py'

class Person(Database):
	"""."""

	def __init__(self):
		"""."""

		Database.__init__(self)

		self.active_status = 1
		self.query_type = 'merge'
		self.entity_type = 'node'
		self.label = 'person'
		self.neo4jQuery = ''
		self.date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
		self.unique_keys = ['Permalink']
		self.to_integer = ['cbRank']


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
				if key == 'PCreatedDate':
					properties.update({'PCreatedDate':today_date})

				elif key == 'Gender':
					flag = data.get('Gender')
					if flag == 2:
						properties.update({'Gender':'Female'})
					elif flag == 1:
						properties.update({'Gender':'Male'})
					else:
						properties.update({'Gender':''})

				elif key == 'CBUrl':
					relative_path = data.get('CBUrl')
					if relative_path:
						relative_path = Transform.to_str(relative_path)
						absolute_path = 'https://www.crunchbase.com/{}'.format(relative_path)
						properties.update({'CBUrl':absolute_path})

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

		self.neo4jQuery = self.get_neo4j_queries('person', 'merge', 'node')
		self.neo4j_properties = self.get_neo4j_properties(self.active_status, self.label, self.entity_type)
		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.driver = self.get_neo4j_connection()

	def create_update_person_node(self, data):
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
							info_msg = 'Function = create_update_person_node()', 
							warning_msg = 'Error in node {}.'.format(key), 
							error_msg = 'Module = '+log_file, 
							critical_msg = str(Transform.to_str(e)))
						continue
					
					else:
						pass

			except Exception as e:
				self.logg(debug_msg = 'Error while creating neo4j node query.', 
					info_msg = 'Function = create_update_person_node()', 
					warning_msg = 'Neo4j node query error.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))


	def get_data(self, data_set, permalink=None):
		"""."""

		if data_set == 'delta':
			data = self.fetch_all_mysql_data(self.date, 'person', 'select', 'node')
			return data

		elif data_set == 'on_demand':
			data = self.fetch_all_mysql_data(permalink, 'person', 'select_ondemand', 'node')
			return data

	@classmethod
	def run(cls, data_set='delta'):
		"""."""

		person = cls()
		data = person.get_data(data_set)
		person.create_update_person_node(data)


if __name__ == '__main__':
	"""."""

	Person.run()

