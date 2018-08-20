
from company import Company
from database import Database
from transform import Transform
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'hasAcquired.py'

class HasAcquired(Database):
	"""."""

	def __init__(self):
		"""."""

		Database.__init__(self)

		self.active_status = 1
		self.query_type = 'merge'
		self.entity_type = 'relation'
		self.label = 'hasAcquired'
		self.date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
		self.to_integer = ['TransactionValueUSD']
		self.unique_keys = [
							'OrganizationAcquireeID',
							'OrganizationAcquirerID',
							'AcquireePrimaryRole',
							'AcquirerPrimaryRole'
							]

	def create_query_data(self, data):
		"""."""
		
		try:
			properties = []
			first_key = []
			second_key = []
			first_label = []
			second_label = []
			for key, value in data.iteritems():
				key = Transform.to_str(key)
				value = Transform.to_str(value)
		
				if key in self.unique_keys:
					if key == 'OrganizationAcquirerID':
						item = '{}:"{}",Source:"CB"'.format('Permalink',value)
						first_key.append(item)
						continue
					
					elif key == 'OrganizationAcquireeID':
						item = '{}:"{}",Source:"CB"'.format('Permalink',value)
						second_key.append(item)
						continue
					
					elif key == 'AcquirerPrimaryRole':
						first_label.append(value)
						continue

					elif key == 'AcquireePrimaryRole':
						second_label.append(value)
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
			first_key_string = ','.join(first_key)
			second_key_string = ','.join(second_key)
			first_label_string = ','.join(first_label)
			second_label_string = ','.join(second_label)

		except Exception as e:
			self.logg(debug_msg = 'Error while perparing properties.', 
				info_msg = 'Function = create_query_data()', 
				warning_msg = 'Data to properties string failed.', 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			return first_label_string, first_key_string, second_label_string, second_key_string, property_string


	def create_data_dictionary(self, properties, neo4j_mapping, row):
		"""."""
		
		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(neo4j_mapping, row)

		for key, value in properties.iteritems():
			try:
				if key == 'UpdatedDate':
					properties.update({'UpdatedDate':today_date})

				elif key == 'AnnoundedOnYear':
					AnnouncedOn = data.get('AnnoundedOn')
					if AnnouncedOn:
						year = str(AnnouncedOn.year)
						properties.update({'AnnoundedOnYear':year})

				elif key == 'AnnoundedOnMonth':
					AnnouncedOn = data.get('AnnoundedOn')
					if AnnouncedOn:
						month = str(AnnouncedOn.month)
						properties.update({'AnnoundedOnMonth':month})

				elif key == 'OrganizationAcquireeID':
					org_id = data.get('OrganizationAcquireeID')
					permalink, primary_role = self.get_organization_permalink(org_id)
					
					if permalink and primary_role:
						label = Transform.get_neo4j_label(primary_role)
						properties.update({'OrganizationAcquireeID':permalink})
						properties.update({'AcquireePrimaryRole':label})
					else:
						properties.update({'OrganizationAcquireeID':permalink})
						properties.update({'AcquireePrimaryRole':primary_role})
				
				elif key == 'OrganizationAcquirerID':
					org_id = data.get('OrganizationAcquirerID')
					permalink, primary_role = self.get_organization_permalink(org_id)
					if permalink and primary_role:
						label = Transform.get_neo4j_label(primary_role)
						properties.update({'OrganizationAcquirerID':permalink})
						properties.update({'AcquirerPrimaryRole':label})
					else:
						properties.update({'OrganizationAcquirerID':permalink})
						properties.update({'AcquirerPrimaryRole':primary_role})

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

	def check_create_organization_node(self, label, permalink):
		"""."""
	
		query = 'MATCH (n:{}) where n.Permalink = "{}" and n.Source = "CB" RETURN n.Permalink'\
		.format(label, permalink)

		try:
			with self.driver.session() as session:
				result = session.run(query)

		except Exception as e:
			self.logg(debug_msg = 'Error while checking company node existence.', 
				info_msg = 'Function = check_create_company_node()', 
				warning_msg = 'Error in node {}.'.format(key), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(Transform.to_str(e)))

		else:
			items = [item for item in result]
			try:
				Permalink = items[0]['n.Permalink']

			except IndexError as e:
				company = Company()
				data = company.get_data('on_demand', permalink=permalink)
				company.create_update_organization_node(data)
				return True

			except Exception as e:
				pass

	def check_node_existence(self, data):
		"""."""

		permalinks = [
						(data.get('OrganizationAcquireeID'), data.get('AcquireePrimaryRole')),
						(data.get('OrganizationAcquirerID'), data.get('AcquirerPrimaryRole'))
						]

		for permalink in permalinks:
			permalink = Transform.to_str(permalink[0])
			label = Transform.to_str(permalink[1])
			self.check_create_organization_node(label, permalink)

	def initialize_variables(self):
		"""."""

		self.neo4jQuery = self.get_neo4j_queries('hasAcquired', 'merge', 'relation')
		self.neo4j_properties = self.get_neo4j_properties(self.active_status, self.label, self.entity_type)
		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.driver = self.get_neo4j_connection()

	def create_update_hasAcquired_relationship(self, data):
		"""."""
		
		self.initialize_variables()


		for row in data:
			try:
				properties_data = self.create_data_dictionary(self.neo4j_properties, self.neo4j_mapping, row)
				
				self.check_node_existence(properties_data)

				first_label, first_key, second_label, second_key, properties =  self.create_query_data(properties_data)
				if first_label and first_key and second_label and second_key and properties:
					query = self.neo4jQuery.format(first_label, first_key, second_label, second_key, properties)
					try:
						with self.driver.session() as session:
							session.run(query)

					except Exception as e:
						self.logg(debug_msg = 'Error while merging new relationship in neo4j.', 
							info_msg = 'Function = create_update_hasAcquired_relationship()', 
							warning_msg = 'Error in node {}.'.format(first_key), 
							error_msg = 'Module = '+log_file, 
							critical_msg = str(Transform.to_str(e)))
						continue
					
					else:
						pass

			except Exception as e:
				self.logg(debug_msg = 'Error while creating neo4j node query.', 
					info_msg = 'Function = create_update_hasAcquired_relationship()', 
					warning_msg = 'Neo4j node query error.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))

	def get_data(self, data_set):
		"""."""
		
		data = self.fetch_all_mysql_data(self.date, 'hasAcquired', 'select', 'relation')

		return data

	@classmethod
	def run(cls, data_set='delta'):
		"""."""

		hasAcquired = cls()
		data = hasAcquired.get_data(data_set)
		hasAcquired.create_update_hasAcquired_relationship(data)


if __name__ == '__main__':
	"""."""

	HasAcquired.run()





