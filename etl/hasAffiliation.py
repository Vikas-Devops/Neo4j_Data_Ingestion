
from person import Person
from company import Company
from database import Database
from transform import Transform
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'hasAffiliation.py'

class HasAffiliation(Database):
	"""."""

	def __init__(self):
		"""."""

		Database.__init__(self)

		self.active_status = 1
		self.query_type = 'merge'
		self.entity_type = 'relation'
		self.label = 'hasAffiliation'
		self.date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
		self.unique_keys = ['Permalink', 'PeoplePermalink', 'PrimaryRole']
		self.skip = []

	def create_query_data(self, data):
		"""."""
		
		try:
			properties = []
			first_key = []
			second_key = []
			second_label = []

			for key, value in data.iteritems():
				key = Transform.to_str(key)
				value = Transform.to_str(value)
		
				if key in self.unique_keys:
					if key == 'Permalink':
						item = '{}:"{}",Source:"CB"'.format('Permalink',value)
						second_key.append(item)
						continue

					elif key == 'PeoplePermalink':
						item = '{}:"{}"'.format('Permalink',value)
						first_key.append(item)
						continue

					elif key == 'PrimaryRole':
						second_label.append(value)
						continue

				if key in self.skip:
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
			second_label_string = ','.join(second_label)

		except Exception as e:
			self.logg(debug_msg = 'Error while perparing properties.', 
				info_msg = 'Function = create_query_data()', 
				warning_msg = 'Data to properties string failed.', 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			return first_key_string, second_label_string, second_key_string, property_string


	def create_data_dictionary(self, properties, neo4j_mapping, row):
		"""."""

		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(neo4j_mapping, row)

		for key, value in properties.iteritems():
			try:
				if key == 'UpdatedDate':
					properties.update({'UpdatedDate':today_date})

				elif key == 'IsCurrent':
					flag = data.get('IsCurrent')
					if flag == 1:
						properties.update({'IsCurrent':'true'})

					elif flag == 0 :
						properties.update({'IsCurrent':'false'})

					else:
						properties.update({'IsCurrent':None})

				elif key == 'StartedOnYear':
					StartedOn = data.get('StartedOn')
					if StartedOn:
						year = str(StartedOn.year)
						properties.update({'StartedOnYear':year})

				elif key == 'StartedOnMonth':
					StartedOn = data.get('StartedOn')
					if StartedOn:
						month = str(StartedOn.month)
						properties.update({'StartedOnMonth':month})

				elif key == 'PrimaryRole':
					role = data.get('PrimaryRole')
					label = Transform.get_neo4j_label(role)
					if label:
						properties.update({'PrimaryRole':label})

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
				warning_msg = 'Error in node {}.'.format(query), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(Transform.to_str(e)))

		else:
			items = [item for item in result]
			try:
				Permalink = items[0]['n.Permalink']

			except IndexError as e:
				company = Company()
				data = company.get_data('on_demand', permalink=permalink)
				if data:
					company.create_update_organization_node(data)
				else:
					pass
				return True

			except Exception as e:
				pass

	def check_create_person_node(self, permalink):
		"""."""

		query = 'MATCH (p:Person) where p.Permalink = "{}" RETURN p.Permalink'\
		.format(permalink)

		try:
			with self.driver.session() as session:
				result = session.run(query)

		except Exception as e:
			self.logg(debug_msg = 'Error while checking Person node existence.', 
				info_msg = 'Function = check_create_person_node()', 
				warning_msg = 'Error in node {}.'.format(query), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(Transform.to_str(e)))

		else:
			items = [item for item in result]
			try:
				Permalink = items[0]['p.Permalink']

			except IndexError as e:
				person = Person()
				data = person.get_data('on_demand', permalink=permalink)
				if data:
					person.create_update_person_node(data)
				else:
					pass
				return True

			except Exception as e:
				pass

	def check_node_existence(self, data):
		"""."""

		permalink = Transform.to_str(data.get('Permalink'))
		label = Transform.to_str(data.get('PrimaryRole'))
		people_permalink = Transform.to_str(data.get('PeoplePermalink'))
		self.check_create_organization_node(label, permalink)
		self.check_create_person_node(people_permalink)
	
	def initialize_variables(self):
		"""."""

		self.neo4jQuery = self.get_neo4j_queries('hasAffiliation', 'merge', 'relation')
		self.neo4j_properties = self.get_neo4j_properties(self.active_status, self.label, self.entity_type)
		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.driver = self.get_neo4j_connection()

	def create_update_hasAffiliation_relationship(self, data):
		"""."""
	
		self.initialize_variables()

		for row in data:
			try:
				properties_data = self.create_data_dictionary(self.neo4j_properties, self.neo4j_mapping, row)
				
				self.check_node_existence(properties_data)

				first_key, second_label, second_key, properties =  self.create_query_data(properties_data)

				if first_key and second_label and second_key and properties:
					query = self.neo4jQuery.format(first_key, second_label, second_key, properties)
					try:
						with self.driver.session() as session:
							session.run(query)

					except Exception as e:
						self.logg(debug_msg = 'Error while merging new relationship in neo4j.', 
							info_msg = 'Function = create_update_hasAffiliation_relationship()', 
							warning_msg = 'Error in node {}.'.format(first_key), 
							error_msg = 'Module = '+log_file, 
							critical_msg = str(Transform.to_str(e)))
						continue
					
					else:
						pass

			except Exception as e:
				self.logg(debug_msg = 'Error while creating neo4j node query.', 
					info_msg = 'Function = create_update_hasAffiliation_relationship()', 
					warning_msg = 'Neo4j node query error.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))


	def get_data(self, data_set):
		"""."""
		
		data = self.fetch_all_mysql_data(self.date, 'hasAffiliation', 'select', 'relation')
		return data

	@classmethod
	def run(cls, data_set='delta'):
		"""."""

		hasAffiliation = cls()
		data = hasAffiliation.get_data(data_set)
		hasAffiliation.create_update_hasAffiliation_relationship(data)


if __name__ == '__main__':
	"""."""

	HasAffiliation.run()





