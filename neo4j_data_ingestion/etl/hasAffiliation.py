"""HasAffiliation Module for hasAffiliation relation in neo4j"""

from person import Person
from database import Database
from transform import Transform
from collections import OrderedDict
from organization import Organization
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'hasAffiliation.py'

class HasAffiliation(Database):
	"""Creates and updates hasAffiliation relation in neo4j."""

	def __init__(self):
		"""Initialize object variables."""

		Database.__init__(self)

		self.label = 'hasAffiliation'
		self.active_status = 1
		self.entity_type = 'relation'
		self.mysql_delta_query = 'select_delta'
		self.mysql_filter_query = 'select_filtered'
		self.mysql_mapped_organization_query = 'select_mapped'
		self.date = (datetime.now() - timedelta(days=9)).strftime("%Y-%m-%d")
		self.merge_query = 'merge'
		self.delete_query = 'delete'
		self.unique_keys = ['Permalink', 'PeoplePermalink', 'PrimaryRole']
		self.to_integer = [
							'StartedOnYear',
							'StartedOnMonth'
							'EndedOnYear',
							'EndedOnMonth',
							'PeopleJobsId'
							]
		self.skip = []
		

		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.neo4jQuery = self.get_neo4j_queries(self.label, self.merge_query, self.entity_type)
		# self.neo4jDeleteQuery = self.get_neo4j_queries(self.label, self.delete_query, self.entity_type)
		self.neo4j_properties = self.get_neo4j_properties(
			self.active_status, self.label, self.entity_type
			)

	def create_query_data(self, data):
		"""Creates dynamic string for node properties and match condition."""
		
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

	def create_data_dictionary(self, row):
		"""Converts mysql data into dictionary of neo4j properties."""

		properties = OrderedDict()

		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(self.neo4j_mapping, row)

		for key, value in self.neo4j_properties.iteritems():
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

				elif key == 'EndedOnYear':
					EndedOn = data.get('EndedOn')
					if EndedOn:
						year = str(EndedOn.year)
						properties.update({'EndedOnYear':year})

				elif key == 'EndedOnMonth':
					EndedOn = data.get('EndedOn')
					if EndedOn:
						month = str(EndedOn.month)
						properties.update({'EndedOnMonth':month})

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

	def check_node_existence(self, data):
		"""Checks if the nodes for the relationship exists, if not creates new nodes."""

		if data:
			permalink = Transform.to_str(data.get('Permalink'))
			label = Transform.to_str(data.get('PrimaryRole'))
			people_permalink = Transform.to_str(data.get('PeoplePermalink'))

			try:
				organization = Organization()
				organization.check_create_organization_node(label, permalink)
				person = Person()
				person.check_create_person_node(people_permalink)

			except Exception as e:
				pass
	
	def create_update_hasAffiliation_relationship(self, data):
		"""Process raw data from mysql and executes neo4j queries."""
		
		if data:
			for row in data:
				try:
					properties_data = self.create_data_dictionary(row)
					self.check_node_existence(properties_data)
					first_key, second_label, second_key, properties =  self.create_query_data(properties_data)
					if first_key and second_label and second_key and properties:
						query = self.neo4jQuery.format(first_key, second_label, second_key, properties)
						self.execute_neo4j_merge_query(query)
					else:
						pass

				except Exception as e:
					self.logg(debug_msg = 'Error while creating neo4j node query.', 
						info_msg = 'Function = create_update_hasAffiliation_relationship()',
						warning_msg = 'Error in creating dynamic node query.',
						error_msg = 'Module = '+log_file,
						critical_msg = str(e))


	def get_data(self, data_set):
		"""Fetch data from mysql w.r.t relation."""
		
		if data_set == 'delta':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_delta_query, self.entity_type, match=self.date
				)
			return data

		elif data_set == 'mapped':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_mapped_organization_query, self.entity_type, match=None
				)
			return data

	@classmethod
	def run(cls):
		"""."""

		try:
			hasAffiliation = cls()
			data = hasAffiliation.get_data(data_set='delta')
			if data:
				hasAffiliation.create_update_hasAffiliation_relationship(data)

		except Exception as e:
			return False

		else:
			return True

if __name__ == '__main__':
	"""."""

	HasAffiliation.run()





