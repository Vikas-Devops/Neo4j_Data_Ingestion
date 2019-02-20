"""HasAcquired Module for hasAcquired relation in neo4j"""

from database import Database
from transform import Transform
from collections import OrderedDict
from organization import Organization
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'hasAcquired.py'

class HasAcquired(Database):
	"""Creates and updates hasAcquired relation in neo4j."""

	def __init__(self):
		"""Initialize object variables."""

		Database.__init__(self)

		self.label = 'hasAcquired'
		self.active_status = 1
		self.entity_type = 'relation'
		self.mysql_delta_query = 'select_delta'
		self.mysql_filter_query = 'select_filtered'
		self.mysql_mapped_organization_query = 'select_mapped'
		self.date = (datetime.now() - timedelta(days=9)).strftime("%Y-%m-%d")
		self.merge_query = 'merge'
		self.delete_query = 'delete'
		self.to_integer = [
							'TransactionValueUSD', 
							'AnnoundedOnYear', 
							'AnnoundedOnMonth',
							'CompletedOnYear', 
							'CompletedOnMonth'
							]
		self.unique_keys = [
							'OrganizationAcquireeID',
							'OrganizationAcquirerID',
							'AcquireePrimaryRole',
							'AcquirerPrimaryRole'
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
			first_key = []
			second_key = []
			first_label = []
			second_label = []
			for key, value in data.iteritems():
				key = Transform.to_str(key)
				value = Transform.to_str(value)
		
				if key in self.unique_keys:
					if key == 'OrganizationAcquirerID':
						item = '{}:"{}"'.format('Permalink',value)
						first_key.append(item)
						continue
					
					elif key == 'OrganizationAcquireeID':
						item = '{}:"{}"'.format('Permalink',value)
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

	def create_data_dictionary(self, row):
		"""Converts mysql data into dictionary of neo4j properties."""

		properties = OrderedDict()

		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(self.neo4j_mapping, row)

		for key, value in self.neo4j_properties.iteritems():
			try:
				if key == 'UpdatedDate':
					properties.update({'UpdatedDate':today_date})

				elif key == 'AnnoundedOnYear':
					AnnouncedOn = data.get('AnnoundedOn')
					if AnnouncedOn:
						year = str(AnnouncedOn.year)
						properties.update({'AnnoundedOnYear':year})

				elif key == 'CompletedOnYear':
					CompletedOn = data.get('CompletedOn')
					if CompletedOn:
						year = str(CompletedOn.year)
						properties.update({'CompletedOnYear':year})

				elif key == 'CompletedOnMonth':
					CompletedOn = data.get('CompletedOn')
					if CompletedOn:
						month = str(CompletedOn.month)
						properties.update({'CompletedOnMonth':month})

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

	def check_node_existence(self, data):
		"""Checks if the nodes for the relationship exists, if not creates new nodes."""
		
		if data:
			permalinks = [
							(data.get('OrganizationAcquireeID'), data.get('AcquireePrimaryRole')),
							(data.get('OrganizationAcquirerID'), data.get('AcquirerPrimaryRole'))
							]

			for permalink, label in permalinks:
				permalink = Transform.to_str(permalink)
				label = Transform.to_str(label)
				try:
					organization = Organization()
					organization.check_create_organization_node(label, permalink)
				except Exception as e:
					pass

	def create_update_hasAcquired_relationship(self, data):
		"""Process raw data from mysql and executes neo4j queries."""

		if data:
			for row in data:
				try:
					properties_data = self.create_data_dictionary(row)
					first_label, first_key, second_label, second_key, properties =  self.create_query_data(properties_data)
					if first_label and first_key and second_label and second_key and properties:
						IsDeleted = row.get('IsDeleted')
						if IsDeleted:
							query = self.neo4jDeleteQuery.format(first_label, first_key, second_label, second_key)
						else:
							self.check_node_existence(properties_data)
							query = self.neo4jQuery.format(first_label, first_key, second_label, second_key, properties)

						self.execute_neo4j_merge_query(query)
					else:
						pass

				except Exception as e:
					self.logg(debug_msg = 'Error while creating neo4j node query.', 
						info_msg = 'Function = create_update_hasAcquired_relationship()', 
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
			
			hasAcquired = cls()
			data = hasAcquired.get_data(data_set='delta')
			if data:
				hasAcquired.create_update_hasAcquired_relationship(data)

		except Exception as e:
			return False

		else:
			return True


if __name__ == '__main__':
	"""."""

	HasAcquired.run()





