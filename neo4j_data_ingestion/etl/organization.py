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

log_file = 'organization.py'

class Organization(Database):
	"""Creates and updates Organization nodes in neo4j."""

	def __init__(self):
		"""Initialize object variables."""

		Database.__init__(self)

		self.label = 'Company'
		self.active_status = 1
		self.entity_type = 'node'
		self.mysql_delta_query = 'select_delta'
		self.mysql_filter_query = 'select_filtered'
		self.mysql_mapped_organization_query = 'select_mapped'
		self.date = (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d")
		self.merge_query = 'merge'
		self.delete_query = 'delete'
		self.unique_keys = ['Permalink']
		self.to_upper_case = ['StockExchange']
		self.to_integer = [
							'EmployeeMax', 
							'EmployeeMin', 
							'cbRank', 
							'TotalFundingUSD', 
							'NumberOfInvestments',
							'FoundedOnYear',
							'FoundedOnMonth',
							'IPOMonth',
							'IPOYear',
							'ClosedOnYear',
							'ClosedOnMonth'
							]

		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.neo4jQuery = self.get_neo4j_queries(self.label, self.merge_query, self.entity_type)
		self.neo4j_properties = self.get_neo4j_properties(
			self.active_status, self.label, self.entity_type
			)

	def create_query_data(self, data):
		"""Creates dynamic string for node properties and match condition."""
		
		try:
			label = []
			properties = []
			unique_properties = []

			for key, value in data.iteritems():
				key = Transform.to_str(key)
				value = Transform.to_str(value)
		
				if key in self.unique_keys:
					item = '{}:"{}"'.format(key,value)
					unique_properties.append(item)

				elif key in self.to_integer and (value or value == 0):
					value = Transform.to_integer(value)
					item = '{}:{}'.format(key,value)
					properties.append(item)

				elif key in self.to_upper_case:
					if value:
						item = '{}:"{}"'.format(key,value.upper())
						properties.append(item)

				elif key == 'PrimaryRole':
					label.append(value)
					item = '{}:"{}"'.format(key,value)
					properties.append(item)

				elif value != 0 and not value:
					continue

				else:
					if isinstance(value, str) and '"' in value:
						value = value.replace('"','') 

					item = '{}:"{}"'.format(key,value)
					properties.append(item)

			label_string = ','.join(label)
			property_string = ','.join(properties)
			unique_string = ','.join(unique_properties)

		except Exception as e:
			self.logg(debug_msg = 'Error while perparing properties.', 
				info_msg = 'Function = create_query_data()', 
				warning_msg = 'Data to properties string failed.', 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			return label_string, unique_string, property_string


	def create_data_dictionary(self, row):
		"""Converts mysql data into dictionary of neo4j properties."""
		
		properties = OrderedDict()
		
		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(self.neo4j_mapping, row)

		for key, value in self.neo4j_properties.iteritems():
			try:
				if key == 'Source':
					properties.setdefault('Source','CB')
				
				elif key == 'UpdatedDate':
					properties.setdefault('UpdatedDate',today_date)

				elif key == 'IPOMonth':
					IPODate = data.get('IPODate')
					if IPODate:
						month = str(IPODate.month)
						properties.setdefault('IPOMonth',month)
				
				elif key == 'IPOYear':
					IPODate = data.get('IPODate')
					if IPODate:
						year = str(IPODate.year)
						properties.setdefault('IPOYear',year)
					
				elif key == 'HeadQuarters':
					hq = data.get('City')
					flag = data.get('HeadQuarterStatus')
					if flag and hq:
						properties.setdefault('HeadQuarters',hq)

				elif key == 'IsClosed':
					flag = data.get('IsClosed')
					if flag == 1:
						properties.setdefault('IsClosed','Yes')
					else:
						properties.setdefault('IsClosed','No')

				elif key == 'IsDeleted':
					flag = data.get('IsDeleted')
					if flag == 1:
						properties.setdefault('IsDeleted','Yes')
					else:
						properties.setdefault('IsDeleted','No')

				elif key == 'FoundedOnYear':
					FoundedOn = data.get('FoundedOn')
					if FoundedOn:
						year = str(FoundedOn.year)
						properties.setdefault('FoundedOnYear',year)

				elif key == 'FoundedOnMonth':
					FoundedOn = data.get('FoundedOn')
					if FoundedOn:
						month = str(FoundedOn.month)
						properties.setdefault('FoundedOnMonth',month)

				elif key == 'ClosedOnYear':
					ClosedOn = data.get('ClosedOn')
					if ClosedOn:
						year = str(ClosedOn.year)
						properties.setdefault('ClosedOnYear',year)

				elif key == 'ClosedOnMonth':
					ClosedOn = data.get('ClosedOn')
					if ClosedOn:
						month = str(ClosedOn.month)
						properties.setdefault('ClosedOnMonth',month)

				elif key == 'PrimaryRole':
					role = Transform.to_str(data.get('PrimaryRole'))
					label = Transform.get_neo4j_label(role)
					if label:
						properties.setdefault('PrimaryRole',label)
					else:
						properties.setdefault('PrimaryRole','')

				elif key == 'IPOStatus':
					ipo_id = data.get('IpoId')
					if ipo_id:
						deleted_ipo = data.get('DeletedIpo')
						if not deleted_ipo:
							properties.setdefault('IPOStatus','Public')

				elif key == 'CBUrl':
					relative_path = data.get('CBUrl')
					if relative_path:
						relative_path = Transform.to_str(relative_path)
						absolute_path = 'https://www.crunchbase.com/{}'.format(relative_path)
						properties.setdefault('CBUrl',absolute_path)

				else:
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

	def create_update_organization_node(self, data):
		"""Process raw data from mysql and executes neo4j queries."""
		
		if data:
			for row in data:
				try:
					properties_data = self.create_data_dictionary(row)
					label, key, properties =  self.create_query_data(properties_data)
					if label and key and properties:
						query = self.neo4jQuery.format(label, key, properties)
						self.execute_neo4j_merge_query(query)
					else:
						pass

				except Exception as e:
					self.logg(debug_msg = 'Error while creating neo4j node query.',
						info_msg = 'Function = create_update_organization_node()',
						warning_msg = 'Error in creating dynamic node query.',
						error_msg = 'Module = '+log_file,
						critical_msg = str(e))
					continue

	def check_create_organization_node(self, label, permalink):
		"""Check if the organization exists in neo4j.
		   If node does not exists creates a new node.
		"""

		query = 'MATCH (n:{}) where n.Permalink = "{}" RETURN n.Permalink'\
		.format(label, permalink)

		result = self.execute_neo4j_match_query(query)

		if result:
			items = [item for item in result]
			try:
				Permalink = items[0]['n.Permalink']

			except IndexError as e:
				data = self.get_data(data_set='selected', permalink=permalink)
				if data:
					self.create_update_organization_node(data)
					return True

			except Exception as e:
				pass

	def get_data(self, data_set=None, permalink=None):
		"""Fetch data from mysql w.r.t node."""

		if data_set == 'delta':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_delta_query, self.entity_type, match=self.date
				)
			return data

		elif data_set == 'selected':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_filter_query, self.entity_type, match=permalink
				)
			return data

		elif data_set == 'mapped':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_mapped_organization_query, self.entity_type, match=None
				)
			return data

		elif data_set == 'refresh':
			data = self.fetch_all_mysql_data(
				self.label, 'select_refresh', self.entity_type, match=None
				)
			return data
	
	@classmethod
	def run(cls):
		"""."""

		try:
			organization = cls()
			data = organization.get_data(data_set='delta')
			if data:
				organization.create_update_organization_node(data)
		
		except Exception as e:
			return False
		
		else:
			return True

if __name__ == '__main__':
	"""."""

	# Organization.run()


	obj = Organization()
	data = obj.get_data(data_set='selected',permalink='red-hat')
	if data:
		obj.create_update_organization_node(data)







	





