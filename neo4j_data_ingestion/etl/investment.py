"""Investment Module for investment relation in neo4j"""

from person import Person
from database import Database
from transform import Transform
from collections import OrderedDict
from fundingRound import FundingRound
from organization import Organization
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'investment.py'

class Investment(Database):
	"""Creates and updates investment relation in neo4j."""

	def __init__(self):
		"""Initialize object variables."""

		Database.__init__(self)

		self.label = 'Investment'
		self.active_status = 1
		self.entity_type = 'relation'
		self.mysql_delta_query = 'select_delta'
		self.mysql_filter_query = 'select_filtered'
		self.mysql_mapped_organization_query = 'select_mapped'
		self.date = (datetime.now() - timedelta(days=9)).strftime("%Y-%m-%d")
		self.merge_query = 'merge'
		self.delete_query = 'delete'
		self.unique_keys = ['FundingRoundId','Permalink','PrimaryRole']
		self.skip = ['Type']

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
			for key, value in data.iteritems():
				key = Transform.to_str(key)
				value = Transform.to_str(value)
		
				if key in self.unique_keys:
					if key == 'Permalink':
						item = '{}:"{}",Source:"CB"'.format('Permalink',value)
						first_key.append(item)
						continue

					elif key == 'FundingRoundId':
						item = '{}:"{}"'.format('FundingRoundId',value)
						second_key.append(item)
						continue

					elif key == 'PrimaryRole':
						first_label.append(value)
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
			first_label_string = ','.join(first_label)

		except Exception as e:
			self.logg(debug_msg = 'Error while perparing properties.', 
				info_msg = 'Function = create_query_data()', 
				warning_msg = 'Data to properties string failed.', 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(e))

		else:
			return first_label_string, first_key_string, second_key_string, property_string


	def create_data_dictionary(self, row):
		"""Converts mysql data into dictionary of neo4j properties."""

		properties = OrderedDict()

		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(self.neo4j_mapping, row)

		for key, value in self.neo4j_properties.iteritems():
			try:
				if key == 'UpdatedDate':
					properties.update({'UpdatedDate':today_date})

				elif key == 'IsLeadInvestor':
					flag = data.get(key)
					if flag == 0:
						properties.update({'IsLeadInvestor':'False'})
					elif flag == 1:
						properties.update({'IsLeadInvestor':'True'})

				elif key == 'Type':
					investor_type = Transform.to_str(data.get('Type'))
					if investor_type == 1:
						permalink = Transform.to_str(data.get('Permalink'))
						permalink, primary_role = self.get_organization_primary_role(permalink)
						label = Transform.get_neo4j_label(Transform.to_str(primary_role))
						properties.update({'PrimaryRole':label})

					elif investor_type == 2:
						properties.update({'PrimaryRole':'Person'})

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
			funding_round_id = Transform.to_str(data.get('FundingRoundId'))

			try:
				fundingRound = FundingRound()
				fundingRound.check_create_funding_round_node(funding_round_id)
				
				if label == 'Person':
					person = Person()
					person.check_create_person_node(permalink)
				
				else:
					organization = Organization()
					organization.check_create_organization_node(label, permalink)

			except Exception as e:
				pass

	def create_update_investment_relationship(self, data):
		"""Process raw data from mysql and executes neo4j queries."""

		if data:
			for row in data:
				try:
					properties_data = self.create_data_dictionary(row)
					first_label, first_key, second_key, properties =  self.create_query_data(properties_data)
					if first_label and first_key and second_key and properties:
						IsDeleted = row.get('IsDeleted')
						if IsDeleted:
							query = self.neo4jDeleteQuery.format(first_label, first_key, second_key)
						
						else:
							self.check_node_existence(properties_data)
							query = self.neo4jQuery.format(first_label, first_key, second_key, properties)
						
						self.execute_neo4j_merge_query(query)
					else:
						pass

				except Exception as e:
					self.logg(debug_msg = 'Error while creating neo4j node query.', 
						info_msg = 'Function = create_update_investment_relationship()', 
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
			investment = cls()
			data = investment.get_data(data_set='delta')
			if data:
				investment.create_update_investment_relationship(data)

		except Exception as e:
			return False

		else:
			return True


if __name__ == '__main__':
	"""."""

	Investment.run()





