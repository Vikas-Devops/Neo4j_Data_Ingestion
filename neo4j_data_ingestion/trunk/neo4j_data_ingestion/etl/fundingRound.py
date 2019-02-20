"""FundingRound Module for Funding Round nodes in neo4j"""

from database import Database
from transform import Transform
from collections import OrderedDict
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'fundingRound.py'

class FundingRound(Database):
	"""Creates and updates Funding Round nodes in neo4j."""

	def __init__(self):
		"""Initialize object variables."""

		Database.__init__(self)

		self.label = 'FundingRound'
		self.active_status = 1
		self.entity_type = 'node'
		self.mysql_delta_query = 'select_delta'
		self.mysql_filter_query = 'select_filtered'
		self.mysql_mapped_organization_query = 'select_mapped'
		self.date = (datetime.now() - timedelta(days=4)).strftime("%Y-%m-%d")
		self.merge_query = 'merge'
		self.delete_query = 'delete'
		self.unique_keys = ['FundingRoundId']
		self.to_upper_case = ['FundingSeries']
		self.to_integer = [
							'MoneyRaisedUSD',
							'AnnouncedOnMonth',
							'AnnouncedOnYear',
							'PostMoneyValuationUSD',
							'TargetMoneyRaisedUSD',
							'ClosedOnYear',
							'ClosedOnMonth'
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

				if key in self.to_upper_case and value:
					if value:
						item = '{}:"{}"'.format(key,value.upper())
						properties.append(item)
				
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

				elif key == 'ClosedOnYear':
					ClosedOn = data.get('ClosedOn')
					if ClosedOn:
						year = str(ClosedOn.year)
						properties.update({'ClosedOnYear':year})

				elif key == 'ClosedOnMonth':
					ClosedOn = data.get('ClosedOn')
					if ClosedOn:
						month = str(ClosedOn.month)
						properties.update({'ClosedOnMonth':month})
						
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

	def create_update_funding_round_node(self, data):
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
							# query = self.neo4jQuery.format(key, properties)
						else:
							query = self.neo4jQuery.format(key, properties)
						
						self.execute_neo4j_merge_query(query)
					else:
						pass

				except Exception as e:
					self.logg(debug_msg = 'Error while creating neo4j node query.', 
						info_msg = 'Function = create_update_funding_round_node()',
						warning_msg = 'Error in creating dynamic node query.',
						error_msg = 'Module = '+log_file,
						critical_msg = str(e))
					continue

	def check_create_funding_round_node(self, funding_round_id):
		"""Check if the funding round exists in neo4j.
		   If node does not exists creates a new node.
		"""

		query = 'MATCH (f:FundingRound) where f.FundingRoundId = "{}" RETURN f.FundingRoundId'\
		.format(funding_round_id)

		result = self.execute_neo4j_match_query(query)

		if result:
			items = [item for item in result]
			try:
				Funding_Round_ID = items[0]['f.FundingRoundId']

			except IndexError as e:
				data = self.get_data(data_set='selected', funding_round_id=funding_round_id)
				if data:
					self.create_update_funding_round_node(data)
					return True

			except Exception as e:
				pass

	def get_data(self, data_set, funding_round_id=None):
		"""Fetch data from mysql w.r.t node."""

		if data_set == 'delta':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_delta_query, self.entity_type, match=self.date
				)
			return data

		elif data_set == 'selected':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_filter_query, self.entity_type, match=funding_round_id
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
			fundingRound = cls()
			data = fundingRound.get_data(data_set='delta')
			if data:
				fundingRound.create_update_funding_round_node(data)

		except Exception as e:
			return False

		else:
			return True


if __name__ == '__main__':
	"""."""

	FundingRound.run()





