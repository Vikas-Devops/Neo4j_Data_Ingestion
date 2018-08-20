
from fundingRound import FundingRound
from company import Company
from database import Database
from transform import Transform
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'investment.py'

class Investment(Database):
	"""."""

	def __init__(self):
		"""."""

		Database.__init__(self)

		self.active_status = 1
		self.query_type = 'merge'
		self.entity_type = 'relation'
		self.label = 'Investment'
		self.date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
		self.unique_keys = ['FundingRoundId','Permalink','PrimaryRole']
		self.skip = ['Type']

	def create_query_data(self, data):
		"""."""
		
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


	def create_data_dictionary(self, properties, neo4j_mapping, row):
		"""."""

		today_date = datetime.now().strftime("%Y-%m-%d")
		data = Transform.map_data(neo4j_mapping, row)

		for key, value in properties.iteritems():
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
				company.create_update_organization_node(data)
				return True

			except Exception as e:
				pass

	def check_create_funding_round_node(self, funding_round_id):
		"""."""

		query = 'MATCH (f:FundingRound) where f.FundingRoundId = "{}" RETURN f.FundingRoundId'\
		.format(funding_round_id)

		try:
			with self.driver.session() as session:
				result = session.run(query)

		except Exception as e:
			self.logg(debug_msg = 'Error while checking funding round node existence.', 
				info_msg = 'Function = check_create_funding_round_node()', 
				warning_msg = 'Error in node {}.'.format(query), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(Transform.to_str(e)))

		else:
			items = [item for item in result]
			try:
				Funding_Round_ID = items[0]['f.FundingRoundId']

			except IndexError as e:
				fundingRound = FundingRound()
				data = fundingRound.get_data('on_demand', funding_round_id=funding_round_id)
				fundingRound.create_update_funding_round_node(data)
				return True

			except Exception as e:
				pass

	def check_node_existence(self, data):
		"""."""
		
		permalink = Transform.to_str(data.get('Permalink'))
		label = Transform.to_str(data.get('PrimaryRole'))
		funding_round_id = Transform.to_str(data.get('FundingRoundId'))

		if label == 'Person':
			print 'People Node'
			pass

		else:
			self.check_create_organization_node(label, permalink)

		self.check_create_funding_round_node(funding_round_id)


	def initialize_variables(self):
		"""."""

		self.neo4jQuery = self.get_neo4j_queries('Investment', 'merge', 'relation')
		self.neo4j_properties = self.get_neo4j_properties(self.active_status, self.label, self.entity_type)
		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.driver = self.get_neo4j_connection()

	def create_update_investment_relationship(self, data):
		"""."""
	
		self.initialize_variables()
		# import pdb; pdb.set_trace()
		for row in data:
			try:
				properties_data = self.create_data_dictionary(self.neo4j_properties, self.neo4j_mapping, row)
				
				self.check_node_existence(properties_data)

				first_label, first_key, second_key, properties =  self.create_query_data(properties_data)
				if first_label and first_key and second_key and properties:
					query = self.neo4jQuery.format(first_label, first_key, second_key, properties)
					try:
						with self.driver.session() as session:
							session.run(query)

					except Exception as e:
						self.logg(debug_msg = 'Error while merging new relationship in neo4j.', 
							info_msg = 'Function = create_update_investment_relationship()', 
							warning_msg = 'Error in node {}.'.format(key), 
							error_msg = 'Module = '+log_file, 
							critical_msg = str(Transform.to_str(e)))
						continue
					
					else:
						pass

			except Exception as e:
				self.logg(debug_msg = 'Error while creating neo4j node query.', 
					info_msg = 'Function = create_update_investment_relationship()', 
					warning_msg = 'Neo4j node query error.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))


	def get_data(self, data_set):
		"""."""
		
		data = self.fetch_all_mysql_data(self.date, 'Investment', 'select', 'relation')
		return data

	@classmethod
	def run(cls, data_set='delta'):
		"""."""

		investment = cls()
		data = investment.get_data(data_set)
		investment.create_update_investment_relationship(data)


if __name__ == '__main__':
	"""."""

	Investment.run()





