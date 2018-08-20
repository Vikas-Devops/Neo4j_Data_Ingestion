
from fund import Fund
from company import Company
from database import Database
from transform import Transform
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'hasFund.py'

class HasFund(Database):
	"""."""

	def __init__(self):
		"""."""

		Database.__init__(self)

		self.active_status = 1
		self.query_type = 'merge'
		self.entity_type = 'relation'
		self.label = 'hasFund'
		self.date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
		self.unique_keys = ['FundsId','Permalink','PrimaryRole']
		self.skip = []

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

					elif key == 'FundsId':
						item = '{}:"{}"'.format('FundsId',value)
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
				company.create_update_organization_node(data)
				return True

			except Exception as e:
				pass

	def check_create_fund_node(self, fund_id):
		"""."""

		query = 'MATCH (f:Fund) where f.FundsId = "{}" RETURN f.FundsId'\
		.format(fund_id)

		try:
			with self.driver.session() as session:
				result = session.run(query)

		except Exception as e:
			self.logg(debug_msg = 'Error while checking fund node existence.', 
				info_msg = 'Function = check_create_fund_node()', 
				warning_msg = 'Error in node {}.'.format(query), 
				error_msg = 'Module = '+log_file, 
				critical_msg = str(Transform.to_str(e)))

		else:
			items = [item for item in result]
			try:
				Fund_ID = items[0]['f.FundsId']

			except IndexError as e:
				fund = Fund()
				data = fund.get_data('on_demand', fund_id=fund_id)
				fund.create_update_fund_node(data)
				return True

			except Exception as e:
				pass

	def check_node_existence(self, data):
		"""."""

		permalink = Transform.to_str(data.get('Permalink'))
		label = Transform.to_str(data.get('PrimaryRole'))
		fund_id = Transform.to_str(data.get('FundsId'))
		self.check_create_organization_node(label, permalink)
		self.check_create_fund_node(fund_id)


	def initialize_variables(self):
		"""."""

		self.neo4jQuery = self.get_neo4j_queries('hasFund', 'merge', 'relation')
		self.neo4j_properties = self.get_neo4j_properties(self.active_status, self.label, self.entity_type)
		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.driver = self.get_neo4j_connection()

	def create_update_hasFund_relationship(self, data):
		"""."""
	
		self.initialize_variables()

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
							info_msg = 'Function = create_update_hasFund_relationship()', 
							warning_msg = 'Error in node {}.'.format(key), 
							error_msg = 'Module = '+log_file, 
							critical_msg = str(Transform.to_str(e)))
						continue
					
					else:
						pass

			except Exception as e:
				self.logg(debug_msg = 'Error while creating neo4j node query.', 
					info_msg = 'Function = create_update_hasFund_relationship()', 
					warning_msg = 'Neo4j node query error.', 
					error_msg = 'Module = '+log_file, 
					critical_msg = str(e))


	def get_data(self, data_set):
		"""."""
		
		data = self.fetch_all_mysql_data(self.date, 'hasFund', 'select', 'relation')
		return data

	@classmethod
	def run(cls, data_set='delta'):
		"""."""

		hasFund = cls()
		data = hasFund.get_data(data_set)
		hasFund.create_update_hasFund_relationship(data)


if __name__ == '__main__':
	"""."""

	HasFund.run()





