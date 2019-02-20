"""FundingRound Module for Event nodes in neo4j"""

from database import Database
from transform import Transform
from collections import OrderedDict
from datetime import datetime, timedelta

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"

log_file = 'event.py'

class Event(Database):
	"""Creates and updates Event nodes in neo4j."""

	def __init__(self):
		"""Initialize object variables."""

		Database.__init__(self)

		self.label = 'Event'
		self.active_status = 1
		self.entity_type = 'node'
		self.mysql_delta_query = 'select_delta'
		self.mysql_filter_query = 'select_filtered'
		self.mysql_all_data_query = 'select_all'
		self.date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
		self.create_query = 'create'
		self.delete_query = 'delete'
		self.unique_keys = ['Uuid']
		self.to_integer = [
							'StartOnYear', 
							'StartOnMonth',
							'EndOnYear',
							'EndOnMonth'
							]

		self.neo4j_mapping = self.get_neo4j_mapping(self.label)
		self.neo4jQuery = self.get_neo4j_queries(self.label, self.create_query, self.entity_type)
		self.neo4jDeleteQuery = self.get_neo4j_queries(self.label, self.delete_query, self.entity_type)
		self.neo4j_properties = self.get_neo4j_properties(
			self.active_status, self.label, self.entity_type
			)

	def delete_all_event_node_and_relationships(self):
		"""."""

		status = self.execute_neo4j_merge_query(self.neo4jDeleteQuery)
		return status

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

				elif key == 'StartOnMonth':
					AnnouncedOn = data.get('StartOn')
					if AnnouncedOn:
						month = str(AnnouncedOn.month)
						properties.update({'StartOnMonth':month})

				elif key == 'StartOnYear':
					AnnouncedOn = data.get('StartOn')
					if AnnouncedOn:
						year = str(AnnouncedOn.year)
						properties.update({'StartOnYear':year})

				elif key == 'EndOnMonth':
					AnnouncedOn = data.get('EndOn')
					if AnnouncedOn:
						month = str(AnnouncedOn.month)
						properties.update({'EndOnMonth':month})

				elif key == 'EndOnYear':
					AnnouncedOn = data.get('EndOn')
					if AnnouncedOn:
						year = str(AnnouncedOn.year)
						properties.update({'EndOnYear':year})

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

	def create_update_event_node(self, data):
		"""Process raw data from mysql and executes neo4j queries."""
	
		if data:
			for row in data:
				try:
					properties_data = self.create_data_dictionary(row)
					key, properties =  self.create_query_data(properties_data)
					if key and properties:
						query = self.neo4jQuery.format(key, properties)
						self.execute_neo4j_merge_query(query)
					else:
						pass

				except Exception as e:
					self.logg(debug_msg = 'Error while creating neo4j node query.', 
						info_msg = 'Function = create_update_event_node()',
						warning_msg = 'Error in creating dynamic node query.',
						error_msg = 'Module = '+log_file,
						critical_msg = str(e))
					continue

	def check_create_event_node(self, uuid):
		"""Check if the Event exists in neo4j.
		   If node does not exists creates a new node.
		"""

		query = 'MATCH (e:Event) where e.Uuid = "{}" RETURN e.Uuid'\
		.format(uuid)

		result = self.execute_neo4j_match_query(query)

		if result:
			items = [item for item in result]
			try:
				UUID = items[0]['e.Uuid']

			except IndexError as e:
				data = self.get_data(data_set='selected', uuid=uuid)
				if data:
					self.create_update_event_node(data)
					return True

			except Exception as e:
				pass

	def get_data(self, data_set, uuid=None):
		"""Fetch data from mysql w.r.t node."""

		if data_set == 'delta':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_delta_query, self.entity_type, match=self.date
				)
			return data

		elif data_set == 'selected':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_filter_query, self.entity_type, match=uuid
				)
			return data

		elif data_set == 'select_all':
			data = self.fetch_all_mysql_data(
				self.label, self.mysql_all_data_query, self.entity_type, match=None
				)
			return data
			
	@classmethod
	def run(cls):
		"""."""

		try:
			event = cls()
			data = event.get_data(data_set='select_all')
			if data:
				status = event.delete_all_event_node_and_relationships()
				if status:
					event.create_update_event_node(data)

		except Exception as e:
			return False

		else:
			return True


if __name__ == '__main__':
	"""."""

	Event.run()





