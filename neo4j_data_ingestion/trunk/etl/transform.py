
from collections import OrderedDict

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"
__updatedDate__ = "2018-07-19"

class Transform:

	def __init__(self):
		"""."""
		pass

	@staticmethod
	def to_unicode(unicode_or_str):
		"""."""

		if isinstance(unicode_or_str, str):
			value = unicode_or_str.decode('utf-8')

		else:
			value = unicode_or_str

		return value

	@staticmethod
	def to_str(unicode_or_str):
		"""."""

		if isinstance(unicode_or_str, unicode):
			value = unicode_or_str.encode('utf-8')
			
		else:
			value = unicode_or_str 

		return value

	@staticmethod
	def to_integer(value):
		"""."""

		value = 'toInt("{}")'.format(value)
		return value

	@staticmethod
	def to_float(value):
		"""."""

		if not value:
			value = '0'
		value = 'toFloat("{}")'.format(value)
		return value

	@staticmethod
	def map_data(neo4j_mapping, data):
		"""."""

		mapped_data = OrderedDict()

		for key, neo4j_property in neo4j_mapping.iteritems():
			value = data.get(key)
			mapped_data.setdefault(neo4j_property, value)

		return mapped_data

	@staticmethod
	def get_neo4j_label(role):
		"""."""

		role_mapping = {
						'company':'Company',
						'group':'Group',
						'investor':'Investor',
						'school':'AcademicInstitute'
						}

		label = role_mapping.get(role)

		return label

		
		
		


