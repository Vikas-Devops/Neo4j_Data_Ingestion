
import ConfigParser

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"
__updatedDate__ = "2018-07-19"

log_file = 'parse_config.py'

class ParseConfig():
	""" Configuration file reading class."""

	def __init__(self, filename):
		""" Initializes ConfigParser and avoids lower case conversions."""

		self.filename = filename
		self.config = ConfigParser.RawConfigParser()
		#avoids lowercase conversions of keys by default
		self.config.optionxform = lambda option: option

	def parse(self):
		""" Reads config file and returns configurations as dict of dict."""

		configDict = {}
		try:
			self.config.readfp(open(self.filename,'r'))
			sections = self.config.sections()
			for section in sections:
				configDict.setdefault(section,{})
				for option in self.config.options(section):
					configDict[section].setdefault(option,self.config.get(section,option))
		except Exception as e:
			pass
		else:	
			return configDict

	@classmethod
	def get_config_dict(cls, filename):
		""" Create a class instance and returns a config dict from parse method."""
		
		parse_config = cls(filename)
		return parse_config.parse()


if __name__ == '__main__':
	"""module testing"""
	parseConfig = ParseConfig('filename')
	parse_dict = parseConfig.parse()
			

