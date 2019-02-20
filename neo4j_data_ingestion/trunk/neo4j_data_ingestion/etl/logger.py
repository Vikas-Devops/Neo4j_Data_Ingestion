"""Logger implementation. """

import os
import logging
import datetime
from parse_config import ParseConfig

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"
__updatedDate__ = "2018-07-19"

sep = os.sep
LOGGER_LEVEL = 'info'


class Logger:
	"""Log Errors."""

	LEVELS = {'debug': logging.DEBUG,'info': logging.INFO,'warning': logging.WARNING,
          'error': logging.ERROR,'critical': logging.CRITICAL}

	def __init__(self):
		"""Initialize and create log file path and set logger level."""

		self.configfile = sep.join(os.path.abspath(os.path.join(os.path.dirname(__file__))).split(sep)[:-1])+sep+"resources"+sep+"config.cfg"
		self.parse_dict = ParseConfig.get_config_dict(self.configfile)
		self.folderPath = self.parse_dict.get('filePath').get('logger_path')
		self.user_dir 	= sep.join(os.path.abspath(os.path.join(os.path.dirname(__file__))).split(sep)[:3])
		self.base_dir 	= '{}/{}/logs/'.format(self.user_dir, self.folderPath)
		self.level 		= self.LEVELS.get(LOGGER_LEVEL)

		try:
			if not os.path.isdir(self.base_dir):
				os.makedirs(self.base_dir)
		except Exception as e:
			pass

	def logg(self, **kwargs):
		""" Logs error in error log with respect to filename and error.
			Arguments passed: debug_msg, info_msg, warning_msg, error_msg, critical_msg"""

		try:
			current_date = datetime.datetime.strftime(datetime.datetime.now().date(), '%Y-%m-%d')
			logging.basicConfig(filename=self.base_dir+current_date+'.log', 
				format='%(asctime)s |%(name)s:%(levelname)s:%(message)s',
				datefmt='%H:%M:%S', level=self.level)
			
			logging.debug(kwargs.pop('debug_msg',''))
			logging.info(kwargs.pop('info_msg',''))
			logging.warning(kwargs.pop('warning_msg',''))
			logging.error(kwargs.pop('error_msg','Unknown Module'))
			logging.critical(kwargs.pop('critical_msg','')+\
				'\n------------------------------------------------------------------')

		except Exception as e:
			print e
			pass


if __name__ == "__main__":
	"""module testing."""
	logger = Logger()
	logger.logg(debug_msg = 'description about the case.', 
		info_msg = 'function in which the error occured.', 
		warning_msg ='warning message.',
		error_msg ='module name.',
		critical_msg ='error caught.')
