import logging.config
logging.config.fileConfig('properties/config/logging.config')
logger = logging.getLogger('create_spark')
logger.info("Debug message")