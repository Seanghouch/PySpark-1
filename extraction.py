import logging.config

from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('properties/config/logging.config')
logger = logging.getLogger('extraction')

def extract_files(df, format, filepath, split_no, headerReq, compressionType):
    try:
        logger.warning('extracting_file method starting....')
        # df.coalesce(split_no).write.mode("overwrite").format(format).save(filepath, header=headerReq, compression=compressionType)
        df.coalesce(split_no).write \
            .mode("overwrite") \
            .format(format) \
            .option("header", headerReq) \
            .option("compression", compressionType) \
            .save(filepath)
    except Exception as e:
        logger.error('An error occurred at extract_file method:::', str(e))
        raise
    else:
        logger.warning('extract_file method successfully...')