import logging.config
from pyspark.sql.functions import *

logging.config.fileConfig('properties/config/logging.config')
logger = logging.getLogger('validate')

def get_current_date(spark):
    try:
        logger.warning('started the get_current_date method...')
        output = spark.sql(""" select current_date """)
        print('validating spark object with current date -' + str(output.collect()))

    except Exception as ex:
        logger.error('An error occurred in get_current_date', str(ex))
        raise

    else:
        logger.warning('Validation done, go forward...')

def print_schema(df, dfName):
    try:
        logger.warning(f'printing schema method starting... {dfName}')
        sch = df.schema.fields
        for i in sch:
            logger.info(f'\t{i}')
    except Exception as e:
        logger.error('An error occurred at print schema :::', str(e))
        raise
    else:
        logger.info('print schema done, go forward....')

def check_for_null(df, dfName):
    try:
        logger.info(f'check for null method executing.... for {dfName}')
        check_null_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])

    except Exception as e:
        logger.error('An error occurred while working on check_for_null=== ', str(e))
        raise
    else:
        logger.warning('Check_for_null executed successfully....')

    return check_null_df