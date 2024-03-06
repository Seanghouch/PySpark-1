from os.path import abspath

from pyspark.sql import SparkSession
import logging.config
import os

logging.config.fileConfig('properties/config/logging.config')
logger = logging.getLogger('create_spark')

def get_spark_object(envn, appName):

    try:
        logger.info('get_spark_object method started')
        if envn == 'DEV':
            master = 'local'
        else:
            master = 'Yarn'

        logger.info(f'master is {master}')
        current = os.getcwd()
        warehouse_location = abspath('spark-warehouse')

        spark = SparkSession.builder.master(master).appName(appName) \
            .config("spark.sql.warehouse.dir", warehouse_location) \
            .enableHiveSupport() \
            .config('spark.driver.extraClassPath', current + '/driver/jar/mssql-jdbc-12.4.2.jre8.jar').getOrCreate()

    except Exception as ex:
        logger.error("An error occurred in the get_spark_object...", str(ex))
        raise

    else:
        logger.info('Spark object create....')
    return spark