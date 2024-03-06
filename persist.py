import logging.config

from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import datetime as date

logging.config.fileConfig('properties/config/logging.config')
logger = logging.getLogger('Persist')

def data_hive_persist_persc(spark, df, dfname, partitionBy, mode):
    try:
        logger.warning(f'persisting the data into Hive table for {dfname}')
        logger.warning('lets create database...')
        spark.sql("CREATE DATABASE IF NOT EXISTS cities")
        spark.sql("USE cities")

        logger.warning(f'No writing {df} into hive tables by {partitionBy}')
        df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)

    except Exception as e:
        logger.error('An occurred while processing data_hive_persist:::', str(e))
        raise
    else:
        logger.warning('Data successfully persisted into hive tables...')

def read_data_hive_presc(spark, database, dbtable):
    try:
        # table_path = f"hive://{database}/{dbtable}"
        # df = spark.read.format("hive").option("partitionBy", partitionBy).load(table_path)
        spark.sql(f"USE {database}")
        # df = spark.read.table('df_city')
        df = spark.sql(f"SELECT * FROM {dbtable}")
        # df = spark.read.format('hive').option("database", 'cities').option("table", 'df_city').load()
    except Exception as e:
        logger.error('An occurred while processing data_hive_persist:::', str(e))
        raise
    else:
        logger.warning('Data successfully persisted into hive tables...')
    return df.show()

def persist_data_mssql(spark, df, dfname, url,  dbtable, mode, user, password):
    try:
        logger.warning(f'executing the data_persist_mssql method...{dfname}')

        df.write.format("jdbc").option("url", url).option("dbtable", dbtable).mode(mode)\
            .option("user", user).option("password", password).option("trustServerCertificate", True).save()

    except Exception as e:
        logger.error("An error occurred @ persist_data_mysql method::::", str(e))

        raise

    else:
        logger.warning(f'Persist_data_mysql method executed successfully...into {dbtable}')

def persist_read_data_mssql(spark, url, dbtable, user, password):
    try:
        logger.warning(f'executing the data_persist_mssql method...{dbtable}')

        df = spark.read.format("jdbc").option("url", url).option("dbtable", dbtable) \
            .option("user", user).option("password", password).option("trustServerCertificate", True).load()

    except Exception as e:
        logger.error("An error occurred @ persist_data_mssql method::::", str(e))

        raise

    else:
        logger.warning(f'Persist_data_mysql method executed successfully...into {dbtable}')
    return df.show()