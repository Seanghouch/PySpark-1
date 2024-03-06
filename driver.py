import os
import sys

from time import perf_counter
import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date, print_schema, check_for_null
from ingest import load_files, display_df, df_count
from data_transformation import *
from data_processing import *
from extraction import *
from persist import *
import logging
import logging.config

logging.config.fileConfig('properties/config/logging.config')

start_time = perf_counter()

def main():
    global file_dir, header, inferSchema, file_format
    try:
        logging.info('i am the main method...')
        logging.info('calling spark object')
        spark = get_spark_object(gav.envn, gav.appName)

        logging.info('Validating spark object.......')
        get_current_date(spark)

        for file in os.listdir(gav.src_olap):
            print("File is " + file)
            file_dir = gav.src_olap + '\\' + file
            # print(file_dir)
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        logging.info(f'Reading file which is of > {file_format}')
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)
        logging.info(f'displaying the dataframe {df_city}')
        display_df(df_city, 'df')
        logging.info('validating the dataframe...')
        df_count(df_city, 'df')

        for file2 in os.listdir(gav.src_oltp):
            print("File is " + file2)
            file_dir = gav.src_oltp + '\\' + file2
            # print(file_dir)
            if file2.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file2.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)
        logging.info(f'displaying the dataframe {df_fact}')
        display_df(df_fact, 'df')
        logging.info('validating the dataframe...')
        df_count(df_fact, 'df')

        logging.info('implementing data processing...')
        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)
        display_df(df_city_sel, 'df_city')
        display_df(df_presc_sel, 'df_fact')

        logging.info('validating schema for dataframe....')
        print_schema(df_city_sel, 'df_city')
        print_schema(df_presc_sel, 'df_fact')
        display_df(df_presc_sel, 'df_face')

        logging.info('checking null values schema for dataframe....')
        # check_df = check_for_null(df_presc_sel, 'df_check_null')
        # display_df(check_df, 'check_df')

        logging.info('transformation data processing....')
        logging.info('displaying the df_report_1')
        df_report_1 = data_report1(df_city_sel, df_presc_sel)
        # display_df(df_report_1, 'data_report')

        logging.info('display data_report2 method...')
        df_report_2 = data_report2(df_presc_sel)
        display_df(df_report_2, 'df_report_2')

        logging.info('extracting files output....')
        city_path = gav.city_path
        # extract_files(df_report_1, 'csv', city_path, 1, False, 'snappy')
        # extract_files(df_report_1, 'orc', city_path, 1, False, 'snappy')
        presc_path = gav.presc_path
        extract_files(df_report_2, 'parquet', presc_path, 1, False, 'gzip')
        data = spark.read.option("compression", "gzip").parquet(presc_path)
        logging.info('show read data spark...')
        print(df_report_2.count())
        print(data.count())
        data.show()
        data.printSchema()

        logging.info('writing into have table')
        # data_hive_persist_persc(spark=spark, df=df_report_1, dfname='df_city', partitionBy='state_name', mode='append')
        logging.info("successfully written into Hive")

        logging.info(f"Now write {df_report_1} into MSSQL")
        persist_data_mssql(spark=spark, df=df_report_1, dfname='df_city', url=gav.mssql_url,
                           dbtable='city_df', mode='append', user=gav.user, password=gav.password)

        read_data_hive_presc(spark=spark, database='cities', dbtable='df_city')
        persist_read_data_mssql(spark=spark, url=gav.mssql_url, dbtable='city_df', user=gav.user, password=gav.password)
        spark.stop()
    except Exception as ex:
        logging.error("An error occurred when calling main(), please check the trance=== ", str(ex))
        sys.exit(1)

if __name__ == '__main__':
    main()
    end_time = perf_counter()
    logging.info(f'total amount of time taken: {end_time - start_time} seconds...')
    logging.info('Application done')