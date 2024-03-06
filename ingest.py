import logging.config

logging.config.fileConfig('properties/config/logging.config')
logger = logging.getLogger('infest')

def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('Load_file method started...')

        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.csv(file_dir, header=True)

    except Exception as ex:
        logger.error("An error occurred at load_file=== ", str(ex))
        raise
    else:
        logger.warning(f'dataframe created  which is if {file_format}')

    return df

def display_df(df, dfName):
    df_show = df.show()
    return df_show

def df_count(df, dfName):
    try:
        logger.warning(f'here to count the records in the {dfName}')
        df_c = df.count()
    except Exception as ex:
        logger.error("An error occurred df_count===", str(ex))
        raise
    else:
        logger.warning(f'Number of records present in the {df} are :: {df_c}')
    return df_c