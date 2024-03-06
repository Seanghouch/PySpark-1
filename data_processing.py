import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('properties/config/logging.config')
logger = logging.getLogger('data_processing')

def data_clean(df1, df2):
    try:
        logger.warning('data_clean method() stating....')
        logger.warning('selecting required columns and converting som of columns into upper case...')
        df_city_sel = df1.select(upper(col('city')).alias('city'), df1.state_id,
                                 upper(df1.state_name).alias('state_name'),
                                 upper(df1.county_name).alias('county_name'),
                                 df1.population, df1.zips)
        logger.warning('working on OLAP dataset selecting couple of columns and rename...')

        df_presc_sel = df2.select(df2.npi.alias('presc_id'), df2.nppes_provider_last_org_name.alias('presc_lname'),
                                  df2.nppes_provider_first_name.alias('presc_fname'),
                                  df2.nppes_provider_city.alias('presc_city'),
                                  df2.nppes_provider_state.alias('presc_state'),
                                  df2.specialty_description.alias('precs_spclt'),
                                  df2.drug_name, df2.total_claim_count.alias('tx_cnt'), df2.total_day_supply,
                                  df2.total_drug_cost, df2.years_of_exp)
        logger.warning('Adding a new column to df_presc_sel')
        df_presc_sel = df_presc_sel.withColumn('country_name', lit('USA'))

        logger.warning('converting year_of_exp string to int and replacing = ')
        df_presc_sel = df_presc_sel.withColumn('years_of_exp', regexp_replace(col('years_of_exp'), r'^=', " "))
        df_presc_sel = df_presc_sel.withColumn('years_of_exp', col('years_of_exp').cast('int'))

        logger.warning('concat firstName and lastName')
        df_presc_sel = df_presc_sel.withColumn('presc_fullname', concat_ws(" ", 'presc_lname', 'presc_fname'))

        logger.warning('now check for null values in all column')
        # df_presc_sel = df_presc_sel.select([count(when (isnan(c) | col(c).isNull(), c)).alias(c) for c in df_presc_sel.columns])

        logger.warning('drop the null values in the respective column...')
        df_presc_sel = df_presc_sel.dropna(subset='presc_id')
        df_presc_sel = df_presc_sel.dropna(subset='drug_name')

        logger.warning('fill the null values in tx_cnt with the avg values...')
        mean_tx_cnt = df_presc_sel.select(mean(col('tx_cnt'))).collect()[0][0]
        df_presc_sel = df_presc_sel.fillna(mean_tx_cnt, 'tx_cnt')

        logger.warning('now check for null values cleaned are not...')
        # df_presc_sel = df_presc_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_presc_sel.columns])

        logger.warning('working on OLTP dataset selecting couple of columns and rename...')

    except Exception as ex:
        logger.error('An error occurred with data_processing====', str(ex))
        raise
    else:
        logger.warning('data_clean meth executed done, go forward...')
        return df_city_sel, df_presc_sel
