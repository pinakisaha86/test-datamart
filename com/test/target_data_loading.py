from pyspark.sql import *
from pyspark.sql.functions import *
import yaml
import os.path
from com.utility.utils import *


if __name__ == '__main__':

# Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    s3_bucket = app_conf['s3_conf']['s3_bucket']
    staging_loc = app_conf['s3_conf']['staging_loc']

    tgt_list = app_conf['tgt_list']
    for tgt in tgt_list:
        tgt_conf = app_conf[tgt]
        if tgt == 'REGIS_DIM':
            regis_dim_df = spark.read.parquet('s3a://' + s3_bucket + '/' + staging_loc + '/' + tgt_conf['source_data'])

            regis_dim_df.createOrReplaceTempView("CP")
            result = spark.sql(tgt_conf['loadingQuery'])

           # result.repartition(1).write.option("header", "true").mode("overwrite").parquet("")
            jdbc_url = get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            result.coalesce(1).write \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", "csvdb1.REGIS_DIM") \
                .mode("overwrite") \
                .save()
# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"
# --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/test/target_data_loading.py