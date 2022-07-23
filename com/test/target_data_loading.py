from pyspark.sql import *
from pyspark.sql.functions import *
import yaml
import os.path
from com.utility.utils import *


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )
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
            spark.sql(app_conf[tgt_list]).show(5, False)

# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1,mysql:mysql-connector-java:8.0.15" com/test/target_data_loading.py