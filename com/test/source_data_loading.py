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
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    s3_bucket = app_conf['s3_conf']['s3_bucket']
    staging_loc = app_conf['s3_conf']['staging_loc']

    src_list = app_conf['src_list']
    for src in src_list:
        src_conf = app_conf[src]
        if src == 'SB':
            # read data from mysql, create data frame out of it and write it into aws s3 bucket
            jdbc_params = {"url": get_mysql_jdbc_url(app_secret),
                          "lowerBound": "1",
                          "upperBound": "100",
                          "dbtable": app_conf["mysql_conf"]["dbtable"],
                          "numPartitions": "2",
                          "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                          "user": app_secret["mysql_conf"]["username"],
                          "password": app_secret["mysql_conf"]["password"]
                           }

            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            txn_df = read_from_mysql(spark, jdbc_params)\
                .withColumn('ins_dt', current_date())

            txn_df.show()
            txn_df.write \
                .partitionBy('ins_dt') \
                .mode('append') \
                .parquet('s3a://' + s3_bucket + '/' + staging_loc + '/' + src)

        elif src == 'OL':
            ol_txn_df = read_from_sftp(spark, app_secret, app_conf, os, current_dir)\
                .withColumn('ins_dt', current_date())

            ol_txn_df.show(5, False)

            ol_txn_df.write \
                .partitionBy('ins_dt') \
                .mode('append') \
                .parquet('s3a://' + s3_bucket + '/' + staging_loc + '/' + src)

        elif src == 'ADDR' :
            students = read_from_mongodb(spark, app_conf) \
                .withColumn('ins_dt', current_date())

            students.show()

            students.write\
                .partitionBy('ins_dt')\
                .mode('append')\
                .parquet('s3a://' + s3_bucket + '/' + staging_loc + '/' + src)


# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1" dataframe/ingestion/others/systems/sftp_df.py
# read data from mongodb, create data frame out of it and write it into aws s3 bucket


# spark-submit --packages "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" dataframe/ingestion/others/systems/mongo_df.py
# read data from s3, create data frame out of it and write it into aws s3 bucket
