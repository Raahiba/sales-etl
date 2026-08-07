from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .master('local[*]')\
                    .appName("SALES-ETL")\
                    .getOrCreate()


def extract_data (data):

    df = (
            spark.read.format("csv")\
                     .option("header", True)\
                     .option("inferSchema", True)\
                     .option("multiline", True)\
                     .option("escape", "\"")\
                     .option("quote", "\"")\
                     .load(data)
    )
    return df


