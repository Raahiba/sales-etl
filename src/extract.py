
# Create and initialize a Spark session

from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .master('local[*]')\
                    .appName("SALES-ETL")\
                    .getOrCreate()


# Extract raw CSV data into a Spark DataFrame

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


