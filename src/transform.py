from pyspark.sql.functions import *
from pyspark.sql.types import *

# Import raw data paths from the configuration file

from config.config import (
    ORDER_ITEM,
    ORDER_PATH,
    ORDER_PAYMENT_PATH,
    ORDER_REVIEW_PATH,
    PRODUCTS_CATEGORY_PATH,
    PRODUCT_PATH,
    SELLER_PATH,
    CUSTOMER_PATH,
    LOCATION_PATH
)


# Remove duplicate records from the DataFrame

def data_cleaning(dataframe):

    print("Removing Duplicates")
    dataframe = dataframe.drop_duplicates()
    print("Duplicates removed")

    return dataframe

# Count null values in each column

# def null_count(dataframe):
#      # Count nulls
#     print("Counting null values......")
#     null_count = dataframe.select(
#     [
#     count(when(col(i).isNull(),i)).alias(i)
#     for i in dataframe.columns
#     ]
#     )
#     return null_count
#     print("success")


# Convert review date columns from String to Timestamp

def data_cast(dataFrame):
   dataFrame =  dataFrame.withColumn("review_creation_date", col("review_creation_date").cast("timestamp"))\
                         .withColumn("review_answer_timestamp", col("review_answer_timestamp").cast("timestamp"))
   return dataFrame


# Create new columns to calculate order processing time
# and the number of days before the estimated delivery date
# that the order was actually delivered

def feature_engg(dataframe):
    dataframe = dataframe.withColumn(
                                              "Order_Processing_Time",
                                               date_diff(
                                                   "order_delivered_customer_date", 
                                                   "order_purchase_timestamp"
                                                   )
                                                )

    dataframe = dataframe.withColumn(
                                    "estimated_days_difference", \
                                            date_diff(
                                            "order_estimated_delivery_date", 
                                            "order_delivered_customer_date"
                                            )
                                     )

    return dataframe


# Categorize payment values into low, medium, and high-value categories

def payment_cat(dataframe):
    dataframe = dataframe.withColumn("payment_category",
                                      when(col("payment_value") <= 3000, "low")
                                     .when( (col("payment_value") > 3000) & (col("payment_value") < 9000), "medium")
                                     .otherwise("high"))
    return dataframe