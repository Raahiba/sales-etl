from pyspark.sql.functions import *
from pyspark.sql.types import *

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


# data cleaning 


def data_cleaning(dataframe):

    # Duplicates 
    print("Removing Duplicates")
    dataframe = dataframe.drop_duplicates()
    print("Duplicates removed")

    return dataframe

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

def data_cast(dataFrame):
   dataFrame =  dataFrame.withColumn("review_creation_date", col("review_creation_date").cast("timestamp"))\
                         .withColumn("review_answer_timestamp", col("review_answer_timestamp").cast("timestamp"))
   return dataFrame


def feature_engg(dataframe):
    dataframe = dataframe.withColumn(
                                              "Order_Processing_Time",
                                               date_diff(
                                                   "order_delivered_customer_date", 
                                                   "order_purchase_timestamp"
                                                   )
                                                )

# number of days earlier the order arrived
    dataframe = dataframe.withColumn(
                                    "estimated_days_diffrence", \
                                            date_diff(
                                            "order_estimated_delivery_date", 
                                            "order_delivered_customer_date"
                                            )
                                     )

    return dataframe

def payment_cat(dataframe):
    dataframe = dataframe.withColumn("payment_category",
                                      when(col("payment_value") <= 3000, "low")
                                     .when( (col("payment_value") > 3000) & (col("payment_value") < 9000), "medium")
                                     .otherwise("high"))
    return dataframe