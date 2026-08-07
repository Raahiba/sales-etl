from src.extract import extract_data
from src.transform import data_cleaning, data_cast, feature_engg, payment_cat
from src.load import load_function
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import (
    ORDER_PATH,
    ORDER_PAYMENT_PATH,
    ORDER_REVIEW_PATH
)

from config.config import(

    ORDER_OUTPUT_PATH,
    REVIEW_OUTPUT_PATH,
    PAYMENT_OUTPUT_PATH
)


# Order review transformation and load

reviews_df = extract_data(ORDER_REVIEW_PATH)
reviews_df= data_cleaning(reviews_df)
reviews_df = data_cast(reviews_df)

load_function(reviews_df, REVIEW_OUTPUT_PATH)


# Payments transformation and load

payments_df = extract_data(ORDER_PAYMENT_PATH)
payments_df = data_cleaning(payments_df)
payments_df = payment_cat(payments_df)

load_function(payments_df, PAYMENT_OUTPUT_PATH)

# Orders transformation and load

orders_df = extract_data(ORDER_PATH)
orders_df = data_cleaning(orders_df)
orders_df = feature_engg(orders_df)

load_function(orders_df, ORDER_OUTPUT_PATH)



reviews_df = data_cast(reviews_df)
reviews_df.printSchema()
reviews_df.show(5, truncate=False)

