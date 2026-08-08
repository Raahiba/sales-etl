from pyspark.sql.functions import *
from pyspark.sql.types import *

# Import functions for data extraction, transformation, and loading

from src.extract import extract_data
from src.transform import data_cleaning, data_cast, feature_engg, payment_cat
from src.load import load_function


# Import input path from configuration file
from config.config import (
    ORDER_PATH,
    ORDER_PAYMENT_PATH,
    ORDER_REVIEW_PATH
)

# Import output path from configuration file
from config.config import(

    ORDER_OUTPUT_PATH,
    REVIEW_OUTPUT_PATH,
    PAYMENT_OUTPUT_PATH
)


# ETL Pipeline
# Extract, transform, and load the order reviews dataset

reviews_df = extract_data(ORDER_REVIEW_PATH)

reviews_df= data_cleaning(reviews_df)
reviews_df = data_cast(reviews_df)

load_function(reviews_df, REVIEW_OUTPUT_PATH)


# Extract, transform, and load the payments dataset

payments_df = extract_data(ORDER_PAYMENT_PATH)

payments_df = data_cleaning(payments_df)
payments_df = payment_cat(payments_df)

load_function(payments_df, PAYMENT_OUTPUT_PATH)

# Extract, transform, and load the orders dataset

orders_df = extract_data(ORDER_PATH)

orders_df = data_cleaning(orders_df)
orders_df = feature_engg(orders_df)

load_function(orders_df, ORDER_OUTPUT_PATH)


