# Write the transformed dataframe in the parquet file

def load_function(dataframe, output_path):
    dataframe.write.format("parquet")\
                   .mode("overwrite")\
                   .option("header", True)\
                   .save(output_path)
    print(f"The file is succesfully saved at {output_path}")