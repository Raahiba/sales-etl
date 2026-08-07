def load_function(dataframe, output_path):
    dataframe.write.format("csv")\
                   .mode("overwrite")\
                   .save(output_path)
    print(f"The file is succesfully saved at {output_path}")