import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, sum, count


# Create a SparkSession
spark = SparkSession.builder.getOrCreate()


# As the file was to large I unziped it and saved it in my local machine
file_path = "../../arcos_all_washpost.tsv"

# Read the data
opioid_df = spark.read.csv(file_path, sep="\t", header=True)

# Define the Parquet file path
# parquet_file_path = (
#    "../00_source_data/arcos_all_washpost.parquet"  # Change to your Parquet file path
# )

# Save as a Parquet file
# df.write.mode("ingnore").parquet(parquet_file_path)

# Creating a new date variable for grouping
opioid_df = opioid_df.withColumn(
    "year_month", date_format("TRANSACTION_DATE", "yyyy-MM")
)

# Groupby the data
opioid_df = opioid_df.groupBy(
    "BUYER_STATE", "BUYER_COUNTY", "DRUG_NAME", "year_month"
).agg(
    sum("MME").alias("total_morphine_mg"),
    count("REPORTER_DEA_NO").alias("total_transactions"),
)

# Converting to pandas
dataframe_to_print = opioid_df.toPandas()

# Saving df_pf to parquet
dataframe_to_print.to_parquet(
    "../20_intermediate_files/arcos_all_washpost_collapsed.parquet"
)
