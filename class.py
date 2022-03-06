# Databricks notebook source
print("Hello")

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/nba.csv/nba.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)


# COMMAND ----------

df.describe(["_c4"]).show()