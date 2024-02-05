# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data using access keys 
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container 
# MAGIC 1. Read data from circuits.csv file 

# COMMAND ----------

##dbutils.widgets.text("p_data_source", "")
##v_data_source = dbutils.widgets.get ("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dl0001.dfs.core.windows.net","NA3SU2tac6L3bjOhCWacQUUy3DrCsR/YyE+Jpgmcp9CIEH19ZRkwlk9h+S9V83U/2Zc+sigf9i2p+AStfaJdoQ==")
#raw_folder_path = 'abfss://demo@formula1dl0001.dfs.core.windows.net'
raw_folder_path = 'abfss://raw@formula1dl0001.dfs.core.windows.net'
processed_folder_path = 'abfss://processed@formula1dl0001.dfs.core.windows.net'
presentation_folder_path = 'abfss://presentation@formula1dl0001.dfs.core.windows.net'
demo_folder_path = 'abfss://deltademo@formula1dl0001.dfs.core.windows.net'

# COMMAND ----------

circu_df = spark.read.csv("abfss://demo@formula1dl0001.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display (circu_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/javcircuits.csv")
    ##.csv("abfss://demo@formula1dl0001.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.columns

# COMMAND ----------

circuits_schema.fields

# COMMAND ----------

circuits_df.describe()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

circuits_df.head(2) [0]

# COMMAND ----------

circuits_df.select('circuitId').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### select only the required columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col ("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### rename columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                          .withColumnRenamed("circuitRef", "circuit_Ref") \
                                          .withColumnRenamed("lat", "latitude") \
                                          .withColumnRenamed("lng", "longitude") \
                                          .withColumnRenamed("alt", "altitude") 
                                          #\
                                           #.withColumn("data_source", lit(v_data_source))
                                                       


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add ingestion date to the Dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df1 = circuits_final_df.withColumn("env", lit("test"))

# COMMAND ----------

display(circuits_final_df1)

# COMMAND ----------

circuits_final_df1.createOrReplaceTempView('circuitos')

# COMMAND ----------

results = spark.sql("select country, sum(altitude) as altitude from circuitos group by country")

# COMMAND ----------

display (results)

# COMMAND ----------

circuits_final_df1.write.mode("overwrite").parquet(f"{processed_folder_path}/circuitse_s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data to a datalake as parquet o la siguiente linea a la delta lake depende que quieras

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet("abfss://demo@formula1dl0001.dfs.core.windows.net/circuits")
#la siguiente linea lo lleva a un parquet file al container de processed
circuits_final_df1.write.mode("overwrite").parquet(f"{processed_folder_path}/circuitsf_p")
#la siguiente linea lo lleva a una spark table
#circuits_final_df1.write.mode("overwrite").format ("parquet").saveAsTable("f1_processed.circuits")
#(f"{processed_folder_path}/circuits")


# COMMAND ----------

# MAGIC %md
# MAGIC #leo el parquet de nuevo

# COMMAND ----------

circuits_f_p = spark.read.parquet("abfss://processed@formula1dl0001.dfs.core.windows.net/circuitsf_p")

# COMMAND ----------

display (circuits_f_p)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Write to a delta lake 
# MAGIC ##aqui

# COMMAND ----------

##circuits_final_df1.write.format("delta").mode("overwrite").saveAsTable("f1_demo.circuits")

# COMMAND ----------

circuits_final_df1.write.format("delta").mode("overwrite").saveAsTable("f1_bidemojav.vega_circuits_delta_1")

# COMMAND ----------

circuits_final_df1.write.format("delta").mode("overwrite").saveAsTable("f1_bidemojav.circuits_delta_was_pf")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_processed.circuits

# COMMAND ----------

#la siguiente linea es para corroborar que estas leyendo del parquet file

# COMMAND ----------

##df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

##display(df)

# COMMAND ----------

dbutils.notebook.exit("Success_Vega")
