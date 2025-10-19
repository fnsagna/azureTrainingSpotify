# Databricks notebook source
#df = spark.read.format("parquet").load("abfss://bronze@staccountspotify.dfs.core.windows.net/dimUser")
#display(df)

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

import os 
import sys

project_pth = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_pth)

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ## **AUTOLOADER**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **DimUser**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@staccountspotify.dfs.core.windows.net/DimUser/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@staccountspotify.dfs.core.windows.net/dimUser")

# COMMAND ----------

display(df_user)

# COMMAND ----------

df_user = df_user.withColumn("user_name",upper(col("user_name")))
display(df_user)

# COMMAND ----------

df_user_obj = reusable()
df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])
display(df_user)

# COMMAND ----------

df_user.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@staccountspotify.dfs.core.windows.net/DimUser/chekpoint")\
            .trigger(once=True)\
            .option("path","abfss://silver@staccountspotify.dfs.core.windows.net/DimUser/data")\
            .toTable("spotify_catalog.silver.DimUser")

# COMMAND ----------

#df_user.writeStream.format("delta")\
#            .outputMode("append")\
#            .option("checkpointLocation","abfss://silver@staccountspotify.dfs.core.windows.net/DimUser/chekpoint")\
#            .trigger(once=True)\
#            .option("path","abfss://silver@staccountspotify.dfs.core.windows.net/DimUser/data")\
#            .start("abfss://silver@staccountspotify.dfs.core.windows.net/DimUser/data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimArtist**

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@staccountspotify.dfs.core.windows.net/DimArt/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@staccountspotify.dfs.core.windows.net/dimArtist")

# COMMAND ----------

display(df_art)

# COMMAND ----------

df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art,['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])
display(df_art)   

# COMMAND ----------

df_art.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@staccountspotify.dfs.core.windows.net/DimArt/chekpoint")\
            .trigger(once=True)\
            .option("path","abfss://silver@staccountspotify.dfs.core.windows.net/DimArt/data")\
            .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@staccountspotify.dfs.core.windows.net/DimTrack/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@staccountspotify.dfs.core.windows.net/dimTrack")

# COMMAND ----------

display(df_track)

# COMMAND ----------

df_track = df_track.withColumn("durationFlag",when(col('duration_sec')<150,"low")\
                                            .when(col('duration_sec')<300,"medium")\
                                            .otherwise("high"))

df_track = df_track.withColumn("track_name",regexp_replace(col('track_name'),'-',' '))

df_track = reusable().dropColumns(df_track,['_rescued_data'])

display(df_track)

# COMMAND ----------

df_track.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@staccountspotify.dfs.core.windows.net/DimTrack/chekpoint")\
            .trigger(once=True)\
            .option("path","abfss://silver@staccountspotify.dfs.core.windows.net/DimTrack/data")\
            .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@staccountspotify.dfs.core.windows.net/dimDate/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@staccountspotify.dfs.core.windows.net/dimDate")

# COMMAND ----------

df_date = reusable().dropColumns(df_date,['_rescued_data'])

df_date.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@staccountspotify.dfs.core.windows.net/dimDate/chekpoint")\
            .trigger(once=True)\
            .option("path","abfss://silver@staccountspotify.dfs.core.windows.net/dimDate/data")\
            .toTable("spotify_catalog.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **FactStream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@staccountspotify.dfs.core.windows.net/FactStream/chekpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@staccountspotify.dfs.core.windows.net/factStream")

# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,['_rescued_data'])

df_fact.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@staccountspotify.dfs.core.windows.net/FactStream/chekpoint")\
            .trigger(once=True)\
            .option("path","abfss://silver@staccountspotify.dfs.core.windows.net/FactStream/data")\
            .toTable("spotify_catalog.silver.FactStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_catalog.gold.dimtrack 
# MAGIC where `__END_AT` is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_catalog.gold.dimtrack 
# MAGIC where track_id in (46,5)