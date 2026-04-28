# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3463b5ec-ddb9-4967-abc8-c512826faf68",
# META       "default_lakehouse_name": "lh_earthquake",
# META       "default_lakehouse_workspace_id": "bb1444cd-93a8-4c08-a21a-87ee9a1ca8ad",
# META       "known_lakehouses": [
# META         {
# META           "id": "3463b5ec-ddb9-4967-abc8-c512826faf68"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cc19c67d-30ac-9309-421f-1d703391b911",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col,when,udf,col,current_timestamp
from pyspark.sql.types import StringType
from delta.tables import DeltaTable


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import reverse_geocoder as rgc
def get_country_code(lat,lon):
    """
    Returns ISO country code for given latitude and longitude.
    """
    result = rgc.search([(lat,lon)], mode =1 )
    return result[0]['cc']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = "1999-08-17"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_eq = spark.read.table("silver_earthquake").filter(col("event_date")>= start_date)
# df_eq = spark.read.table("silver_earthquake").filter(col("event_date") == "1990-06-21")
#display(df_eq)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### --------------------
# #### **Data Enrichment**
# #### --------------------

# CELL ********************

# This step creates the Gold analytical dataset from the Silver table.
# The data is enriched with additional analytical attributes such as:
#   - country_code (derived from latitude and longitude using reverse geocoding)
#   - sig_category (significance classification)
#   - mag_category (magnitude classification)
#   - depth_category (earthquake depth classification)
#   - hemisphere (derived from latitude)

get_country_code_udf = udf(get_country_code, StringType())
df_eq = df_eq \
                    .drop("ingested_at")\
                    .withColumn("country_code" , get_country_code_udf(col("latitude"),col("longitude"))) \
                    .withColumn("hemisphere" ,
                                    when(col("latitude") >=0 , "Northern")
                                    .otherwise("Southern")
                                ) \
                    .withColumn("ingested_at" , current_timestamp())    

df_eq = df_eq.select(
                    "event_id",
                    "event_date_time",
                    "event_date" ,
                    "event_time" ,
                    "country_code",
                    "longitude",
                    "latitude",
                    "depth",
                    "mag",
                    "mag_type",
                    "sig",
                    "is_tsunami",
                    "hemisphere" ,
                    "status",
                    "location",
                    "url", 
                    "updated_at",
                    "ingested_at" ,
                    "last_updated_at"                                                                                                                       

)  

df_eq.createOrReplaceTempView("vw_eq")

#display(df_eq_enriched)             

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%pyspark
df_country = spark.read.synapsesql("wh_earthquake.dbo.dim_country")
df_mag     = spark.read.synapsesql("wh_earthquake.dbo.dim_mag_category")
df_sig     = spark.read.synapsesql("wh_earthquake.dbo.dim_sig_category")
df_depth   = spark.read.synapsesql("wh_earthquake.dbo.dim_depth_category")

df_country.createOrReplaceTempView("dim_country")
df_mag.createOrReplaceTempView("dim_mag_category")
df_sig.createOrReplaceTempView("dim_sig_category")
df_depth.createOrReplaceTempView("dim_depth_category")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_eq_enriched AS 
# MAGIC SELECT e.* , m.mag_category_key , s.sig_category_key ,  d.depth_category_key 
# MAGIC FROM vw_eq e
# MAGIC     INNER JOIN wh_earthquake.dbo.dim_country        c ON e.country_code = c.country_code
# MAGIC     INNER JOIN wh_earthquake.dbo.dim_mag_category   m ON  e.mag   >  m.min_value AND ( e.mag   <= m.max_value OR m.max_value IS NULL)
# MAGIC     INNER JOIN wh_earthquake.dbo.dim_sig_category   s ON  e.sig   >  s.min_value AND ( e.sig   <= s.max_value OR s.max_value IS NULL)
# MAGIC     INNER JOIN wh_earthquake.dbo.dim_depth_category d ON  e.depth >  d.min_value AND ( e.depth <= d.max_value OR d.max_value IS NULL )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_eq_enriched AS 
# MAGIC SELECT e.* , m.mag_category_key , s.sig_category_key ,  d.depth_category_key 
# MAGIC FROM vw_eq e
# MAGIC     INNER JOIN `wh_earthquake`.`dbo`.`dim_country`        c ON e.country_code = c.country_code
# MAGIC     INNER JOIN `wh_earthquake`.`dbo`.`dim_mag_category`   m ON  e.mag   >  m.min_value AND ( e.mag   <= m.max_value OR m.max_value IS NULL)
# MAGIC     INNER JOIN `wh_earthquake`.`dbo`.`dim_sig_category`   s ON  e.sig   >  s.min_value AND ( e.sig   <= s.max_value OR s.max_value IS NULL)
# MAGIC     INNER JOIN `wh_earthquake`.`dbo`.`dim_depth_category` d ON  e.depth >  d.min_value AND ( e.depth <= d.max_value OR d.max_value IS NULL )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM vw_eq_enriched

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ------------------------------------------------------
# #### **Load the data in Gole Layer (dim and fact tables)**
# #### ------------------------------------------------------

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO wh_earthquake.dbo.fact_earthquake AS target
# MAGIC USING vw_fact_ready AS source
# MAGIC ON target.event_id = source.event_id
# MAGIC 
# MAGIC WHEN MATCHED AND source.updated_at > target.updated_at THEN
# MAGIC     UPDATE SET
# MAGIC         target.date_key = source.date_key,
# MAGIC         target.country_key = source.country_key,
# MAGIC         target.mag_category_key = source.mag_category_key,
# MAGIC         target.sig_category_key = source.sig_category_key,
# MAGIC         target.depth_category_key = source.depth_category_key,
# MAGIC         target.event_date_time = source.event_date_time,
# MAGIC         target.magnitude = source.magnitude,
# MAGIC         target.significance_score = source.significance_score,
# MAGIC         target.depth = source.depth,
# MAGIC         target.is_tsunami = source.is_tsunami,
# MAGIC         target.updated_at = source.updated_at,
# MAGIC         target.ingested_at = source.ingested_at
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         event_id,
# MAGIC         date_key,
# MAGIC         country_key,
# MAGIC         mag_category_key,
# MAGIC         sig_category_key,
# MAGIC         depth_category_key,
# MAGIC         event_date_time,
# MAGIC         magnitude,
# MAGIC         significance_score,
# MAGIC         depth,
# MAGIC         is_tsunami,
# MAGIC         updated_at,
# MAGIC         ingested_at
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.event_id,
# MAGIC         source.date_key,
# MAGIC         source.country_key,
# MAGIC         source.mag_category_key,
# MAGIC         source.sig_category_key,
# MAGIC         source.depth_category_key,
# MAGIC         source.event_date_time,
# MAGIC         source.magnitude,
# MAGIC         source.significance_score,
# MAGIC         source.depth,
# MAGIC         source.is_tsunami,
# MAGIC         source.updated_at,
# MAGIC         source.ingested_at
# MAGIC     )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# The Gold table stores the curated earthquake events used for analytics
# and as the source for building dimensional models in the Warehouse.
#
# Load Strategy:
#   - Upsert (MERGE) based on event_id
#   - New records are inserted
#   - Existing records are updated only when source.updated_at is newer
#
# This ensures the Gold dataset remains consistent and avoids duplicates
# when the pipeline is executed multiple times.
#
# Target Table:
#   gold_earthquake_enriched

target_table = "wh_eaarthquake.dbo.fact_earthquake"

if not spark.catalog.tableExists(target_table):
    df_eq_enriched.write.format("delta").mode("overwrite").saveAsTable(target_table)
else :
    gold_table = DeltaTable.forName(spark, target_table)

    (
        gold_table.alias("target")
        .merge(
            df_eq_enriched.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenMatchedUpdate(
            condition = "source.updated_at > target.updated_at" ,
            set = {
                "event_id": "source.event_id",
                "event_date_time": "source.event_date_time",
                "event_date":"source.event_date",
                "event_time":"source.event_time",
                "status":"source.status",
                "latitude": "source.latitude",
                "longitude": "source.longitude",
                "depth": "source.depth",
                "mag": "source.mag",
                "mag_type":"source.mag_type",
                "sig": "source.sig",
                "is_tsunami":"source.is_tsunami",
                "country_code": "source.country_code",
                "sig_category": "source.sig_category",
                "depth_category": "source.depth_category",
                "hemisphere": "source.hemisphere",
                "url":"source.url",
                "updated_at": "source.updated_at",
                "ingested_at": "source.ingested_at" ,
                "last_updated_at" : current_timestamp()               
            }
        )
        .whenNotMatchedInsert(
            values = {
                "event_id": "source.event_id",
                "event_date_time": "source.event_date_time",
                "event_date":"source.event_date",
                "event_time":"source.event_time",
                "status":"source.status",
                "latitude": "source.latitude",
                "longitude": "source.longitude",
                "depth": "source.depth",
                "mag": "source.mag",
                "mag_type":"source.mag_type",
                "sig": "source.sig",
                "is_tsunami":"source.is_tsunami",
                "country_code": "source.country_code",
                "sig_category": "source.sig_category",
                "depth_category": "source.depth_category",
                "hemisphere": "source.hemisphere",
                "url":"source.url",
                "updated_at": "source.updated_at",
                "ingested_at": "source.ingested_at"   ,
                "last_updated_at" : current_timestamp()        
            }
        )
        .execute()
    )   



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
