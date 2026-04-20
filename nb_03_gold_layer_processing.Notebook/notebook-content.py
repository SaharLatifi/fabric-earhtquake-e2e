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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_date = "1999-08-17"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_eq = spark.read.table("silver_earthquake").filter(col("ingested_at")> start_date)
# df_eq = spark.read.table("silver_earthquake").filter(col("event_date") == "1990-06-21")
#display(df_eq)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This step creates the Gold analytical dataset from the Silver table.
# The data is enriched with additional analytical attributes such as:
#   - country_code (derived from latitude and longitude using reverse geocoding)
#   - sig_category (significance classification)
#   - mag_category (magnitude classification)
#   - depth_category (earthquake depth classification)
#   - hemisphere (derived from latitude)

get_country_code_udf = udf(get_country_code, StringType())
df_eq_enriched = df_eq \
                    .drop("ingested_at")\
                    .withColumn("country_code" , get_country_code_udf(col("latitude"),col("longitude"))) \
                    .withColumn("sig_category" ,
                                      when(col("sig") < 100 , "Minor")
                                      .when(col("sig") < 300 , "Moderate")
                                      .when(col("sig") < 600 , "Strong")
                                      .otherwise("Major")
                                ) \
                    .withColumn("depth_category" ,
                                      when(col("depth") < 70, "Shallow")
                                      .when(col("depth") < 300, "Intermediate")
                                      .otherwise("Deep")
                                ) \
                    .withColumn("mag_category" ,
                                    when(col("mag") < 3 , "Micro")
                                    .when(col("mag") < 4 , "Minor")
                                    .when(col("mag") < 5 , "Light")
                                    .when(col("mag") < 6 , "Moderate")
                                    .when(col("mag") < 7 , "Strong")
                                    .otherwise("Major")
                                ) \
                    .withColumn("hemisphere" ,
                                    when(col("latitude") >=0 , "Northern")
                                    .otherwise("Southern")
                                ) \
                    .withColumn("ingested_at" , current_timestamp())    

df_eq_enriched = df_eq_enriched.select(
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
                    "depth_category",
                    "mag_category",
                    "sig_category",
                    "hemisphere" ,
                    "status",
                    "location",
                    "url", 
                    "updated_at",
                    "ingested_at"                                                                                                                       

)        
#display(df_eq_enriched)             

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# country = get_country_code(18.0165, -66.7588)
# print(country)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

target_table = "gold_earthquake"

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
                "ingested_at": "source.ingested_at"                
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
                "ingested_at": "source.ingested_at"     
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
