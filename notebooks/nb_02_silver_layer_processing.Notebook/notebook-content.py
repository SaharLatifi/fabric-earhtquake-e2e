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

# MARKDOWN ********************

# # Eeq Events - Silver Layer Processing
# 


# CELL ********************

from pyspark.sql.functions import col,current_timestamp ,from_unixtime , to_date , date_format ,udf,when
from pyspark.sql.types import IntegerType, TimestampType , StringType 
from datetime import date , timedelta 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#start_date = "1990-06-21" #date.today() - timedelta(7) 
# end_date = "2026-04-14" #date.today() - timedelta(1)
# print(start_date,end_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the raw data from Bronze layer into a dataframe 
file_name = f"{start_date}_earthquake_data.json"
df_eq_raw = spark.read.format("json").option("multiline","true").load(f"Files/{file_name}")
#display(df_eq_raw)

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


get_country_code_udf = udf(get_country_code, StringType())
df_eq_silver = df_eq_raw.\
                select(
                        col("id").alias("event_id"),
                        col("geometry.coordinates")[0].alias('longitude') ,
                        col("geometry.coordinates")[1].alias("latitude"), 
                        col("geometry.coordinates")[2].alias("depth") ,
                        col("properties.mag").alias("mag") ,
                        col("properties.url").alias("url"),
                        col("properties.magtype").alias("mag_type"),
                        col("properties.sig").alias("sig"),
                        from_unixtime(col("properties.updated")/1000).cast("timestamp").alias("updated_at"),
                        col("properties.tsunami").alias("tsunami"),
                        col("properties.place").alias("location"),
                        from_unixtime(col("properties.time")/1000).cast("timestamp").alias("event_date_time"),
                        col("properties.type").alias("event_type") ,
                        col("properties.status").alias("status")
                )

df_eq_silver  = df_eq_silver \
                              .withColumn("country_code" , get_country_code_udf(col("latitude"),col("longitude"))) \
                              .withColumn("is_tsunami", when(col("tsunami") == 1, True).otherwise(False)) \
                              .withColumn("hemisphere" ,
                                    when(col("latitude") >=0 , "Northern")
                                    .otherwise("Southern")
                                ) \
                              .withColumn("event_date",to_date(col("event_date_time")))\
                              .withColumn("event_time",date_format(col("event_date_time"), "HH:mm:ss") )  \
                              .withColumn("ingested_at" , current_timestamp()) \
                              .withColumn("last_updated_at" , current_timestamp())
                              
#display(df_eq_silver )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_eq_silver.count()
#df_eq_silver.groupBy(col("event_type")).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### -----------------------------
# #### **Data Quality Enforcement**
# #### -----------------------------


# CELL ********************

# -----------------------------
# **Data Quality Enforcement**
# -----------------------------
# 1. Log all DQ check results to the monitoring table
# 2. Evaluate critical validation rules
# 3. If any critical rule fails, raise an error and stop the pipeline
# 4. If all critical checks pass, proceed to write the Silver dataset
df_eq_silver = df_eq_silver.filter(col("mag").isNotNull())

dq_result = []

# -----------------------------
# event_id checks (critical)
# -----------------------------

null_event_id_counts =  df_eq_silver.filter(col("event_id").isNull()).count()
dq_result.append(("event_id_not_null" ,null_event_id_counts , null_event_id_counts ==0,"critical" ))

duplicate_event_id_count = df_eq_silver.groupBy("event_id").count().filter(col("count")>1).count()
dq_result.append(("event_id_not_duplicate" ,duplicate_event_id_count , duplicate_event_id_count ==0,"Critical" ))


# -----------------------------
# latitude checks (critical)
# -----------------------------
null_latitude_count = df_eq_silver.filter(col("latitude").isNull()).count()
dq_result.append(("latitude_not_null", null_latitude_count, null_latitude_count == 0, "critical"))

invalid_latitude_count = df_eq_silver.filter((col("latitude") < -90) | (col("latitude") > 90)).count()
dq_result.append(("latitude_valid_range", invalid_latitude_count, invalid_latitude_count == 0, "critical"))

# -----------------------------
# longitude checks (critical)
# -----------------------------
null_longitude_count = df_eq_silver.filter(col("longitude").isNull()).count()
dq_result.append(("longitude_not_null", null_longitude_count, null_longitude_count == 0, "critical"))

invalid_longitude_count = df_eq_silver.filter((col("longitude") < -180) | (col("longitude") > 180)).count()
dq_result.append(("longitude_valid_range", invalid_longitude_count, invalid_longitude_count == 0, "critical"))

# -----------------------------
# magnitude checks 
# -----------------------------
null_magnitude_count = df_eq_silver.filter(col("mag").isNull()).count()
dq_result.append(("magnitude_not_null", null_magnitude_count, null_magnitude_count == 0, "critical"))

invalid_magnitude_count = df_eq_silver.filter((col("mag") < -2) | (col("mag") > 10)).count()
dq_result.append(("magnitude_valid_range", invalid_magnitude_count, invalid_magnitude_count == 0, "warning"))


# -----------------------------
# significance checks (critical)
# -----------------------------
null_significance_count = df_eq_silver.filter(col("sig").isNull()).count()
dq_result.append(("significance_not_null" ,null_significance_count , null_significance_count == 0 , "warning" ))

invalid_significance_count = df_eq_silver.filter( (col("sig") < 0 ) | (col("sig") > 1000 )).count() 
dq_result.append(("significance_invalid" ,invalid_significance_count , invalid_significance_count == 0 , "warning" ))

# -----------------------------
# depth checks (warning)
# -----------------------------
null_depth_count = df_eq_silver.filter(col("depth").isNull()).count()
dq_result.append(("depth_not_null" ,null_depth_count, null_depth_count==0,"warning" ))

invalid_depth_count = df_eq_silver.filter(col("depth")<0).count()
dq_result.append(("depth_non_negative" , invalid_depth_count, null_depth_count ==0 , "warning"))

# -----------------------------
# datetime checks (critical)
# -----------------------------
null_event_date_time_count = df_eq_silver.filter(col("event_date_time").isNull()).count()
dq_result.append(("event_date_time_not_null", null_event_date_time_count, null_event_date_time_count==0,"critical"))

null_updated_at_count = df_eq_silver.filter(col("updated_at").isNull()).count()
dq_result.append(("updated_at_not_null", null_updated_at_count, null_updated_at_count == 0, "critical"))

future_event_date_time_count = df_eq_silver.filter(col("event_date_time") > current_timestamp()).count()
dq_result.append(("event_time_not_future" , future_event_date_time_count, future_event_date_time_count==0,"warning"))

future_updated_at_count = df_eq_silver.filter(col("updated_at") > current_timestamp()).count()
dq_result.append(("updated_at_not_future" , future_updated_at_count, future_updated_at_count==0,"warning")) 

# -----------------------------
# type checks (critical)
# -----------------------------
null_event_type_count = df_eq_silver.filter(col("event_type").isNull()).count()
dq_result.append(("event_type_not_null", null_event_type_count, null_event_type_count == 0, "critical"))

invalid_event_type_count = df_eq_silver.filter(col("event_type") != "eq").count()
dq_result.append(("event_type_is_eq", invalid_event_type_count, invalid_event_type_count == 0, "warning"))

# -----------------------------
# Create DQ log dataframe
# -----------------------------
df_dq_result_silver =spark.createDataFrame(dq_result,["check_name", "failed_row_count", "passed", "severity"])
df_dq_result_silver = df_dq_result_silver.withColumn("check_date" , current_timestamp())
display(df_dq_result_silver)

# -----------------------------
# Write DQ log table
# -----------------------------
df_dq_result_silver.write.mode("append").saveAsTable("silver_earthquake_log")

# -----------------------------
# Check if any critical DQ rules failed
# -----------------------------
critical_failure = df_dq_result_silver.filter((col("severity") ==  "critical") & (col("passed") == False )).count()
#print(critical_failure)
if critical_failure > 0:
    raise ValueError("Critical data quality checks failed. See silver_dq_log table for details.")
 



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Business rule:
# Keep only VALIDATED records where event_type = 'eq'
# Other event types returned by the API are excluded from the Silver dataset

df_eq_silver = df_eq_silver.filter((col("event_type") == "earthquake") & (col("status") == "reviewed") )
#df_eq_silver.groupBy(col("event_type")).count().show()
#df_eq_silver.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ---------------------------------
# #### **Load the data in Silver Layer**
# #### ---------------------------------


# CELL ********************

# -----------------------------
# Silver Load Strategy: Merge / Upsert
# -----------------------------
# The Bronze layer retrieves a rolling event window, so the incoming dataset
# may contain both new records and previously ingested records with updated values.
#
# Silver therefore uses an upsert strategy instead.

from delta.tables import DeltaTable

target_table = "silver_earthquake"

# -----------------------------------
# If the table does not exist, create it
# -----------------------------------
if not spark.catalog.tableExists(target_table):
    df_eq_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

# -----------------------------------
# Otherwise perform MERGE (upsert)
# -----------------------------------
else:
    silver_table = DeltaTable.forName(spark,target_table)
    (
        silver_table.alias("target") 
        .merge(
            df_eq_silver.alias("source") ,
            "target.event_id = source.event_id"            
        )
        .whenMatchedUpdate(
            condition="source.updated_at > target.updated_at" ,
            set ={
                "event_id" :"source.event_id",
                "longitude": "source.longitude",
                "latitude":"source.latitude" ,
                "depth":"source.depth",
                "mag":"source.mag",
                "url":"source.url",
                "mag_type":"source.mag_type",
                "sig":"source.sig",
                "updated_at":"source.updated_at",
                "is_tsunami":"source.is_tsunami",
                "location":"source.location",
                "event_date_time":"source.event_date_time",
                "event_date":"source.event_date",
                "event_time":"source.event_time",
                "event_type":"source.event_type",
                "status":"source.status",
                "ingested_at":"source.ingested_at" ,
                "last_updated_at" : current_timestamp()
            }
        )
        .whenNotMatchedInsert(
            values = {
                "event_id" :"source.event_id",
                "longitude": "source.longitude",
                "latitude":"source.latitude" ,
                "depth":"source.depth",
                "mag":"source.mag",
                "url":"source.url",
                "mag_type":"source.mag_type", 
                "sig":"source.sig",
                "updated_at":"source.updated_at",
                "is_tsunami":"source.is_tsunami",
                "location":"source.location",
                "event_date_time":"source.event_date_time",
                "event_date":"source.event_date",
                "event_time":"source.event_time",
                "event_type":"source.event_type",
                "status":"source.status",
                "ingested_at":"source.ingested_at" ,
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
