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
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
from datetime import date , timedelta 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = date.today() - timedelta(7) 
end_date = date.today() - timedelta(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(start_date, end_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# API URL with star and end dates
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch the data using GET request
response = requests.get(url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if the request was successful
if response.status_code  == 200:
    # Get the data in JSON format
    data_json = response.json()
    data_json = data_json['features']

    file_name = f"{start_date}_earthquake_data.json"
    file_path = f"/lakehouse/default/Files/{file_name}"

    # Open the file and save the data in JSON format
    with open(file_path,'w') as file:
        json.dump(data_json,file,indent=  4)
    print(f"Data has been loaed to {file_path}")

else:
    print("Data could not be fetched, staus_code:" , response.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline","true").json(f"Files/{file_name}")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
