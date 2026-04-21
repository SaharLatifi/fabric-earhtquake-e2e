# fabric-earthquake-e2e 🌍

## Project Overview

This project explores global earthquake data using Microsoft Fabric.  
The data is sourced from the USGS Earthquake API and processed through an end-to-end data pipeline.

The project follows a **Medallion Architecture (Bronze, Silver, Gold)** to progressively improve the structure, quality, and usability of the data.  
The final curated dataset is used to build a Power BI report for analysis and visualization.

Data Source:  
USGS Earthquake API  
https://earthquake.usgs.gov/fdsnws/event/1/

---

## Architecture

This project uses the **Medallion Architecture** pattern to organize the data into three layers.

### Bronze Layer — Raw Data

The Bronze layer stores the raw earthquake data retrieved from the **USGS Earthquake API**.

At each pipeline run, the API is queried using a **rolling time window (last 7 days)**.  
This approach ensures that both **new earthquake events** and **possible updates to recently reported events** are captured.

Each execution saves the API response as a **raw JSON snapshot** in the Fabric Lakehouse.  
The Bronze layer preserves the **original source structure** and maintains the ingestion history with minimal processing.

Duplicate or modified records are expected at this stage and are handled later in the **Silver layer**.

#### Raw Data Structure (USGS API Response)

The USGS Earthquake API returns data in **GeoJSON format**.

The response contains two main sections:

- **metadata** → Information about the request (API version, query URL, record count, etc.)
- **features** → The actual earthquake event records

Each element inside `features` represents an earthquake event and contains nested objects such as:

- **properties** → event attributes (magnitude, location description, timestamps, status, etc.)
- **geometry** → geographic coordinates (`longitude`, `latitude`, `depth`)

These nested fields are expanded and flattened in the **Silver layer** to create a structured dataset suitable for analysis.

Example structure of the API response:

```json
{
  "type": "FeatureCollection",
  "metadata": {
    "generated": 1776204023000,
    "title": "USGS Earthquakes",
    "status": 200,
    "count": 4732
  },
  "features": [
    {
      "type": "Feature",
      "properties": {
        "mag": 1.97,
        "place": "1 km E of Magas Arriba, Puerto Rico",
        "time": 1776124505710,
        "status": "reviewed",
        "tsunami": 0,
        "sig": 60,
        "magType": "md",
        "type": "earthquake"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [-66.7588, 18.0165, 13.01]
      },
      "id": "pr71513633"
    }
  ]
}


```
🔗[Open Notebook](nb_01_bronze_layer_processing.Notebook/notebook-content.py)
---
### Silver Layer — Cleaned & Structured Data

The Silver layer transforms the raw earthquake data from the Bronze layer into a **clean, structured dataset** suitable for downstream processing and analysis.

This stage focuses on **data standardization, validation, and consolidation** while preserving the core event-level data from the source.

#### Data Transformation

Key transformations performed in the Silver layer include:

- Read raw JSON data from the Bronze Lakehouse
- Flatten nested JSON objects (`properties`, `geometry`)
- Extract geographic coordinates into separate columns (`longitude`, `latitude`, `depth`)
- Select relevant fields required for analysis
- Rename columns for clarity and consistency
- Convert data types (timestamps, numeric values)
- Standardize schema and field formats

#### Data Quality Validation

The Silver pipeline enforces a set of **data quality checks** to ensure reliability of the dataset before writing to the Silver table.
Each validation step logs its result to a **data quality monitoring table**.

Data quality checks are classified as:

- **Critical checks** → pipeline execution stops if the rule fails
- **Non-critical checks** → warnings are logged but processing continues

This ensures that invalid or corrupted records **do not propagate to downstream layers**.



The Silver table uses an **upsert (merge) strategy** to maintain a consolidated dataset of earthquake events.
The result of this stage is a structured **Silver Delta table** containing validated earthquake event records.
This table serves as the **source dataset for the Gold layer**, where additional analytical enrichments are applied.

### Gold Layer — Analytics Ready Data

The Gold layer enriches the structured dataset from the Silver layer and prepares it for analytical consumption.

At this stage, additional transformations and derived attributes are created to improve the usability of the dataset for reporting and exploration.

#### Environment Setup

A dedicated **Fabric environment** is attached to the Gold notebook to install and use the `reverse_geocoder` Python library.

This library is used to perform **reverse geocoding**, enabling the pipeline to derive a `country_code` from the geographic coordinates (`latitude`, `longitude`) of each earthquake event.

#### Data Enrichment

The Gold layer adds several analytical attributes to the dataset, including:

- `country_code` — derived from latitude and longitude using reverse geocoding
- `sig_category` — classification of earthquakes based on the USGS significance score
- `depth_category` — classification of earthquakes by depth
- `hemisphere` — geographic hemisphere derived from latitude
- `ingested_at` — timestamp indicating when the record was processed in the Gold layer

These enrichments improve the dataset's usability for geographic and analytical exploration.
The result of this stage is a **single enriched event-level table**


## Architecture Diagram

![Architecture Diagram](architecture/Architecture.png)

➡️ **[Open full-size diagram](architecture/Architecture.png)**
---

### Visualization

The final dataset from the **Gold Layer** is used in **Power BI** to build dashboards and explore earthquake patterns and insights.

---

