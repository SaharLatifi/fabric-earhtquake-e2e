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

The bronze layer is responsible for data ingestion.

- A **PySpark notebook** calls the **USGS Earthquake API**
- Raw earthquake data is collected and stored in the **Fabric Lakehouse**
- Data is stored with minimal transformation to preserve the original structure

This layer acts as the raw data foundation for the pipeline.


[Open Notebook](nb_01_bronze_layer_processing.Notebook/notebook-content.py)
---

### Silver Layer — Cleaned & Structured Data

The silver layer focuses on improving data quality and structure.

- PySpark notebooks transform the raw data
- Data is cleaned and standardized
- Relevant fields are selected and formatted
- Data types and schema are improved

At this stage, the dataset becomes more structured and suitable for analysis.

---

### Gold Layer — Analytics Ready Data

The gold layer prepares the final dataset used for reporting.

- Additional transformations are applied
- Aggregations or derived fields may be created
- The dataset is optimized for analytical use

This layer provides the curated data that feeds into reporting and dashboards.
## Architecture Diagram

![Architecture Diagram](architecture/Architecture.png)

➡️ **[Open full-size diagram](architecture/Architecture.png)**
---

### Visualization

The final dataset from the **Gold Layer** is used in **Power BI** to build dashboards and explore earthquake patterns and insights.

---

