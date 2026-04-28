# fabric-earthquake-e2e 🌍

## Table of Contents
- [Project Overview](#project-overview)
- [Business Questions](#business-questions)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [Data Pipeline](#data-pipeline--processing)
- [Dashboard](#dashboard-power-bi)
- [Repository Structure](#repository-structure)
- [Tech Stack](#tech-stack)


## Project Overview

This project explores global earthquake data using Microsoft Fabric. The data is sourced from the USGS Earthquake API and processed through an end-to-end data pipeline.

The solution follows a **Medallion Architecture (Bronze, Silver, Gold)** to progressively improve the structure, quality, and usability of the data. Raw API data is ingested into the Bronze layer, cleaned and enriched in the Silver layer, and transformed into curated analytics-ready tables in the Gold layer.

The final dataset is used to build a Power BI report that helps analyze earthquake activity by location, magnitude, depth, significance, and time.

**Data Source:**  
USGS Earthquake API  
https://earthquake.usgs.gov/fdsnws/event/1/

---
## Business Questions

The data model and reporting layer are designed to support analysis of global earthquake activity and answer questions such as:

- How does earthquake frequency change over time?
- Which countries experience the highest number of earthquakes?
- Which countries have the highest average earthquake magnitude?
- What is the distribution of earthquakes by depth category?
- What is the distribution of earthquakes by significance level?
- Which countries are associated with the most significant earthquake events?
- What are the most recent earthquake events?
- How are earthquake events geographically distributed across countries?
---
## Architecture

This project follows the Medallion Architecture pattern to structure data processing into three layers: Bronze, Silver, and Gold.

### Bronze — Raw Data
- Ingests earthquake data from the USGS API using a rolling 7-day window  
- Stores raw JSON snapshots in the Fabric Lakehouse  
- Preserves source structure with minimal transformation  
- Captures both new and updated events  

### Silver — Cleaned & Structured Data
- Flattens and standardizes the raw JSON data  
- Extracts key fields (magnitude, location, timestamps, coordinates)  
- Applies data quality validations (critical and non-critical checks)  
- Uses an upsert (merge) strategy to maintain a consolidated dataset  

### Gold — Analytics-Ready Data
- Enriches data for analytical use  
- Derives country from coordinates using reverse geocoding  
- Enriches the dataset with derived attributes (significance, depth)
- Produces a curated dataset optimized for reporting and Power BI  

**Environment Setup**
A dedicated Fabric environment is used to install the `reverse_geocoder` library, enabling derivation of `country_code` from geographic coordinates.


An architecture diagram is provided below to illustrate the end-to-end data flow.

![Architecture Diagram](architecture/Architecture.png)

➡️ **[Open full-size diagram](architecture/Architecture.png)**

---
## Data Model
The solution uses a star schema design to support efficient analytical queries.

- **fact_earthquake** contains event-level data (magnitude, time, location, depth, significance)
- Dimension tables provide descriptive context:
  - **dim_country**
  - **dim_mag_category**
  - **dim_depth_category**
  - **dim_sig_category**

This structure enables flexible analysis across geographic, temporal, and categorical dimensions.
![Data Model](docs/data-model.png)


## Pipeline Overview

> **Note (Design Simplification)**
>  
> To simplify this end-to-end practice project, the pipeline assumes daily execution and processes only the previous day's file.  
>  
> I’m aware this approach is not fully idempotent, as missed or failed runs may result in unprocessed data.  
>  
> For the purpose of this project, I’ve intentionally skipped implementing a more robust ingestion pattern to keep the pipeline simple and focused on the core flow.

**Bronze Layer (Notebook → Lakehouse)**  
- Notebook extracts earthquake data from the API for a fixed date range (today-7 to today-1)  
- Raw data is stored in the Lakehouse (Bronze layer)

**Silver Layer (Notebook → Lakehouse)**  
- Notebook processes the previous day's file  
- Applies basic transformations and writes cleaned data to Silver tables in the Lakehouse  

**Gold Layer (Stored Procedure → Warehouse)**  
- Stored procedure loads data from Silver into dimensional and fact tables  
- Data is stored in the Warehouse for reporting and analytics  





### Visualization

The final dataset from the **Gold Layer** is used in **Power BI** to build dashboards and explore earthquake patterns and insights.

---

