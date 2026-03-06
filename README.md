# HR Data Pipeline with Databricks (Medallion Architecture)

This project implements a simple **data pipeline using Databricks and Delta Live Tables (DLT)** following the **Medallion Architecture (Bronze → Silver)**.

## Project Overview

The pipeline ingests HR-related CSV datasets and processes them through multiple layers to prepare the data for analytics and visualization.

The following datasets are used:

- hr_dataset
- fake_totals
- fake_waffle_chart

## Pipeline Architecture

RAW → BRONZE → SILVER → BI

### Raw Layer
Raw CSV files are uploaded into the `rawdata` volume.

Example structure:

  ### Bronze Layer
The Bronze pipeline uses **Databricks Autoloader** to ingest files into Delta tables.

Key characteristics:

- Schema inference with `cloudFiles`
- Streaming ingestion
- Schema evolution enabled
- Basic column normalization

### Silver Layer
The Silver layer is implemented using **Delta Live Tables (DLT)**.

Transformations include:

- Data type casting
- Data quality expectations
- SCD Type 2 implementation for employee data
- Streaming transformations

Note:  
Dataset names must be **manually specified in the pipeline parameters**.

## Data Visualization

After the pipeline processing is completed, the curated data is connected to **Tableau**.

A dashboard was created to visualize HR metrics and explore the processed data.

## Technologies Used

- Databricks
- Delta Lake
- Delta Live Tables (DLT)
- Autoloader
- PySpark
- Tableau
