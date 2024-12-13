# Azure Medallion-architecture-Data-Engineering-End-To-End-Project

![Project Overview](https://github.com/OsamaELsohafy/Azure-Medallion-architecture-Data-Engineering-Project/blob/main/Project_overview.jpg)
Overview

This project showcases an End-to-End Data Engineering solution using Azure's ecosystem, structured on the Medallion Architecture framework. It demonstrates data ingestion, transformation, and analytics using Azure Data Factory, Databricks, Azure Synapse Analytics, and PySpark.

The Medallion Architecture categorizes data into:

Bronze Layer: Raw data ingestion from APIs or other sources.

Silver Layer: Cleaned and enriched data for intermediate processing.

Gold Layer: Analytics-ready data for reporting and dashboards.


## Key Features
Medallion Architecture Implementation: Designed data layers (Bronze, Silver, Gold) for optimized storage and processing.
Robust Data Pipelines: Built using Azure Data Factory for scalable data ingestion and orchestration.
Data Transformation: Utilized Databricks and PySpark for cleaning and enriching data in the Silver Layer.
Data Warehousing: Leveraged Azure Synapse Analytics for efficient querying and analytics.
Interactive Dashboards: Created Power BI dashboards for actionable insights from the Gold Layer.


## Technologies Used
Azure Data Factory: For orchestrating data pipelines.

Azure Data Lake Gen2: Serving as the data storage foundation.

Azure Databricks: For data transformation and real-time processing.

PySpark: To perform scalable big data transformations.

Azure Synapse Analytics: For warehousing and querying the transformed data.

Power BI: To visualize data for decision-making.

## Project Workflow

Data Architecture Design

Established the Medallion Architecture (Bronze, Silver, Gold layers).
Data Ingestion (Bronze Layer)

Collected raw data via API and stored it in Azure Data Lake.
Data Transformation (Silver Layer)

Cleaned and enriched data using PySpark in Databricks.
Data Warehousing (Gold Layer)

Loaded processed data into Azure Synapse Analytics for querying.
Visualization

Connected Power BI to Synapse Analytics to generate dashboards.
