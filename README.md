### Data Lakehouse Architecture

This project implements a **Data Lakehouse architecture using the Medallion Architecture**, consisting of three data layers: **Bronze, Silver, and Gold**. The purpose of the architecture is to create a scalable and organized data pipeline that moves data from its raw source into clean, transformed, and business-ready datasets.

The data pipeline is developed using **Python and PySpark** and deployed on the **Databricks platform**.

#### Bronze Layer – Raw Data

The **Bronze layer** stores data in its original form as it is extracted from the source systems. No business transformations or data cleansing are applied at this stage.

The purpose of this layer is to preserve the original source data, providing a reliable historical copy that can be used for data recovery, auditing, troubleshooting, and reprocessing when necessary.

#### Silver Layer – Cleaned and Transformed Data

The **Silver layer** contains data that has been cleaned, validated, and transformed from the Bronze layer.

Typical processing includes:

* Removing duplicate records
* Handling null and invalid values
* Standardizing data formats
* Applying data quality rules
* Renaming and standardizing columns
* Joining and transforming related datasets
* Applying business and technical transformations

The goal of the Silver layer is to provide **consistent and reliable datasets** that are ready for further business processing.

#### Gold Layer – Business-Ready Data

The **Gold layer** contains business-ready datasets designed for **data analysts, reporting, dashboards, and business intelligence**.

Complex transformations and business logic are performed before the data reaches this layer so that analysts can access the required information with simple SQL queries instead of repeatedly performing complex transformations themselves.

This layer may contain curated tables, aggregated datasets, and SQL views designed around specific business requirements and analytical use cases.

### Technology Stack

* **Python** – ETL and data engineering development
* **PySpark** – Distributed data extraction and transformation
* **Databricks** – Data engineering and Lakehouse platform
* **SQL** – Data transformation, querying, and business logic
* **Medallion Architecture** – Bronze → Silver → Gold data organization

### Overall Data Flow

**Source Systems → Bronze → Silver → Gold → Data Analysts / BI / Reporting**

The architecture separates **raw data storage, data transformation, and business consumption**, making the pipeline easier to maintain, troubleshoot, scale, and extend as the volume and number of data sources increase.
