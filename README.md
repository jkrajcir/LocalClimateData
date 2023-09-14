# Data Preparation and Aggregation of Local Climatological Data Dataset

## Overview

The dataset includes data points of climatic values at monthly, daily, and hourly intervals.

The average, variance, and standard deviation was calculated on data points for each interval. Monthly data points are grouped by year, daily grouped by day of the week, and hourly grouped by hour of the day.

All three intervals share some similar columns such as:

- Wind speed (miles per hour)
- Minimum/Maximum temperature (Fahrenheit)
- Atmospheric pressure (in Hg)
- Precipitation (inches)

There are several "outlier" columns:

- Monthly: Days with Heavy Fog
- Monthly: Days with Thunderstorms
- Hourly: Relative Humidity (percentage)
- Hourly: Visibility (miles)

Dataset and documenation:

- [West-Chicago-DuPage-Airport-Local-Climatological-Data-FROM-2014-01-01-TO-2023-09-03.csv](./West-Chicago-DuPage-Airport-Local-Climatological-Data-FROM-2014-01-01-TO-2023-09-03.csv) contains the dataset.
- [LCD_documentation.pdf](https://www.ncei.noaa.gov/pub/data/cdo/documentation/LCD_documentation.pdf) goes into greater detail about the contents of the dataset.

## Tech

Spark.NET library is being used to implement the Spark job to perform data preparation and aggregation. The serverless Spark pools in Azure Synapse Analytics is being using to run the Spark job. CSV dataset file and subsequent parquet files are stored in Azure Data Lake Storage Gen2.

## Implementation Steps Summary

1. CSV dataset is written to parquet files
2. Filter out unnecessary columns, including:

    - Columns that don't have any values or only has one distinct value
    - Redundant columns
    - Raw data column

3. Split filtered DataFrame into three DataFrames based on their report type:

    - Monthly summary DataFrame
    - Daily summary DataFrame
    - Hourly DataFrame

4. Cast several columns and perform some aggregation on each DataFrame, results shown below

## Results (text files)

- [Monthly aggregations grouped by year](./results/monthly.txt)
- [Daily aggregations grouped by day of the week](./results/daily.txt)
- [Hourly aggregations grouped by hour of the day](./results/hourly.txt)

## Other

- ["Home page"](https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ncdc:C00684/html) for U.S. Local Climatological Data.
- Other climate data datasets provided by the NOAA can be found [here.](https://www.ncdc.noaa.gov/cdo-web/datasets)
