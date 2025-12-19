# PL_BRONZE_WEATHER – Historical Daily Weather Ingestion

## Purpose

Fetch historical daily weather (precipitation, temperature, wind) from Open-Meteo for all parishes in Na Minha Rua LX and land it as a Bronze Delta table.

This pipeline enables:

- **BN2 / BN3** – seasonal and anomaly baselines with exogenous context;
- **BN5** – weather-aware interpretation of incident workloads.

## Main artefacts

- `NB_Bronze_Weather_OpenMeteo.ipynb`
  - Fabric notebook that:
    - reads the date window from `dim_date_silver` (min/max Registration Date);
    - reads parish coordinates from `dim_location_silver`;
    - calls the Open-Meteo archive API for each parish and day;
    - implements retry logic and batching for robust HTTP calls;
    - writes the results as `tbl_bronze_weather_raw` (Delta).

## Outputs (Lakehouse table)

- `tbl_bronze_weather_raw`
  - One row per **parish–day** with:
    - date;
    - parish name;
    - latitude/longitude;
    - daily precipitation;
    - min/max temperature;
    - max wind speed.

## Relation to thesis

This pipeline implements the **Bronze weather** part of the Medallion architecture described in Chapter 3 and is referenced in the methods section on weather integration (BN5).
