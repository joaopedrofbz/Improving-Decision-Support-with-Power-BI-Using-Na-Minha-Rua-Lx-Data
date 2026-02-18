# PL_BRONZE_WEATHER – Historical Daily Weather Ingestion

## Purpose
Fetch historical daily weather from Open-Meteo for all parishes in Na Minha Rua LX and land it as a Bronze Delta table.

This pipeline enables:
- **BN2 / BN3** – seasonal and anomaly baselines with exogenous context;
- **BN5** – weather-aware interpretation of incident workloads.

## Main artefacts (Fabric workspace)
- **Notebook:** `NB_Bronze_Weather_OpenMeteo` (Fabric notebook)
  - Retrieves the target date window (aligned with the incident analysis period);
  - Uses a governed list of parish coordinates (centroids) as query parameters;
  - Calls the Open-Meteo archive API for each parish and day;
  - Implements retry logic and batching for robust HTTP calls;
  - Writes results as `tbl_bronze_weather_raw` (Delta).

## Outputs (Lakehouse table)
- `tbl_bronze_weather_raw`
  - One row per **parish–day** with:
    - date, parish name, latitude/longitude;
    - daily precipitation

## Relation to thesis
Implements the **Bronze weather** tier of the Medallion architecture described in Chapter 3 and supports the BN5 weather-integration rationale.
