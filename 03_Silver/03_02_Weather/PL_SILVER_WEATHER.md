# PL_SILVER_WEATHER – Conformance of Weather Data

## Purpose

Align Bronze weather data with the incident dimensions by:

- joining weather rows to `dim_date_silver` and `dim_location_silver`;
- deriving daily flags such as `Is_Rainy_Day`.

This enables integrated analyses of incidents and weather at the parish–day level (BN2, BN3, BN5).

## Main artefact

- `DF_Silver_FactWeatherDaily.json`
  - Reads `tbl_bronze_weather_raw`.
  - Normalises parish name to match `dim_location_silver`.
  - Derives surrogate key `sk_date` (`yyyymmdd`).
  - Joins to `dim_location_silver` on parish + centroid coordinates.
  - Derives `Is_Rainy_Day` from `precipitation_sum`.
  - Outputs `fact_weather_daily_silver` with:
    - `sk_date`, `sk_location`;
    - daily weather metrics; 
    - `Is_Rainy_Day`.

## Outputs (Silver table)

- `fact_weather_daily_silver`

## Relation to thesis

Referenced in Chapter 3 under the Silver weather tier and in the description of FactWeatherDaily and weather-sensitive measures.
