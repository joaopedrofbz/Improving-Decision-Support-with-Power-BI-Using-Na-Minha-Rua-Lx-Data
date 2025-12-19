# PL_GOLD_PUBLISH – Gold Star Schemas and Semantic Model Refresh

## Purpose

Publish the conformed Silver tables into stable Gold star-schema tables and refresh the Power BI Direct Lake semantic model.

This pipeline is the Gold tier in the Medallion architecture and provides the analytical contract used by all dashboards.

## Activities (copy operations)

- `Load_dim_date.json`
  - Copies `dim_date_silver` → `dim_date`.

- `Load_dim_location.json`
  - Copies `dim_location_silver` → `dim_location`.

- `Load_dim_incident_type.json`
  - Copies `dim_incident_type_silver` → `dim_incident_type`.

- `Load_fact_incident_daily.json`
  - Copies `fact_incident_daily_silver` → `fact_incident_daily`.

- `Load_fact_weather_daily.json`
  - Copies `fact_weather_daily_silver` → `fact_weather_daily`.

Each copy is a 1-to-1 mapping with no additional transformation, as all cleaning and conformance happen in Silver.

## Semantic model refresh

After all copy activities succeed, the pipeline triggers a refresh of the Direct Lake semantic model bound to:

- `dim_date`
- `dim_location`
- `dim_incident_type`
- `fact_incident_daily`
- `fact_weather_daily`

## Relation to thesis

PL_GOLD_PUBLISH is described in Chapter 3 under the Gold tier and links directly to the semantic model and measures specified in Section 3.6.
