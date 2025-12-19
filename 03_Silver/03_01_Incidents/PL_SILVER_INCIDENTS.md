# PL_SILVER_INCIDENTS – Dimensionalisation of Incidents

## Purpose

Transform the Bronze incident staging table `stg_incidents_appended` into:

- conformed dimensions:
  - `dim_date`
  - `dim_location`
  - `dim_incident_type`
- and the daily incident fact:
  - `fact_incident_daily`

This pipeline implements Kimball steps 2–4 (grain, dimensions, facts) for the **Incident Events** process and supports BN1–BN4 and BN6–BN7.

## Main artefacts (dataflows)

- `DF_Silver_DimDate.json`
  - Builds `dim_date_silver` from the incident date range.
  - Adds `Year`, `Month`, `Day`, `Trimester`, `Semester`, `Week Day`.
  - Creates surrogate key `sk_date` (`yyyymmdd`).
  - Adds time-intelligence flags (`Is Last Year`, `Is Last Year-1 Until Last Period`).

- `DF_Silver_DimLocation.json`
  - Builds `dim_location_silver` from parish names and subsection coordinates.
  - Normalises `Parish Name` and computes parish centroids.
  - Constructs a business key and assigns surrogate key `sk_location`.

- `DF_Silver_DimIncidentType.json`
  - Builds `dim_incident_type_silver` from `Area` and `Type`.
  - Normalises and deduplicates the incident taxonomy.
  - Constructs `bk_incident_type` and assigns `sk_incident_type`.

- `DF_Silver_FactIncidentDaily.json`
  - Reads `stg_incidents_appended`.
  - Derives `sk_date` and joins to `dim_date_silver`.
  - Joins to `dim_location_silver` and `dim_incident_type_silver`.
  - Enforces referential integrity (no null surrogate keys).
  - Adds `sk_fact_incident` and `bk_fact_incident` for auditing.
  - Outputs `fact_incident_daily_silver`.

## Outputs (Silver tables)

- `dim_date_silver`
- `dim_location_silver`
- `dim_incident_type_silver`
- `fact_incident_daily_silver`

## Relation to thesis

This pipeline corresponds to the Silver incident tier in Chapter 3, where DimDate, DimLocation, DimIncidentType, and FactIncidentDaily are described in detail.
