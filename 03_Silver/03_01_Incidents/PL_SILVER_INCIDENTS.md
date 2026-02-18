# PL_SILVER_INCIDENTS – Dimensionalisation of Incidents

## Purpose
Transform the Bronze incident staging table `stg_incidents_appended` into Silver conformed tables:
- `dim_date_silver`
- `dim_location_silver`
- `dim_incident_type_silver`
- `fact_incident_daily_silver`

This pipeline implements Kimball steps 2–4 (grain, dimensions, facts) for the **Incident Events** process and supports BN1–BN4 and BN6–BN7.

## Main artefacts (Fabric workspace)
- **Dataflow:** `DF_Silver_DimDate`
  - Builds `dim_date_silver` from the incident date range.
  - Adds Year, Month, Day, Trimester, Semester, Week Day.
  - Creates surrogate key `sk_date` (`yyyymmdd`) and time-intelligence flags.
- **Dataflow:** `DF_Silver_DimLocation`
  - Builds `dim_location_silver` from parish names and subsection coordinates.
  - Normalises parish names and computes parish centroids.
  - Constructs `bk_location` and assigns surrogate key `sk_location`.
- **Dataflow:** `DF_Silver_DimIncidentType`
  - Builds `dim_incident_type_silver` from `Area` and `Type`.
  - Normalises/deduplicates the incident taxonomy.
  - Constructs `bk_incident_type` and assigns surrogate key `sk_incident_type`.
- **Dataflow:** `DF_Silver_FactIncidentDaily`
  - Reads `stg_incidents_appended` as the **event-level** incident staging table.
  - Derives `sk_date`, joins to `dim_location_silver` and `dim_incident_type_silver`.
  - Enforces referential integrity (no null surrogate keys).
  - Adds `sk_fact_incident` for auditing/lineage.
  - Outputs `fact_incident_daily_silver` with **1 row = 1 incident occurrence**.

## Outputs (Silver tables)
- `dim_date_silver`
- `dim_location_silver`
- `dim_incident_type_silver`
- `fact_incident_daily_silver`

## Relation to thesis
Corresponds to the Silver incident tier in Chapter 3, where DimDate, DimLocation, DimIncidentType, and the event-level incident fact are described.
