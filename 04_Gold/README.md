# 05_SemanticModel – Power BI Tabular Model

This folder contains the semantic layer that underpins all dashboards.

## Contents

- `model.tmdl`
  - Tabular model definition exported from Power BI:
    - tables: `dim_date`, `dim_location`, `dim_incident_type`,
      `fact_incident_daily`, `fact_weather_daily`;
    - relationships;
    - measure definitions;
    - display folders.

## Design highlights

The model implements:

- A Kimball-style star schema over Gold tables.
- Conformed dimensions for time, location, and incident type.
- Families of measures:
  - **Core workload** – `Incidents`, `Avg Incidents`;
  - **Time intelligence** – YTD, LY, YoY;
  - **Seasonal baselines** – daily and monthly baselines, adaptive version;
  - **Anomalies** – z-scores, flags, anomaly days %;
  - **Territorial pressure** – `Avg Incidents per Parish`, `Pressure Index`;
  - **Weather sensitivity** – rainy/dry splits, `Rainy Lift`, `Rainy Sensitivity Category`.

The narrative description of these measures is provided in Chapter 3 of the thesis (Measures Design), while this folder contains the exact technical implementation.
