# Improving Decision Support with Power BI Using Na Minha Rua LX Data

This repository contains the technical artefacts of the master’s dissertation **“Dashboards for Urban Incident Reporting: Improving Decision Support with Power BI Using Na Minha Rua LX Data”**.

The repository is intended as a **technical appendix to Chapter 3 (Methodology)** and documents:
- The Medallion (Bronze–Silver–Gold) ELT design implemented in Microsoft Fabric;
- The Kimball-style dimensional model over Na Minha Rua LX incidents and Open-Meteo weather;
- The Power BI semantic model (TMDL) and DAX measures used by the dashboards.
---

## 1. Methodology in one page

### 1.1 Dimensional modelling (Kimball)

**Facts**
- `fact_incident_daily` (**event-level**): **1 row = 1 incident occurrence** (one citizen report record) linked to Date, Location, and IncidentType.  
  *Naming note:* although the physical table name includes “daily”, the grain is **not aggregated**; daily and higher-level rollups are computed in DAX and visuals.
- `fact_weather_daily` (**daily observations**): **1 row = 1 parish–day** with precipitation/temperature/wind metrics and derived flags (e.g., `Is_Rainy_Day`).

**Conformed dimensions**
- `dim_date`
- `dim_location`
- `dim_incident_type`

### 1.2 Medallion architecture in Microsoft Fabric
- **Bronze**: preserve raw Na Minha Rua LX exports and raw daily weather with minimal transformation.
- **Silver**: standardise, clean, and conform into dimensions and facts with surrogate keys.
- **Gold**: publish stable star-schema tables used by the Direct Lake semantic model.

### 1.3 Business Needs (BN1–BN7)
1. **BN1** – Standardised space–time visibility of citizen-reported workloads  
2. **BN2** – Contextualisation via seasonal baselines  
3. **BN3** – Early identification of anomalies and spatial hotspots  
4. **BN4** – Territory-focused prioritisation under capacity constraints  
5. **BN5** – Pragmatic integration of weather as an exogenous factor  
6. **BN6** – Transparent and traceable indicators grounded in open data  
7. **BN7** – Self-service exploration for different municipal roles  

These needs drive the pipelines, the dimensional model, and the DAX measures used in Power BI.

---

## 2. Thesis cross-reference (repo narratives)

- **Design and operationalisation with Medallion:** `docs/thesis/03_medallion_pipelines.md`  
- **Measures design (DAX):** `docs/thesis/03_measures_design.md`
