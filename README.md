# Improving Decision Support with Power BI Using Na Minha Rua LX Data

This repository contains the technical artefacts of the master’s dissertation  
**“Dashboards for Urban Incident Reporting: Improving Decision Support with Power BI Using Na Minha Rua LX Data”**.

The repo acts as a **technical appendix to Chapter 3 (Methodology)** of the thesis:
- Full Medallion (Bronze–Silver–Gold) ELT pipelines in Microsoft Fabric;
- Kimball dimensional model over Na Minha Rua LX incidents and Open-Meteo weather;
- Power BI semantic model (TMDL) and DAX measures;
- Dashboard report file and screenshots.

## 1. Methodology in one page

The project uses:

- **Kimball dimensional modelling**  
  Facts: `FactIncidentDaily`, `FactWeatherDaily`.  
  Conformed dimensions: `DimDate`, `DimLocation`, `DimIncidentType`.  
  Grain:  
  - Incidents – one row per *Day × Parish × IncidentType*  
  - Weather – one row per *Day × Parish*

- **Medallion Architecture in Microsoft Fabric**  
  - **Bronze** – raw Na Minha Rua LX Excel exports + raw Open-Meteo daily weather, preserved with minimal changes.  
  - **Silver** – cleaning, standardisation and conformance into dimensional tables and daily facts.  
  - **Gold** – publication of star schemas and refresh of a Direct Lake semantic model used by the dashboards.

- **Business Needs BN1–BN7**  
  1. Standardised space–time visibility of citizen-reported workloads  
  2. Contextualisation via seasonal baselines  
  3. Early identification of anomalies and spatial hotspots  
  4. Territory-focused prioritisation under capacity constraints  
  5. Pragmatic integration of weather as an exogenous factor  
  6. Transparent and traceable indicators based on open data  
  7. Self-service exploration for different municipal roles  

These needs drive the pipelines, the dimensional model and the measures used in the Power BI dashboards.

Full narrative:  
- [Design and Operationalisation with Medallion](docs/thesis/03_medallion_pipelines_full.md)  
- [Measures Design](docs/thesis/03_measures_design_full.md)
