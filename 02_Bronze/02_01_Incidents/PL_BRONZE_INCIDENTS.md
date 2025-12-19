# PL_BRONZE_INCIDENTS – Na Minha Rua LX Incident Ingestion

## Purpose

Ingest all Na Minha Rua LX Excel exports into the Lakehouse, append them into a single raw table, and expose a lightly standardised staging table for downstream Silver transformations.

This pipeline supports:

- **BN1** – by consolidating the full incident corpus;
- **BN6** – by preserving raw schema and provenance.

## Main artefacts

- `DF_Bronze_Incidents_Append.json`  
  Power Query / Dataflow definition that:
  - loads yearly Excel files (e.g. 2020–2024);
  - applies minimal type casting (dates, text, coordinates);
  - appends all years into a single query;
  - outputs:
    - `tbl_bronze_incidents_raw` – raw schema, for audit;
    - `stg_incidents_appended` – same rows, English-friendly column names for Silver.

## Outputs (Lakehouse tables)

- `tbl_bronze_incidents_raw`
  - One row per original record from Na Minha Rua LX.
  - Original column names and types preserved as much as possible.

- `stg_incidents_appended`
  - Same rows as the raw table.
  - Columns renamed to analysis-oriented names (e.g. `Registration Date`, `Parish Name`, `Area`, `Type`, etc.).
  - Used as the single input to Silver incident pipelines.

## Relation to thesis

This pipeline implements the **Bronze incidents** part of the Medallion architecture described in Chapter 3 (Bronze – ingestion and fidelity preservation).
