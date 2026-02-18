# PL_BRONZE_INCIDENTS – Na Minha Rua LX Incident Ingestion

## Purpose
Ingest all Na Minha Rua LX Excel exports into the Lakehouse, append them into a single raw table, and expose a lightly standardised staging table for downstream Silver transformations.

This pipeline supports:
- **BN1** – by consolidating the full incident corpus;
- **BN6** – by preserving raw schema and provenance.

## Main artefacts (Fabric workspace)
- **Dataflow:** `DF_Bronze_Incidents_Append` (Power Query / Dataflow)
  - Loads yearly Excel files (e.g., 2020–2024);
  - Applies minimal type casting (dates, text, coordinates);
  - Appends all years into a single dataset;
  - Produces two outputs:
    - `tbl_bronze_incidents_raw` – raw schema, for audit/traceability;
    - `stg_incidents_appended` – same rows, renamed with English-friendly analytical columns for Silver.

## Outputs (Lakehouse tables)
- `tbl_bronze_incidents_raw`
  - One row per original record from Na Minha Rua LX.
  - Original column names preserved as much as possible.
- `stg_incidents_appended`
  - Same rows as the raw table.
  - Columns renamed to analysis-oriented names (e.g., `Registration Date`, `Parish Name`, `Area`, `Type`).
  - Used as the single input to Silver incident pipelines.

## Relation to thesis
Implements the **Bronze incidents** tier of the Medallion architecture described in Chapter 3 (ingestion and fidelity preservation).
