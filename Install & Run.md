## Install & Run (Manual import in Microsoft Fabric — Direct Lake)
---

### 0) What you will import (artefact packaging)

This repo is reproducible without workspace Git integration via these artefacts:

1) **Dataflows Gen2 templates (`*.pqt`)**  
   Import restores queries/parameters/settings; connections/credentials and refresh schedules must be reconfigured after import.

2) **Fabric Data Factory pipelines (`*.zip`)**  
   Export/import uses pipeline templates; Fabric exports a pipeline as a `.zip` file named after the pipeline, and import prompts for connections before instantiation.

3) **Notebook(s) (`*.ipynb`)**  
   Used for the Bronze weather extraction (Open-Meteo) before Silver weather conformance.

4) **Power BI report (`*.pbix`)**  
   The repo includes a PBIX for the report layer. The semantic model is handled separately as **Direct Lake + TMDL** (next section). (User note; Direct Lake design is stated in the repo’s README.)

5) **Semantic model exported as TMDL code**  
   The semantic model metadata (relationships, measures, display folders) is exported as **TMDL**, which can be applied/edited via **TMDL view** in Power BI Desktop.

---

### 1) Prerequisites

- A Microsoft Fabric workspace where you can create/run:
  - **Lakehouse**, **Dataflows Gen2**, **Notebooks**, **Data pipelines (Data Factory)**, and a **Semantic model**.
- Power BI Desktop (Windows) with **TMDL view** available/enabled (for applying the exported TMDL code).
- External data access:
  - Lisbon open data dataset **“Ocorrências ‘Na Minha Rua’”** (incident exports).
  - Open-Meteo **Historical Weather API** (daily variables).

---

### 2) Workspace setup (Fabric)

1. Create a workspace (e.g., `NMRLX`).
2. Create a Lakehouse (e.g., `LH_NMRLX`).
3. In Lakehouse **Files**, create:
   - `Files/incidents/`

---

### 3) Acquire & upload incident exports (Bronze input)

Upload the Na Minha Rua LX year exports into:
- `LH_NMRLX → Files/incidents/`

---

### 4) Import Dataflows Gen2 from `.pqt` (Bronze + Silver)

#### 4.1 Import procedure (repeat per template)

1. Workspace → **New → Dataflow Gen2**
2. Power Query start screen → **Import from Power Query template**
3. Select the `*.pqt`
4. **Configure connection** (credentials are not stored in `.pqt`)
5. Configure **Data destinations** to the Lakehouse tables
6. **Publish**
7. Run once to validate tables are created/updated in the Lakehouse

#### 4.2 Canonical Dataflows and expected outputs (table contract)

Create/import and publish the following logical set (names must match your templates and downstream pipeline activities):

**Bronze**
- `DF_Bronze_Incidents_Append` →  
  - `tbl_bronze_incidents_raw`  
  - `stg_incidents_appended`

**Silver (Incidents)**
- `DF_Silver_DimDate` → `dim_date_silver`
- `DF_Silver_DimLocation` → `dim_location_silver`
- `DF_Silver_DimIncidentType` → `dim_incident_type_silver`
- `DF_Silver_FactIncidentDaily` → `fact_incident_daily_silver`

**Silver (Weather)**
- `DF_Silver_FactWeatherDaily` → `fact_weather_daily_silver`

---

### 5) Import notebook(s) (Bronze weather extraction)

1. Workspace → **Upload/Import → Notebook**
2. Upload the `*.ipynb` notebook(s) from the repo
3. Attach each notebook to `LH_NMRLX`
4. Confirm the notebook writes to the intended Bronze table:
   - `tbl_bronze_weather_raw`
   - timezone `Europe/Lisbon`
   - daily variables used for weather sensitivity analysis (as per project design)
5. Do not run yet if the notebook depends on Silver dimensions (recommended order below).

---

### 6) Import Fabric pipelines from exported `.zip` templates

Your pipelines are exported/imported as **Data Factory templates**.

#### 6.1 Import procedure (repeat per pipeline zip)

1. Workspace → New → **Data pipeline**
2. Pipeline editor → **Home → Import**
3. Select the pipeline `*.zip`
4. Choose/confirm **connections**
5. Select **Use this template** to instantiate the pipeline

---

### 7) Create the Direct Lake semantic model (Gold → Semantic layer)

Direct Lake semantic models can be created **from the Lakehouse in Fabric** or **from Power BI Desktop via OneLake catalog**.

**Option A (Fabric UI):**
1. Open the Lakehouse (Gold tables available).
2. Select **New semantic model**.
3. Choose the Gold tables:
   - `dim_date`, `dim_location`, `dim_incident_type`, `fact_incident_daily`, `fact_weather_daily`
4. Save the semantic model in the workspace.

**Option B (Power BI Desktop):**
1. Power BI Desktop → OneLake catalog → select Lakehouse → **Connect**
2. Choose tables and create the Direct Lake semantic model in the service (Desktop provides live editing).

---

### 8) Apply the exported TMDL code to the semantic model

The repo’s semantic model was exported using **TMDL**. Apply it to reproduce relationships/measures/metadata consistently.

1. Open the semantic model for editing in Power BI Desktop.
2. Switch to **TMDL view**.
3. Paste/import the repository’s TMDL code (e.g., `model.tmdl` content).
4. **Apply** changes.
5. Publish/Save back to the workspace semantic model as needed by your workflow.

---

### 9) Execute the end-to-end run

Run the orchestration pipeline or execute in the dependency-safe sequence below.

**Recommended sequence (dependency-safe):**
1. Run Bronze incidents dataflow(s)
2. Run Silver incident dimension + incident fact dataflows
3. Run Bronze weather notebook (writes `tbl_bronze_weather_raw`)
4. Run Silver weather conformance dataflow
5. Run Gold publish pipeline (Silver → Gold copy)
6. Refresh semantic model (if not already handled by the pipeline)

---

### 10) Open the PBIX report and bind it to the semantic model

1. Open the repository’s `*.pbix` in Power BI Desktop.
2. If the report is a **thin report** (live connection), update the connection to point to your workspace semantic model.

---
