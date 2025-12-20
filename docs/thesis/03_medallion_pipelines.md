DESIGN AND OPERATIONALISATION WITH MEDALLION
The Kimball design is operationalised in Microsoft Fabric through a set of ELT pipelines that follow the Medallion pattern (Bronze → Silver → Gold). Bronze preserves raw data with minimal transformation; Silver standardises and conforms them into dimensional structures; Gold publishes the star schemas that underpin the semantic model in Power BI. This subsection documents the concrete pipelines, their activities, and how they collectively implement Step 7 (“design the ELT/ETL system”) of Kimball’s nine-step method.

BRONZE – INGESTION AND FIDELITY PRESERVATION
The Bronze tier preserves the original shape of Na Minha Rua LX incident exports and of the daily weather history, ensuring that any indicator can be traced back to raw records. Two pipelines are defined: PL_BRONZE_INCIDENTS and PL_BRONZE_WEATHER.
PL_BRONZE_INCIDENTS – Incident ingestion
Purpose. Load all Na Minha Rua LX Excel exports into the Lakehouse, append them into a single raw table, and expose a lightly standardised staging view for downstream dimensional modelling. This supports BN1 and BN6 by consolidating the incident corpus while preserving provenance.
Pipeline structure. Figure shows PL_BRONZE_INCIDENTS, which contains a single Dataflow activity:  

<img width="1036" height="550" alt="image" src="https://github.com/user-attachments/assets/56691bc4-1e74-4f89-9ccb-1f2fe1fdb4f4" />

Activity “Load Incidents” – Invokes the dataflow DF_Bronze_Incidents_Append in the Tese João Ramos workspace.
 
Figure 2-DF_Bronze_Incidents_Append
Inside DF_Bronze_Incidents_Append (Figure 3.y), the following queries are defined:
1.	Year-specific queries (Original folder).
o	gopi2020subseccao xlsx
o	gopi2021subseccao xlsx
o	gopi2022subseccao xlsx
o	ocorrencias-2023 xlsx
o	ocorrencias-2024xlsx
Each query:
o	Connects to one Excel workbook delivered by Lisboa Aberta.
o	Promotes the first row to headers.
o	Applies basic type casting to the original columns (dt_registo, Freguesia, area, tipo, Subseccao, Longitude_Subseccao, Latitude_Subseccao).
 
2.	Query Append Data.
o	Step A1 – Append. Combines the five year-specific queries with Table.Combine, yielding a vertically concatenated incident table covering all available years.
o	Step A2 – Column renaming for staging. The key business columns are renamed to English, analysis-oriented names:
	dt_registo → Registration Date
	Freguesia → Parish Name
	area → Area
	tipo → Type
	Longitude_Subseccao → Subsection Longitude
	Latitude_Subseccao → Subsection Latitude
3.	Output query tbl_bronze_incidents_raw.
o	Outputs the appended incidents to the Lakehouse as tbl_bronze_incidents_raw with the original column names, preserving the raw schema for audit and potential re-processing. No renaming or row filtering is applied beyond type checks.
4.	Output query stg_incidents_appended.
o	Outputs the renamed version of Append Data to the Lakehouse as stg_incidents_appended.
o	This staging table is the single source for all Silver-level transformations (dimensional modelling and fact aggregation). It keeps every row of the raw data while providing stable, English-friendly column names for subsequent queries and notebooks.
The data destination of the dataflow is the Fabric Lakehouse associated with the thesis workspace, ensuring that both Bronze tables are stored as Delta files with automatic schema preservation and time-travel. 

PL_BRONZE_WEATHER – Historical daily weather
Purpose. Retrieve historical daily weather from Open-Meteo for the parishes present in Na Minha Rua LX and load the results into a Bronze table tbl_bronze_weather_raw. This enables BN2, BN3, and BN5 by providing an exogenous context for interpreting variations in incident workloads.

<img width="318" height="418" alt="image" src="https://github.com/user-attachments/assets/1d7b4f76-4bbb-45ab-a496-67e35688d1b9" />

 
Figure 3-PL_BRONZE_WEATHER
Notebook structure. PL_BRONZE_WEATHER consists of a Notebook activity (e.g., NB_Bronze_Weather_OpenMeteo) orchestrating five phases:

Phase 0 – Configuration and imports.
o	Defines TARGET_TABLE = "tbl_bronze_weather_raw".
o	Specifies the daily variables to retrieve (temperature_2m_max, temperature_2m_min, precipitation_sum, wind_speed_10m_max), the target time zone (Europe/Lisbon), retry strategy, batching size, and precision for latitude/longitude.
o	Imports PySpark, requests, pandas, numpy, and concurrency utilities, and defines ensure_col_order_and_types to enforce a canonical column order in the final pandas DataFrame.

Phase 1 – Date window from DimDate.
o	Reads dim_date_silver and selects the Registration Date column as a date.
o	Computes start_date and end_date as the minimum and maximum dates available, and derives N_DAYS as the number of days in this interval.
o	Logs the temporal window that will be requested from Open-Meteo. This guarantees temporal alignment between incident workloads and weather, which is essential for BQ5.1–BQ5.4.

Phase 2 – Coordinates per parish.
o	Reads dim_location_silver.
o	Validates that the table contains Parish Name, Latitude_Centroid, and Longitude_Centroid.
o	Normalises parish names (upper + trim) and casts coordinates to double.
o	Drops nulls and duplicates, producing a list of coordinates of the form (latitude, longitude, parish) for all target parishes.
o	Logs the number of parishes and the expected number of rows (parishes × N_DAYS).

Phase 3 – HTTP session and fetch_daily function.
o	Configures an HTTP session with retry logic (exponential backoff, respect for Retry-After headers, and handling of transient 5xx errors).
o	Defines fetch_daily(lat, lon, freg) which:
	Calls the Open-Meteo archive API for the configured date window and daily variables;
	Converts the JSON response into a pandas DataFrame;
	Converts time into a date column, attaches latitude, longitude, and parish; and
	Ensures numeric typing of all meteorological fields.

Phase 4 – Batch execution with progress logging.
o	Uses a ThreadPoolExecutor to call fetch_daily in parallel for batches of parishes, respecting a given batch size and cooldown between batches.
o	Accumulates all partial DataFrames into frames, tracks the number of rows obtained and expected, and logs progress (coordinates processed, rows retrieved, elapsed time, and estimated time remaining).
o	Raises an error if no data are returned, ensuring robustness against misconfiguration.

Phase 5 – Full load into tbl_bronze_weather_raw.
o	Concatenates all frames into all_pdf, enforces column order and types, and creates a Spark DataFrame with an explicit schema (date and metric types).
o	Normalises parish names (upper + trim), rounds coordinates to the same scale as dim_location_silver, drops null parish/date combinations, and removes duplicate (date, parish) pairs.
o	Writes the result as a Delta table tbl_bronze_weather_raw in overwrite mode and executes an OPTIMIZE … ZORDER BY (date) statement for query performance.
This Bronze design ensures that both incidents and weather are stored with full fidelity, auditable provenance, and consistent temporal and spatial frames before any business logic is applied.
SILVER – CLEANING, STANDARDISATION, AND CONFORMANCE
The Silver tier transforms the Bronze tables into conformed dimensions and daily facts. Two pipelines implement this stage: PL_SILVER_INCIDENTS and PL_SILVER_WEATHER.

PL_SILVER_INCIDENTS – Dimensionalisation of incidents
Purpose - Derive the conformed dimensions DimDate, DimLocation, and DimIncidentType, and the daily incident fact table FactIncidentDaily, from the staging incidents table stg_incidents_appended. This directly implements Steps 2–4 of Kimball’s method and supports BN1–BN4 and BN6–BN7.
Pipeline structure. PL_SILVER_INCIDENTS invokes the dataflow DF_Silver_Incidents, which groups four queries into a single logical transformation:
 <img width="945" height="602" alt="image" src="https://github.com/user-attachments/assets/f3a75077-c4cf-408b-ac3b-09afa6e5f4d5" />

Activity Load dim_date – Dataflow DF_Silver_DimDate
•	Reads stg_incidents_appended from the Lakehouse and selects Registration Date.
•	Computes the minimum and maximum dates and generates a continuous list of dates between them with List.Dates.
•	Adds calendar attributes: Year, Month, Day, Trimester (quarter), Semester (“1st Semester” / “2nd Semester”), and Week Day.
•	Constructs the surrogate key sk_date as an integer yyyymmdd.
•	Adds flags Is Last Year and Is Last Year-1 Until Last Period to support time-intelligence logic in the semantic model.
•	Removes duplicates on sk_date to guarantee a unique row per date.
•	Outputs the result as the DimDate table dim_date_silver.
Activity Load_dim_location – Dataflow DF_Silver_DimLocation
•	Reads stg_incidents_appended from the Lakehouse.
•	Types and normalises Parish Name (upper + trim) and the raw coordinates Subsection Latitude and Subsection Longitude.
•	Groups by Parish Name and computes the median of subsection coordinates, yielding Latitude_Centroid and Longitude_Centroid per parish.
•	Rounds both centroid coordinates to a fixed number of decimal places (currently 5) to stabilise downstream joins.
•	Deduplicates and sorts parishes alphabetically to obtain a deterministic ordering.
•	Builds a composite business key bk_location = Parish Name _ Latitude_Centroid _ Longitude_Centroid, representing the unique parish–centroid combination used across facts.
•	Assigns the surrogate key sk_location via an index column, decoupling analytics from any source identifiers.
•	Outputs the result as the DimLocation table dim_location_silver.
Activity Load_incident_type – Dataflow DF_Silver_DimIncidentType 
•	Reads stg_incidents_appended.
•	Keeps Area and Type, with typing and normalisation (upper + trim) to align with reporting practice.
•	Filters out rows where Area or Type are null.
•	Builds a business key bk_incident_type = Area & " - " & Type.
•	Deduplicates on bk_incident_type, sorts by Area and Type, and assigns a surrogate key sk_incident_type via an index column.
•	Outputs the result as the DimIncidentType table dim_incident_type_silver.
 
Activity Load_fact_incident_daily – Dataflow DF_Silver_FactIncidentDaily
•	Reads stg_incidents_appended from the Lakehouse as the event-level incident staging table, and in the same query reads dim_location_silver and dim_incidenttype_silver as lookup dimensions.
•	Types core columns (Registration Date, Parish Name, Area, Type, Subsection Longitude, Subsection Latitude) and normalises the text fields (upper + trim, PT culture) so that natural keys match the dimension tables.
•	Derives the surrogate date key sk_date as an integer yyyymmdd from Registration Date, aligned with dim_date_silver.
•	Joins with dim_location_silver on Parish Name to obtain sk_location, ensuring that each incident is anchored to the parish centroid used in DimLocation.
•	Joins with dim_incidenttype_silver on (Area, Type) to obtain sk_incident_type, using the harmonised incident taxonomy.
•	Casts sk_location and sk_incident_type to integer and enforces referential integrity by keeping only rows where all surrogate keys (sk_date, sk_location, sk_incident_type) are non-null.
•	Drops raw geometry columns (Subsection Longitude, Subsection Latitude), which are now centralised in DimLocation, and removes natural keys that are no longer required for analysis at fact level.
•	Adds an index-based surrogate key sk_fact_incident for event-level auditing and lineage.
•	Derives a composite grain key bk_fact_incident = sk_date _ sk_location _ sk_incident_type, representing the Day × Parish × IncidentType combination to which each event belongs.
•	Outputs the result as the incident fact staging table fact_incident_daily_silver. At this stage, each row corresponds to a single de-normalised incident occurrence; aggregation to the analytical grain (Day × Parish × Incident Type) is performed when constructing the Gold-level fact tables and measures.
This pipeline centralises dimensional conformance, surrogate-key assignment, and basic data-quality checks in Silver, keeping the Gold star schema and semantic model comparatively lightweight. 

PL_SILVER_WEATHER – Conformance of weather to DimDate and DimLocation
Purpose. Transform the Bronze weather table tbl_bronze_weather_raw into a daily weather fact table aligned with dim_date_silver and dim_location_silver. This supports BN2, BN3, and BN5 by enabling weather-aware baselines, anomalies and explanatory analysis.
Pipeline structure. PL_SILVER_WEATHER invokes a single Dataflow:
 <img width="509" height="531" alt="image" src="https://github.com/user-attachments/assets/0af54019-d0bc-49ca-ad0a-51ee6310e468" />

Activity Load fact_weather_daily – Dataflow DF_Silver_FactWeatherDaily
•	Reads tbl_bronze_weather_raw from the Lakehouse.
•	Types date, freg, latitude, longitude, and the daily weather measures (e.g. precipitation_sum, temperature_2m_min, temperature_2m_max, wind_speed_10m_max).
•	Normalises parish names (freg → Parish Name, upper + trim) to match dim_location_silver.
•	Adds sk_date as integer yyyymmdd computed from date.
•	Reads dim_location_silver and joins on Parish Name, Latitude_Centroid, and Longitude_Centroid to retrieve sk_location.
•	Casts sk_location to integer and derives Is_Rainy_Day (1 if precipitation_sum ≥ 1, else 0).
•	Selects only the columns required for analysis, typically:
sk_date, sk_location, date, precipitation_sum, temperature_2m_min, temperature_2m_max, wind_speed_10m_max, Is_Rainy_Day.
•	Outputs the result as the weather fact staging table fact_weather_daily_silver.
Because fact_weather_daily_silver uses the same DimDate and DimLocation keys as the incident facts, weather and incidents are directly comparable at the parish–day level, enabling co-variation analysis and weather-aware anomaly interpretation (BN5 / BQ5.*).

GOLD – DIMENSIONAL STAR SCHEMAS AND SEMANTIC LAYER

PL_GOLD_PUBLISH – STAR-SCHEMA PUBLICATION AND SEMANTIC MODEL REFRESH
Purpose. The Gold tier promotes the cleaned and conformed Silver tables into their final star-schema names and triggers a refresh of the Direct Lake semantic model used by the dashboards. Gold tables preserve the same row-level grain as Silver (one row per incident event in fact_incident_daily; one row per parish–day of weather in fact_weather_daily) but are treated as the stable analytical contract for reporting and for DAX measure definitions.
Pipeline structure. PL_GOLD_PUBLISH is implemented with five Copy data activities, followed by a Wait and a Semantic model refresh activity:
1.	Load_dim_date
2.	Load_dim_location
3.	Load_dim_incident_type
4.	Load_fact_incident_daily
5.	Load_fact_weather_daily
6.	Wait1 (buffer to ensure all copy operations have completed)
7.	Semantic model refresh (Power BI dataset)
Each Copy data activity reads a Silver table from the Lakehouse and overwrites the corresponding Gold table, with explicit column mapping but no further transformation. The semantic-model activity then refreshes the dataset so that dashboards always point to the latest Gold snapshot.
 <img width="540" height="557" alt="image" src="https://github.com/user-attachments/assets/84fc1b9f-3a84-4939-95ab-f74bb62fbece" />
Activity Load_dim_date
•	Source: Lakehouse table dim_date_silver (root folder Tables).
•	Destination_ Lakehouse table dim_date, action Overwrite.
•	Mapping / steps:
o	Performs a 1-to-1 mapping of all calendar attributes:
sk_date, Registration Date, Year, Month, Day, Trimester,
Semester, Is Last Year, Is Last Year-1 Until Last Period, Week Day.
o	No additional calculations are applied in Gold; uniqueness on sk_date and the flags for time-intelligence logic were already enforced in Silver.
•	Output. Gold Date dimension dim_date, used as the shared time axis for both facts.

Activity Load_dim_location
•	Source: dim_location_silver.
•	Destination: dim_location, action Overwrite.
•	Mapping / steps:
o	Copies the surrogate key and descriptive attributes:
sk_location, Parish Name, Latitude_Centroid, Longitude_Centroid,
and the composite business key bk_location.
o	Internal engineering artefacts (e.g., transient fingerprints) are not present at this stage; they were already handled in the Silver dataflow.
•	Output: Gold Location dimension dim_location, shared by incident and weather facts and used for mapping and territorial analyses.

Activity Load_dim_incident_type
•	Source: dim_incidenttype_silver.
•	Destination: dim_incident_type, action Overwrite.
•	Mapping / steps:
o	Copies the surrogate key and harmonised taxonomy attributes:
sk_incident_type, Area, Type, bk_incident_type.
o	Category labels (Area, Type) are already normalised (upper + trim, no duplicates) in Silver, so Gold simply exposes them for the semantic model.
•	Output: Gold Incident Type dimension dim_incident_type, used for thematic breakdowns and anomaly profiles.

Activity Load_fact_incident_daily
•	Source: fact_incident_daily_silver.
•	Destination: fact_incident_daily, action Overwrite.
•	Mapping / steps:
o	Copies the surrogate keys and identifiers:
sk_fact_incident, sk_date, sk_location, sk_incident_type, bk_fact_incident.
o	No aggregation is performed: each row in fact_incident_daily continues to represent a single incident occurrence, with its date, parish and harmonised incident type resolved via the SKs.
o	Referential integrity (non-null keys and successful joins to the dimensions) has been enforced upstream in Silver, so Gold can remain a thin publication layer.
•	Output. Gold incident fact table fact_incident_daily.
Daily workloads (e.g., Incidents per day × parish × incident type) are obtained in the semantic model by DAX measures (e.g., Incidents = COUNTROWS ( fact_incident_daily)
Activity  Load_fact_weather_daily
•	Source: fact_weather_daily_silver.
•	Destination: fact_weather_daily, action Overwrite.
•	Mapping / steps:
o	Copies the surrogate keys and daily weather indicators:
sk_fact_weather, sk_date, sk_location,
temperature_2m_max, temperature_2m_min,
precipitation_sum, wind_speed_10m_max,
Is_Rainy_Day, bk_fact_weather.
o	As with incidents, no aggregation is applied in Gold; each row corresponds to a single parish–day produced by the notebook and conformed in Silver.
•	Output. Gold weather fact table fact_weather_daily, aligned with the same Date and Location dimensions as incidents and ready for joint analyses of workloads and weather.

Activity Wait1
•	Inserts a short wait after all Copy data activities to ensure that the Gold tables have been fully committed before the semantic model refresh is triggered.
•	This avoids race conditions where the dataset might refresh while one of the copies is still in progress.

Activity Semantic model refresh
•	Inputs: Gold tables dim_date, dim_location, dim_incident_type, fact_incident_daily, fact_weather_daily.
•	Steps:
o	Triggers a refresh of the Direct Lake semantic model (e.g. dataset NMRLX) bound to the Gold tables.
o	The dataset defines relationships:
	fact_incident_daily[sk_date] → dim_date[sk_date]
	fact_incident_daily[sk_location] → dim_location[sk_location]
	fact_incident_daily[sk_incident_type]→ dim_incident_type[sk_incident_type]
	fact_weather_daily[sk_date] → dim_date[sk_date]
	fact_weather_daily[sk_location] → dim_location[sk_location]
•	Link to measures. Detailed definitions of baseline, anomaly and comparison measures (BN2–BN5) are provided later in Section 3.6. From the Gold-tier perspective, PL_GOLD_PUBLISH guarantees that these measures operate over clean, conformed star-schema tables with stable keys and a clear analytical grain enforced via the semantic model.
MASTER ORCHESTRATION
To avoid managing multiple independent refreshes, the Medallion pipelines are wrapped by a single orchestration pipeline, PL_MASTER (Figure X). This pipeline is responsible for running all stages of the ELT process in the correct order and thus provides a single scheduling and monitoring point for the whole artefact.

<img width="945" height="210" alt="image" src="https://github.com/user-attachments/assets/cf65fa42-9bb1-454c-b195-604588ef6060" />

The structure of PL_MASTER is deliberately simple and transparent:
•	Activity Invoke PL_BRONZE_INCIDENTS.
Triggers the ingestion and append of Na Minha Rua LX incident files into tbl_bronze_incidents_raw and the derivation of the staging table stg_incidents_appended.
•	Activity Invoke PL_SILVER_INCIDENTS.
Once incident ingestion completes successfully, this activity runs the Silver conformance for incidents, producing dim_date_silver, dim_location_silver, dim_incidenttype_silver, and fact_incident_daily_silver.
•	Activity Invoke PL_BRONZE_WEATHER.
After incident Silver is available (including DimDate and DimLocation), this step executes the notebook-driven extraction of historical weather, landing it as tbl_bronze_weather_raw.
•	Activity Invoke PL_SILVER_WEATHER.
This activity conforms the Bronze weather data to the same DimDate and DimLocation used by incidents, generating fact_weather_daily_silver.
•	Activity Invoke PL_GOLD_PUBLISH.
Finally, the Gold pipeline copies the conforming Silver tables into the Gold star-schema tables (dim_date, dim_location, dim_incident_type, fact_incident_daily, fact_weather_daily) and refreshes the Direct Lake semantic model.
The control flow is strictly sequential: each Invoke pipeline activity only starts after the previous one has succeeded. This design keeps failure diagnosis straightforward (errors are localised to a specific stage) and ensures that downstream tables are never refreshed against partially updated upstream data, which is critical for the integrity of baselines and anomaly measures discussed in Section 3.6.
In operational terms, only PL_MASTER is scheduled; individual bronze, silver, and gold pipelines are treated as reusable building blocks that can also be run ad hoc for debugging or backfill purposes.
