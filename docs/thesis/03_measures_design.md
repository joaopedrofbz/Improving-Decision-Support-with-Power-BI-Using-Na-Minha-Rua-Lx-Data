Measures Design
The semantic model in Power BI encapsulates the analytical logic of the artefact: all indicators shown in the dashboards are implemented as DAX measures over the conformed star schema. Rather than embedding ad-hoc calculations in visuals, the model exposes a small, well-structured set of measures organised into families: core workload, time intelligence, seasonal baselines, anomalies, territorial pressure, and weather sensitivity. This section summarises these families and explains how they operationalise the business needs BN1–BN5 while remaining transparent and reusable for BN6–BN7.

3.6.1 Core workload measures
At the centre of the model is the base workload measure:
	Incidents – counts rows in fact_incident_daily in the current filter context. At the star-schema grain, each row corresponds to a single incident occurrence linked to Date, Location and IncidentType. This measure underpins all pages and directly supports BN1 (standardised visibility of workloads) because it provides a unique, governed definition of “number of incidents”.
A complementary measure is defined for descriptive statistics:
	Avg Incidents – computes the average of [Incidents] across the set of dates in the current context. It is used mainly in validation and exploratory views, but also serves as a reference in interpretation of workload levels.
Both measures are placed in the Incidents\Core folder, signalling that they are semantic building blocks used by other, more derived indicators.

3.6.2 Time-intelligence measures
To support comparisons across years and periods (BN1, BN2), the model defines a small set of time-intelligence measures using the dim_date[Registration Date] column and standard DAX functions:
	Incidents YTD – cumulative incidents from the start of the current year up to the last date in context, via DATESYTD.
	Incidents (LY) – incidents over the corresponding period in the previous year, via SAMEPERIODLASTYEAR.
	Incidents YoY % – year-on-year percentage change, computed as "YoY"=("Incidents" -"Incidents (LY)" )/"Incidents (LY)"  and exposed as a percentage.
These measures feed the homepage KPI cards and are reused on thematic pages, ensuring that all references to “YTD workload” or “year-on-year variation” share exactly the same definition.

3.6.3 Seasonal baseline measures
BN2 requires that incident counts be interpreted against simple, auditable seasonal baselines. The semantic model implements two such baselines and their variability:
	Incidents Baseline Daily – a 28-day trailing average excluding the current day. For each date, the measure computes the average of [Incidents] over the previous 28 days using DATESINPERIOD. It provides an interpretable daily reference level that reflects recent behaviour.
	Incidents Baseline Daily StdDev – the population standard deviation of [Incidents] over the same 28-day window, used to normalise anomalies at daily grain.
	Incidents Baseline Monthly – a 12-month rolling average that excludes the current month. The measure first identifies the last complete month, constructs the set of dates covering the previous twelve complete months, summarises them into monthly totals, and averages those totals. This yields a robust “typical month” reference.
	Incidents Baseline Monthly StdDev – the population standard deviation of monthly incidents over the same twelve complete months.
To avoid exposing this complexity directly in visuals, an adaptive wrapper is defined:
	Incidents Baseline (Adaptive) – if the date context is at day level (ISINSCOPE( dim_date[Registration Date])), returns the daily baseline; if it is at month level or higher, returns the monthly baseline.
	Incidents Baseline StdDev (Adaptive) – analogous adaptive logic for the standard deviation.
Finally, a relative deviation measure is defined:
	Incidents vs Baseline % (Daily) – percentage difference between [Incidents] and [Incidents Baseline Daily], used in tables and conditional formatting.
These measures drive the “Seasonality & Baselines” page, where they provide explicit answers to BQ2.1–BQ2.4.

3.6.4 Anomaly measures
BN3 focuses on early identification of abnormal surges and hotspots. On the temporal axis, anomalies are implemented through z-scores and binary flags:
	Incidents Anomaly Z-Score Daily –
z_"daily" =("Incidents" -"Incidents Baseline Daily" )/"Incidents Baseline Daily StdDev" 

	Incidents Anomaly Z-Score Monthly – analogous, using monthly baselines and standard deviations.
An adaptive version selects the appropriate z-score based on the grain of the date hierarchy:
	Incidents Anomaly Z-Score (Adaptive) – returns the daily z-score at day level and the monthly z-score at month and above.
To move from continuous scores to actionable signals, threshold-based flags are defined:
	Incidents Anomaly Flag (Daily) – returns 1 when the daily z-score is not blank, the baseline is positive, and z≥2; otherwise 0.
	Incidents Anomaly Flag (Monthly) – same logic at monthly level.
	Incidents Anomaly Flag (Adaptive) – chooses the appropriate flag based on date grain.
Two aggregate anomaly indicators support higher-level monitoring:
	Anomaly Days Count – counts, within the current context, how many dates have daily anomaly flag = 1.
	Anomaly Days % – proportion of dates with anomalies among all distinct reporting dates, exposed as a percentage (kept hidden in the field list but used in cards and tables).
For interpretability, a mean z-score is also available:
	Avg Anomaly Z-Score (Daily) – average of the daily z-score across dates in context, providing a synthetic indicator of how extreme recent workloads have been.
These measures underpin the “Anomalies & Hotspots” page, where anomaly bars are coloured by z-score, hotspot tables are sorted by anomaly magnitude, and managers can explicitly see which days and parishes cross the operational threshold.

3.6.5 Territorial pressure measures
BN4 is concerned with prioritisation under capacity constraints. Rather than raw counts alone, the model introduces a relative pressure indicator:
	Avg Incidents per Parish – computes total incidents in the current context divided by the distinct number of parishes (locations), using ALL(dim_location) to obtain a city-wide average.
	Pressure Index – defined as
"Pressure Index"="Incidents" /"Avg Incidents per Parish" so that values above 1 indicate parishes whose workload is above the city average, and values below 1 indicate lighter-pressure territories.
These measures are used in the “Territorial Prioritisation” (BN4) page to rank parishes and combinations of parish–area, and to narrate equity and prioritisation scenarios in Chapter 5.

3.6.6 Weather sensitivity measures
Weather-related measures reside in fact_weather_daily and support BN5 by quantifying whether rainy conditions are associated with higher incident workloads.
First, the model splits total workload into rainy and dry parish-days:
	Incidents (Rainy) – incidents on parish-days where Is_Rainy_Day = 1, implemented by constructing the set of (sk_date, sk_location) pairs from fact_weather_daily with rain and propagating them to fact_incident_daily via TREATAS.
	Incidents (Dry) – analogous for Is_Rainy_Day = 0.
	Incidents (Total Rainy+Dry) – sum of the two components.
The denominators are:
	Parish-Days (Rainy) – count of (date, parish) pairs with Is_Rainy_Day = 1.
	Parish-Days (Dry) – count of (date, parish) pairs with Is_Rainy_Day = 0.
From these, the model derives per-parish-day incident rates:
	Incidents per Parish-Day (Rainy) – rainy incidents divided by rainy parish-days.
	Incidents per Parish-Day (Dry) – dry incidents divided by dry parish-days.
Two interpretable sensitivity indicators are then defined:
	Rainy Lift –
"Rainy Lift"="Incidents per Parish-Day (Rainy)" /"Incidents per Parish-Day (Dry)" Values greater than 1 indicate higher workloads on rainy days; values below 1 indicate the opposite.
	Rainy Share % – share of incidents that occur on rainy parish-days out of total rainy+dry incidents.
Finally, a categorical label simplifies communication:
	Rainy Sensitivity Category – maps the lift into discrete classes (“High”, “Neutral”, “Lower on rainy days”, etc.), used for colour encoding in the Weather Context page.
The raw weather variable Precipitation (sum of precipitation_sum) is also exposed to support joint charts of incidents and rainfall.

3.6.7 Transparency and reuse
To support BN6 (traceability) and BN7 (self-service exploration), all measures follow a consistent naming convention and are grouped into meaningful display folders (Incidents\Core, Incidents\Baseline, Incidents\Anomalies, Incidents\Territory, Weather\Rain vs Dry, Weather\Sensitivity). More complex indicators are composed exclusively from simpler measures rather than directly from columns, making their logic auditable in a small number of steps. Hidden measures (such as standard deviations and anomaly percentages) are still documented in the model but not exposed directly to casual users, keeping the field list approachable while preserving full methodological transparency for analysts and for the dissertation’s evaluation.
