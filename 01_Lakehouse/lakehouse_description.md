# Lakehouse – Na Minha Rua LX

## Bronze layer

- **tbl_bronze_incidents_raw**
  - Source: Na Minha Rua LX Excel exports (2020–2024)
  - Grain: 1 row = 1 raw incident record (as delivered)
  - Key columns: dt_registo, Freguesia, area, tipo, Subseccao, Longitude_Subseccao, Latitude_Subseccao

- **tbl_bronze_weather_raw**
  - Source: Open-Meteo daily archive API
  - Grain: 1 row = 1 day × parish (freguesia)
  - Key columns: date, freguesia, latitude, longitude, precipitation_sum, temperature_2m_min, temperature_2m_max, wind_speed_10m_max

## Silver layer

- **dim_date_silver**
  - Grain: 1 row = 1 calendar date
  - Keys: sk_date
  - Main attributes: Registration Date, Year, Semester, Trimester, Month, Day, Week Day, Is Last Year, Is Last Year-1 Until Last Period

- **dim_location_silver**
  - Grain: 1 row = 1 parish
  - Keys: sk_location, bk_location
  - Attributes: Parish Name, Latitude_Centroid, Longitude_Centroid

- **dim_incident_type_silver**
  - Grain: 1 row = 1 incident type
  - Keys: sk_incident_type, bk_incident_type
  - Attributes: Area, Type

- **fact_incident_daily_silver**
  - Grain: Day × Parish × IncidentType (event-level before aggregation)
  - Keys: sk_fact_incident, sk_date, sk_location, sk_incident_type

- **fact_weather_daily_silver**
  - Grain: Day × Parish
  - Keys: sk_fact_weather, sk_date, sk_location
  - Measures: precipitation_sum, temperature_2m_min, temperature_2m_max, wind_speed_10m_max, Is_Rainy_Day

## Gold layer

- **dim_date**
- **dim_location**
- **dim_incident_type**
- **fact_incident_daily**
- **fact_weather_daily**

_All Gold tables mirror the Silver schema but act as the stable contract for the semantic model (Direct Lake)._
