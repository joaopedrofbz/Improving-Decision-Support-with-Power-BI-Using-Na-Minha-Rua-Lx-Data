# 99_MASTER — PL_MASTER Pipeline (Import Template)

This folder contains the **exported template (`.zip`)** of the **`PL_MASTER`** pipeline, which orchestrates the end-to-end execution of the Medallion architecture (**Bronze → Silver → Gold**) for the dissertation artefact.

In the project design, `PL_MASTER` coordinates the sequential run of the stage pipelines:
1. `PL_BRONZE_INCIDENTS`
2. `PL_SILVER_INCIDENTS`
3. `PL_BRONZE_WEATHER`
4. `PL_SILVER_WEATHER`
5. `PL_GOLD_PUBLISH`
