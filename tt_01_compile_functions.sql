CREATE OR REPLACE PROCEDURE `project-name.dataset_name.traffic_trends_report_01_compile_functions`()
BEGIN
    -- ########################################################
    -- Step 1: Create Temp Functions
    --########################################################
    CREATE OR REPLACE FUNCTION `project-name.dataset_name.UserPropertiesVal`(
      record_column ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64>>>,
      targetKey STRING
    )
    RETURNS STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64> AS (
      (SELECT AS STRUCT
        MAX(IF(key = targetKey, value.string_value, NULL)) AS string_value,
        MAX(IF(key = targetKey, value.int_value, NULL)) AS int_value,
        MAX(IF(key = targetKey, value.float_value, NULL)) AS float_value,
        MAX(IF(key = targetKey, value.double_value, NULL)) AS double_value,
        MAX(IF(key = targetKey, value.set_timestamp_micros, NULL)) AS set_timestamp_micros
      FROM UNNEST(record_column))
    );

    -- Temp function for parsing the EventParamVal struct
    CREATE OR REPLACE FUNCTION `project-name.dataset_name.EventParamVal`(
      eventParams ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>,
      targetKey STRING
    )
    RETURNS STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64> AS (
      (SELECT AS STRUCT
        MAX(IF(key = targetKey, value.string_value, NULL)) AS string_value,
        MAX(IF(key = targetKey, value.int_value, NULL)) AS int_value,
        MAX(IF(key = targetKey, value.float_value, NULL)) AS float_value,
        MAX(IF(key = targetKey, value.double_value, NULL)) AS double_value
      FROM UNNEST(eventParams))
    );
END;
