CREATE OR REPLACE PROCEDURE `project-name.dataset_name.content_insight_00400_insert_to_history`()
BEGIN

  DECLARE LOAD_DATE DATE;
  DECLARE LOAD_TS TIMESTAMP;

  SET LOAD_DATE = CURRENT_DATE();
  SET LOAD_TS = CURRENT_TIMESTAMP();

  DELETE FROM `project-name.dataset_name.content_insight_history`
  WHERE LOAD_DATE = LOAD_DATE;

  INSERT INTO `project-name.dataset_name.content_insight_history`
    SELECT *
      ,CURRENT_DATE() AS LOAD_DATE  
      ,CURRENT_TIMESTAMP() AS LOAD_TS
    FROM  `project-name.dataset_name.content_insight_00300_output_table`;

END;