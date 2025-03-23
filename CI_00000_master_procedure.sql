CREATE OR REPLACE PROCEDURE `project-name.dataset_name.content_insights_00000_master_procedure`()
BEGIN
  CALL `project-name.dataset_name.content_insights_00050_pre_build_master`();
  CALL `project-name.dataset_name.content_insights_00100_build_master`();
  CALL `project-name.dataset_name.content_insights_00150_content_table`();
  CALL `project-name.dataset_name.content_insights_00300_output_table`();
  CALL `project-name.dataset_name.content_insights_00400_insert_to_history`();
END;
