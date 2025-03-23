CREATE OR REPLACE PROCEDURE `project-name.dataset_name.traffic_trends_report_00_master_procedure`()
BEGIN
  --step 1: compiles functions
    CALL `project-name.dataset_name.traffic_trends_report_01_compile_functions`();
  
  --step 2: Build datatset table 
    CALL `project-name.dataset_name.traffic_trends_report_02_build_dataset_table`();
  
  --Step 3: Build session + temp ga tables
    CALL `project-name.dataset_name.traffic_trends_report_03_build_source_tables`();

  --Step 4: Loop over the data
    CALL `project-name.dataset_name.traffic_trends_report_04_loop_property_specific_events`();

  --Step 5: Build final CTE
    CALL `project-name.dataset_name.traffic_trends_report_05_build_final_output`();
  -- Step 6: Insert monthly data into history table
    CALL `project-name.dataset_name.traffic_trends_report_06_insert_to_hist`();
END;
