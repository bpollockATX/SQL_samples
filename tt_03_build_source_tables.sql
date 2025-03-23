CREATE OR REPLACE PROCEDURE `project-name.dataset_name.traffic_trends_report_03_build_source_tables`()
BEGIN
-- ################################################################################################################
-- Build Session Start Table
--################################################################################################################

  CREATE OR REPLACE TABLE `project-name.dataset_name.session_start` 
  (
    event_date          STRING
    ,user_pseudo_id     STRING
    ,ga_session_id      INTEGER
    ,landing_page_path  STRING
    ,prev_ga_session_id INTEGER
    ,source             STRING
    ,medium             STRING 
    ,campaign           STRING
    );

  CREATE OR REPLACE TABLE `project-name.dataset_name.temp_ga_base`(
    event_date               STRING        
    ,event_name               STRING        
    ,view_id                  INTEGER       
    ,ic_site_id               INTEGER       
    ,site                     STRING        
    ,property_id              STRING        
    ,locale                   STRING        
    ,user_pseudo_id           STRING        
    ,ga_session_id            INTEGER       
    ,canonical_url            STRING        
    ,page_path                STRING        
    ,source             STRING
    ,medium             STRING 
    ,campaign           STRING
    ,month                    STRING
    ,country                  STRING
  );

END;
