CREATE OR REPLACE PROCEDURE `project-name.dataset_name.traffic_trends_report_04_loop_property_specific_events`()
OPTIONS (strict_mode=false)
BEGIN
      DECLARE base_date date;
      DECLARE start_date date;
      DECLARE end_date date;
      DECLARE x INT64 DEFAULT 1;
      DECLARE z INT64 DEFAULT 0;
      DECLARE tablequery string;
      DECLARE regex string;
      DECLARE sql_session_start string;
      DECLARE sql_temp_ga_base string;
      DECLARE table_suffix_start int64;
      DECLARE table_suffix_end int64;
      DECLARE last_click_cutoff_date int64;
      
      SET base_date = CURRENT_DATE();
      SET start_date = (SELECT DATE_SUB(DATE_TRUNC(base_date, MONTH), INTERVAL 1 DAY));
      SET end_date = (SELECT DATE_SUB(DATE_TRUNC(base_date, MONTH), INTERVAL 1 MONTH));
      SET table_suffix_start = start_date;
      SET table_suffix_end = end_date;
      SET last_click_cutoff_date = 20240716;  --before 7/16 use collected_traffic_source. After (>=) 7/16, we use session_traffic_source_last_click
      SET regex = 'REGEXP_EXTRACT(REGEXP_REPLACE(`project-name.dataset_name.EventParamVal`(event_params, "page_location").string_value ,r"^[^/]*//[^/]*",""), r"^([^?]+)??")';
      SET z= (SELECT COUNT(1) FROM `project-name.dataset_name.dataset_table`);

      TRUNCATE TABLE `project-name.dataset_name.session_start`;
      TRUNCATE TABLE `project-name.dataset_name.temp_ga_base`;

      WHILE x<=z DO
      
          SET tablequery =  (SELECT CONCAT(ga4_project, '.analytics_',ga4_subproperty_id) FROM `project-name.dataset_name.dataset_table` WHERE rownum = x);
          --################################################################################################################
          --  Insert property-specific session start events into the session_starts table
          --################################################################################################################
          SET sql_session_start = 'INSERT INTO `project-name.dataset_name.session_start` (event_date,user_pseudo_id,ga_session_id,source,medium,campaign,landing_page_path,prev_ga_session_id)  SELECT DISTINCT event_date,user_pseudo_id,`project-name.dataset_name.EventParamVal`(event_params, "ga_session_id").int_value AS ga_session_id,IF(event_date >= "'||last_click_cutoff_date||'", session_traffic_source_last_click.manual_campaign.source,collected_traffic_source.manual_source) as source, IF(event_date >= "'||last_click_cutoff_date||'",session_traffic_source_last_click.manual_campaign.medium, collected_traffic_source.manual_medium) as medium, IF(event_date >= "'||last_click_cutoff_date||'",session_traffic_source_last_click.manual_campaign.campaign_name, collected_traffic_source.manual_campaign_name) as campaign,'||regex|| ' AS landing_page_path,LAG(`project-name.dataset_name.EventParamVal`(event_params, "ga_session_id").int_value) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS prev_ga_session_id FROM `'||tablequery||'.events_*` WHERE event_name = "session_start" AND _table_suffix BETWEEN "'||table_suffix_start||'" AND "'||table_suffix_end||'"; ';
          
          --################################################################################################################
          -- Step 5: JOIN back to property-specific events tables and insert records into temp_ga_base 
          --################################################################################################################
                SET sql_temp_ga_base = 'INSERT INTO project-name.dataset_name.temp_ga_base SELECT GA.event_date AS date, event_name, map.ga4_subproperty_id AS view_id, map.cid AS ic_site_id, `project-name.dataset_name.EventParamVal`(GA.event_params, "siteName").string_value AS site, map.property_id AS property_id, `project-name.dataset_name.EventParamVal`(GA.event_params, "locale").string_value AS locale,GA.user_pseudo_id, `project-name.dataset_name.EventParamVal`(event_params, "ga_session_id").int_value AS ga_session_id, `project-name.dataset_name.EventParamVal`(event_params, "canonicalUrl").string_value AS canonical_url, '||regex|| ' as page_path, source, medium, campaign, CONCAT(FORMAT_DATE("%B", PARSE_DATE("%Y%m%d", MIN(_table_suffix) OVER ()))," ",EXTRACT(YEAR FROM PARSE_DATE("%Y%m%d", MAX(_table_suffix) OVER ()))) AS month, CASE WHEN geo.country = "United States" THEN "US" ELSE "Intl" END AS country FROM `'||tablequery||'.events_*` GA  JOIN project-name.dataset_name.session_start AS ss ON ss.event_date = GA.event_date AND ss.user_pseudo_id = GA.user_pseudo_id AND ss.ga_session_id = `project-name.dataset_name.EventParamVal`(GA.event_params, "ga_session_id").int_value LEFT JOIN UNNEST(items) AS items JOIN `project-name.mapping.ga_mapping` map on `project-name.dataset_name.EventParamVal`(GA.event_params, "siteName").string_value = map.name and map.bu = "HMG" and map.ga4_active = true WHERE _table_suffix BETWEEN "'||table_suffix_start||'" AND "'||table_suffix_end||'";';
           
            EXECUTE IMMEDIATE sql_session_start;
            EXECUTE IMMEDIATE sql_temp_ga_base;

        SET x=x+1;

      END WHILE;
END;
