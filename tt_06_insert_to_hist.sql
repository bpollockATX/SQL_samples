CREATE OR REPLACE PROCEDURE `project-name.dataset_name.traffic_trends_report_06_insert_to_hist`()
BEGIN
  DELETE FROM `project-name.dataset_name.traffic_trends_report_full_history`
  WHERE update_date = current_date;

  INSERT INTO `project-name.dataset_name.traffic_trends_report_full_history`
  SELECT 
      map.ga4_dataset as ga4_dataset
      ,tt.site as site_name
      ,tt.month as month
      ,tt.segment as segment
      ,tt.users
      ,tt.sessions
      ,tt.pageviews
      ,tt.slide_views
      ,tt.total_views
      ,CURRENT_DATE AS update_date
    FROM 
      `project-name.dataset_name.traffic_trends_report_monthly_output` tt
    LEFT JOIN 
      `project-name.mapping.ga_mapping` map 
      ON tt.property_id = map.property_id
  ;
END;
