CREATE OR REPLACE PROCEDURE `project-name.dataset_name.traffic_trends_report_02_build_dataset_table`()
BEGIN
    
    CREATE OR REPLACE TABLE `project-name.dataset_name.dataset_table` 
      AS (
            SELECT 
              name
              ,ga4_project
              ,ga4_property_id as  ga4_property_id
              ,ga4_subproperty_id as ga4_subproperty_id
              ,RANK() OVER(ORDER BY ga4_subproperty_id) AS rownum
            from `project-name.mapping.ga_mapping`
            where 1=1
            -- AND name = <brand_param>
            AND ga4_active = TRUE
            and business_unit = <bu_param>
            ORDER BY rownum
          );

  END;
