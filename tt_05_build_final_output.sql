CREATE OR REPLACE PROCEDURE `project-name.dataset_name.traffic_trends_report_05_build_final_output`()
BEGIN
-- ################################################################################################################
-- Segment all properties for final output
--################################################################################################################

CREATE OR REPLACE TABLE `project-name.dataset_name.traffic_trends_report_output`
AS 
(
WITH    
  user_segment_01 AS 
  (
    SELECT 
        'user_segment_01' AS segment,      
        country,                    
        view_id,
        property_id,
        ic_site_id,
        site,
        month,
        HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
        IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
        IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
        IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
        IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
      FROM 
        `project-name.dataset_name.temp_ga_base`
      GROUP BY 1,2,3,4,5,6,7                  
  ),

  user_segment_02 AS 
  (
    SELECT 
      'user_segment_02' AS segment,
      country,
      view_id,
      property_id,
      ic_site_id,
      site,
      month,
      count(distinct user_pseudo_id) AS users,
      -- HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
      IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
      IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
    FROM 
      `project-name.dataset_name.temp_ga_base`
    WHERE 
              REGEXP_CONTAINS(campaign,'(?i)string1|string2|string3')
    GROUP BY 1,2,3,4,5,6,7                                                      
  ),                                 
                                    
  user_segment_03 AS 
  (
    SELECT 
      'user_segment_03' AS segment,
      country,
      view_id,
      property_id,
      ic_site_id,
      site,
      month,
      HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
      IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
      IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
    FROM 
      `project-name.dataset_name.temp_ga_base`
    WHERE 
      LOWER(medium) = <medium_param>

    GROUP BY 1,2,3,4,5,6,7
  ),

  user_segment_04 AS 
  (
    SELECT 
      'user_segment_04' AS segment,
      country,
      view_id,
      property_id,
      ic_site_id,
      site,
      month,
      HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
      -- IFNULL(COUNT(DISTINCT user_pseudo_id), 0) AS users,
      IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
      IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
    FROM 
      `project-name.dataset_name.temp_ga_base`
    WHERE 
      (
           campaign LIKE '%syn%' 
        OR REGEXP_CONTAINS(source, '(?i)yahoo|msn|AppleNews|microsoft|flipboard|pocket|smartnews') 
        ) 
      AND (NOT REGEXP_CONTAINS(medium, '(?i)organic|cpc|cpm|Paid|cpa|cpp|cpv|ppv') 
      AND LOWER(source) NOT LIKE '%search%')
    GROUP BY 1,2,3,4,5,6,7
  ),

  user_segment_05 AS 
  (
    SELECT
    'user_segment_05' AS segment,
      country,
      view_id,
      property_id,
      ic_site_id,
      site,
      month,
      HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
      IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
      IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
    FROM 
      `project-name.dataset_name.temp_ga_base`
    WHERE 
      (
         REGEXP_CONTAINS(campaign, '(?i)^ign_') 
      )
    GROUP BY 1,2,3,4,5,6,7
  ),

  user_segment_06 AS 
  (
  SELECT 
      'user_segment_06' AS segment,
      country,
      view_id,
      property_id,
      ic_site_id,
      site,
      month,
      HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
      IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
      IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
    FROM 
      `project-name.dataset_name.temp_ga_base`
    WHERE 
      (
         STARTS_WITH(campaign, 'nl')
      AND REGEXP_CONTAINS(medium, '(?i)email')
      )
    GROUP BY 1,2,3,4,5,6,7
  ),

  user_segment_07 AS 
  (
    SELECT 
      'user_segment_07' AS segment,
      country,
      view_id,
      property_id,
      ic_site_id,
      site,
      month,
      HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
      IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
      IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
    FROM 
      `project-name.dataset_name.temp_ga_base`
    WHERE 1=1
        -- AND REGEXP_CONTAINS(source, '(?i)cpc|cpm|paid')
        AND REGEXP_CONTAINS(campaign,'(?i)membership|print|prd')
        AND NOT REGEXP_CONTAINS(campaign,'(?i)arb_|mgu_|^ign_')
        AND NOT REGEXP_CONTAINS(campaign, '(?i)^ign_') 
    GROUP BY 1,2,3,4,5,6,7
  ),
  
  user_segment_08 AS 
  (
    SELECT 
       'user_segment_08' AS segment,
        country,
        view_id,
        property_id,
        ic_site_id,
        site,
        month,
      HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
        IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
        IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
        IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
        IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
      FROM 
        `project-name.dataset_name.temp_ga_base`
      WHERE 
                NOT REGEXP_CONTAINS(canonical_url,'src=|ref=')
          AND (
                (
                      medium IS NULL
                  and source IS NULL
                  and campaign is NULL
                )
              OR 
                (
                      medium = '(not set)'
                  and source = '(not set)'
                  and campaign = '(not set)'
                )
              )
      GROUP BY 1,2,3,4,5,6,7
  ),

  user_segment_09 AS 
  (
    SELECT 
      'user_segment_09' AS segment,
      country,
      view_id,
      property_id,
      ic_site_id,
      site,
      month,
      HLL_COUNT.EXTRACT(HLL_COUNT.INIT(user_pseudo_id)) AS users,
      IFNULL(SUM(IF(event_name = 'session_start', 1, 0)), 0) AS sessions,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) AS pageviews,
      IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS slide_views,
      IFNULL(SUM(IF(event_name = 'page_view', 1, 0)), 0) + IFNULL(SUM(IF(event_name = 'slide_view', 1, 0)), 0) AS total_views
    FROM 
      `project-name.dataset_name.temp_ga_base`
    WHERE 
      (
           REGEXP_CONTAINS(campaign,r'(?i)trueanth|social|spr|likeshop|tiktoklikeshop')
        OR REGEXP_CONTAINS(source, r'(?i)facebook|^t\.co|twitter|stumble|pinterest|reddit|digg|tumblr|buzzfeed|social|instagram|trueanth|snap|snapchat|tiktok|dash')
      )
    AND NOT REGEXP_CONTAINS(medium, r'(?i)organic|cpc|cpm|paid|cpa|cpp|cpv|ppv')
    AND NOT REGEXP_CONTAINS(campaign,r'(?i)arb|mgu|dda|ign')
    GROUP BY 1,2,3,4,5,6,7
  ),


  create_union AS (
    SELECT * FROM user_segment_01 
    UNION ALL
    
    SELECT * FROM user_segment_02  
    UNION ALL 
    
    SELECT * FROM user_segment_03 
    UNION ALL
    
    SELECT * FROM user_segment_04
    UNION ALL 
    
    SELECT * FROM user_segment_05
    UNION ALL 
    
    SELECT * FROM user_segment_06
    UNION ALL
    
    SELECT * FROM user_segment_07
    UNION ALL
    
    SELECT * FROM user_segment_08
    UNION ALL 
    
    SELECT * FROM user_segment_09
    ),

  -- Create a reference table of all possible values
  -- This will ensure that if a segment has no data it will be included with 0 values
  reference_table AS (
    SELECT DISTINCT 
      view_id,
      country,
      property_id,
      ic_site_id,
      site,
      month,
      segment
    FROM `project-name.dataset_name.temp_ga_base`
    CROSS JOIN (
      SELECT 'user_segment_01' AS segment
      UNION ALL SELECT 'user_segment_02' 
      UNION ALL SELECT 'user_segment_03'
      UNION ALL SELECT 'user_segment_04'
      UNION ALL SELECT 'user_segment_05'
      UNION ALL SELECT 'user_segment_06'
      UNION ALL SELECT 'user_segment_07'
      UNION ALL SELECT 'user_segment_08'
      UNION ALL SELECT 'user_segment_09'
      )
    ),

  -- Assign default values 
  final_query AS (
    SELECT 
      rt.segment,
      rt.country,
      rt.view_id,
      rt.property_id,
      rt.ic_site_id,
      rt.site,
      rt.month,
      IFNULL(cu.users, 0) AS users,
      IFNULL(cu.sessions, 0) AS sessions,
      IFNULL(cu.pageviews, 0) AS pageviews,
      IFNULL(cu.slide_views, 0) AS slide_views,
      IFNULL(cu.total_views, 0) AS total_views
    FROM 
      reference_table rt
    LEFT JOIN 
      create_union cu
      ON 
        rt.segment = cu.segment
      AND rt.country = cu.country
      AND rt.view_id = cu.view_id 
      AND rt.property_id = cu.property_id
      AND rt.ic_site_id = cu.ic_site_id
      AND rt.site = cu.site
      AND rt.month = cu.month
      )

  SELECT *
  FROM final_query
);

END;
