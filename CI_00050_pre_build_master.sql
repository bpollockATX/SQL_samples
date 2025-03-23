CREATE OR REPLACE PROCEDURE `project-name.dataset_name.content_insights_00050_pre_build_master`()
BEGIN

  DECLARE base_date date;
  DECLARE start_date date;
  DECLARE end_date date;

  SET base_date = CURRENT_DATE();
  SET start_date = (SELECT DATE_SUB(DATE_TRUNC(base_date, MONTH), INTERVAL 1 DAY));
  SET end_date = (SELECT DATE_SUB(DATE_TRUNC(base_date, MONTH), INTERVAL 1 MONTH));

  CREATE OR REPLACE TABLE `project-name.dataset_name.content_insights_00050_pre_build_master`
  AS (
  --======================================================================
  -- Pull pageview data
  --======================================================================
          SELECT
            date,
            COALESCE(site_content_id, 'unknown') AS site_content_id,
            COALESCE(ic_site_id, 0) AS ic_site_id,
            campaign,
            CASE
              WHEN campaign LIKE "%string01%" OR campaign LIKE "%string_02%" THEN "segment_name"
              ELSE "default_traffic_string"
          END
            AS traffic_type,
            SUM(pageviews) AS pageviews,
            0 AS publisher_commission,
            0 AS programmatic_impressions,
            0 AS video_programmatic_impressions,
            0 AS programmatic_revenue,
            0 AS video_programmatic_revenue,
            0 AS direct_impressions,
            0 AS video_direct_impressions,
            0 AS direct_revenue,
            0 AS video_direct_revenue,
            0 AS impressions,
            0 AS promotion_views,
            0 AS promotion_clicks,
            0 AS promotion_transactions,
            0 AS promotion_revenue,
            CAST(NULL AS STRING) AS referral_type,
            CAST(NULL AS STRING) AS ad_type,
            NULL AS impressions_prog,
            NULL AS impressions_adx,
            NULL AS impressions_direct,
            CAST(NULL AS STRING) AS advertiser_name
          FROM
            `project-name.dataset_name.pageviews_table`
          WHERE
            date BETWEEN start_date AND end_date 
          GROUP BY
            1,
            2,
            3,
            4,
            5 

  --======================================================================
  -- Pull GAM Aggregate data
  --======================================================================

  UNION ALL
          SELECT
            CAST( FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP(hour)) AS date ) AS date,
            COALESCE(site_content_id, 'unknown') AS site_content_id,
            COALESCE(ic_site_id, 0) AS ic_site_id,
            COALESCE(utm_campaign, '(not set)') AS campaign,
            CASE
              WHEN utm_campaign LIKE "%string01%" OR utm_campaign LIKE "%string02%" OR src LIKE "%string01%" OR src LIKE "%string02%" THEN "segment_name"
              ELSE "default_traffic_string"
          END
            AS traffic_type,
            0 AS pageviews,
            0 AS publisher_commission,
            CASE
              WHEN ( video_id IS NULL AND ad_unit_name_bottom_level NOT LIKE 'video%' ) THEN impressions_prog + impressions_adx
              ELSE 0
          END
            AS programmatic_impressions,
            CASE
              WHEN ( video_id IS NOT NULL OR ad_unit_name_bottom_level LIKE 'video%' ) AND ( advertiser_name NOT LIKE '%string03%' OR advertiser_name IS NULL ) AND ( advertiser_name NOT LIKE '%string04%' OR advertiser_name IS NULL ) THEN impressions
              ELSE 0
          END
            AS video_programmatic_impressions,
            CASE
              WHEN ( video_id IS NULL AND ad_unit_name_bottom_level NOT LIKE 'video%' ) THEN revenue_prog + revenue_adx
              ELSE 0
          END
            AS programmatic_revenue,
            CASE
              WHEN ( video_id IS NOT NULL OR ad_unit_name_bottom_level LIKE 'video%' ) AND ( advertiser_name NOT LIKE '%string03%' OR advertiser_name IS NULL ) AND ( advertiser_name NOT LIKE '%string04%' OR advertiser_name IS NULL ) THEN revenue
              ELSE 0
          END
            AS video_programmatic_revenue,
            CASE
              WHEN ( video_id IS NULL AND ad_unit_name_bottom_level NOT LIKE 'video%' ) THEN impressions_direct
              ELSE 0
          END
            AS direct_impressions,
            CASE
              WHEN ( video_id IS NOT NULL OR ad_unit_name_bottom_level LIKE 'video%' ) AND advertiser_name LIKE '%string03%' THEN impressions
              ELSE 0
          END
            AS video_direct_impressions,
            CASE
              WHEN ( video_id IS NULL AND ad_unit_name_bottom_level NOT LIKE 'video%' ) THEN revenue_direct
              ELSE 0
          END
            AS direct_revenue,
            CASE
              WHEN ( video_id IS NOT NULL OR ad_unit_name_bottom_level LIKE 'video%' ) AND advertiser_name LIKE '%string03%' THEN revenue
              ELSE 0
          END
            AS video_direct_revenue,
            impressions,
            0 AS promotion_views,
            0 AS promotion_clicks,
            0 AS promotion_transactions,
            0 AS promotion_revenue,
            CAST(NULL AS STRING) AS referral_type,
            CAST(NULL AS STRING) AS ad_type,
            NULL AS impressions_prog,
            NULL AS impressions_adx,
            NULL AS impressions_direct,
            CAST(NULL AS STRING) AS advertiser_name
          FROM
            `project-name.dataset_table.revenue_by_campaign_url_daily_table`
          WHERE
            date BETWEEN start_date AND end_date 
            AND ad_unit_name_top_level LIKE "%top_level_string%"
            AND line_item_id <> 123456789


  --======================================================================
  -- Pull Ecomm Aggregate data
  --======================================================================

  UNION ALL
          SELECT
            commerce_date AS date,
            COALESCE(site_content_id, 'unknown') AS site_content_id,
            COALESCE(ic_site_id, 0) AS ic_site_id,
            COALESCE(utm_campaign, '(not set)') AS campaign,
            CASE
              WHEN utm_campaign LIKE "%string01%" OR utm_campaign LIKE "%string02%" THEN "Arb"
              ELSE "default_traffic_string"
          END
            AS traffic_type,
            0 AS pageviews,
            SUM(commissions) AS publisher_commission,
            0 AS programmatic_impressions,
            0 AS video_programmatic_impressions,
            0 AS programmatic_revenue,
            0 AS video_programmatic_revenue,
            0 AS direct_impressions,
            0 AS video_direct_impressions,
            0 AS direct_revenue,
            0 AS video_direct_revenue,
            0 AS impressions,
            0 AS promotion_views,
            0 AS promotion_clicks,
            0 AS promotion_transactions,
            0 AS promotion_revenue,
            CAST(NULL AS STRING) AS referral_type,
            CAST(NULL AS STRING) AS ad_type,
            NULL AS impressions_prog,
            NULL AS impressions_adx,
            NULL AS impressions_direct,
            CAST(NULL AS STRING) AS advertiser_name
          FROM
            `project-name.dataset_name.affiliate_commerce_table`
          WHERE
            commerce_date BETWEEN start_date AND end_date 
          GROUP BY
            1,
            2,
            3,
            4,
            5

  --======================================================================
  -- Pull Membership data
  --======================================================================

  UNION ALL
          SELECT
            date,
            COALESCE(internal_promotion_site_content_id, 'unknown') AS site_content_id,
            COALESCE(ic_site_id, 0) AS ic_site_id,
            campaign,
            NULL AS traffic_type,
            0 AS pageviews,
            0 AS publisher_commission,
            0 AS programmatic_impressions,
            0 AS video_programmatic_impressions,
            0 AS programmatic_revenue,
            0 AS video_programmatic_revenue,
            0 AS direct_impressions,
            0 AS video_direct_impressions,
            0 AS direct_revenue,
            0 AS video_direct_revenue,
            0 AS impressions,
            SUM(internal_promotion_views) AS promotion_views,
            SUM(internal_promotion_clicks) AS promotion_clicks,
            SUM(transactions) AS promotion_transactions,
            SUM(revenue) AS promotion_revenue,
            CAST(NULL AS STRING) AS referral_type,
            CAST(NULL AS STRING) AS ad_type,
            NULL AS impressions_prog,
            NULL AS impressions_adx,
            NULL AS impressions_direct,
            CAST(NULL AS STRING) AS advertiser_name
          FROM
            `project-name.dataset_name.promotions_table`
          WHERE
            date BETWEEN start_date AND end_date  
          GROUP BY
            1,
            2,
            3,
            4
  );
  WHERE referral_type IS NULL
  ;
END;
