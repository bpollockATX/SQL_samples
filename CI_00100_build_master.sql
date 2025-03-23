CREATE OR REPLACE PROCEDURE `project-name.dataset_name.content_insights_00100_build_master`()
BEGIN

  CREATE OR REPLACE TABLE `project-name.dataset_name.content_insights_00100_build_master`
  AS (
      SELECT
        date,
        site_content_id,
        ic_site_id,
        campaign,
        traffic_type,
        CASE
          WHEN traffic_type LIKE "string01" THEN (SUM(pageviews))
          ELSE 0
      END
        AS paid_pageviews,
        CASE
          WHEN traffic_type LIKE "string02" THEN (SUM(pageviews))
          ELSE 0
      END
        AS organic_pageviews,
      CASE WHEN traffic_type = "string01" and referral_type = 'referral_type_01' AND AD_TYPE = 'ad_type_01' THEN SUM(programmatic_impressions + adx_impressions) ELSE 0 END AS paid_programmatic_display_impressions,  
      CASE WHEN traffic_type = "string02" and referral_type = 'referral_type_01' AND AD_TYPE = 'ad_type_01' THEN SUM(programmatic_impressions + adx_impressions) ELSE 0 END AS organic_programmatic_display_impressions, 
      CASE WHEN traffic_type = "string01" and referral_type = 'referral_type_01' AND AD_TYPE = 'ad_type_02' THEN SUM(impressions) ELSE 0 END AS paid_programmatic_video_impressions,
      CASE WHEN traffic_type = "string02" and referral_type = 'referral_type_01' AND AD_TYPE = 'ad_type_02' THEN SUM(impressions) ELSE 0 END AS organic_programmatic_video_impressions,
      CASE
          WHEN traffic_type LIKE "string01" THEN (SUM(programmatic_revenue))
          ELSE 0 
      END AS paid_programmatic_display_revenue,
        CASE
          WHEN traffic_type LIKE "string02" THEN (SUM(programmatic_revenue))
          ELSE 0
      END
        AS organic_programmatic_display_revenue,
        CASE
          WHEN traffic_type LIKE "string01" THEN (SUM(video_programmatic_revenue))
          ELSE 0
      END
        AS paid_programmatic_video_revenue,
        CASE
          WHEN traffic_type LIKE "string02" THEN (SUM(video_programmatic_revenue))
          ELSE 0
      END
        AS organic_programmatic_video_revenue,
      CASE WHEN traffic_type = "string01" and referral_type = 'referral_type_02' and AD_TYPE = 'ad_type_01' THEN sum(adx_impressions) ELSE 0 END AS paid_direct_display_impressions,
      CASE WHEN traffic_type = "string02" and referral_type = 'referral_type_02' AND AD_TYPE = 'ad_type_01' THEN sum(impressions_direct) ELSE 0 END AS organic_direct_display_impressions,
      CASE WHEN traffic_type = "string01" and referral_type = 'referral_type_02' and AD_TYPE = 'ad_type_02' THEN sum(video_direct_impressions) ELSE 0 END AS paid_direct_video_impressions,
      CASE WHEN traffic_type = "string02" and referral_type = 'referral_type_02' and AD_TYPE = 'ad_type_02' THEN sum(video_direct_impressions) ELSE 0 END AS organic_direct_video_impressions,
      CASE WHEN traffic_type LIKE "string01" THEN (SUM(direct_revenue)) ELSE 0 END AS paid_direct_display_revenue,
      CASE WHEN traffic_type LIKE "string02" THEN (SUM(direct_revenue)) ELSE 0 END AS organic_direct_display_revenue,
      CASE WHEN traffic_type LIKE "string01" THEN (SUM(video_direct_revenue)) ELSE 0 END AS paid_direct_video_revenue,
      CASE WHEN traffic_type LIKE "string02" THEN (SUM(video_direct_revenue)) ELSE 0 END AS organic_direct_video_revenue,
      CASE WHEN traffic_type LIKE "string01" THEN SUM(publisher_commission) ELSE 0 END AS paid_total_affiliate_commission,
      CASE WHEN traffic_type LIKE "string02" THEN SUM(publisher_commission) ELSE 0 END AS organic_total_affiliate_commission,
        SUM(impressions) AS impressions,
        SUM(promotion_views) AS promotion_views,
        SUM(promotion_clicks) AS promotion_clicks,
        SUM(promotion_transactions) AS promotion_transactions,
        SUM(promotion_revenue) AS promotion_revenue,
        -- referral_type,
        -- ad_type
    FROM `project-name.dataset_name.content_insights_00050_pre_build_master`
    GROUP BY       
        date,
        site_content_id,
        ic_site_id,
        campaign,
        traffic_type,
        referral_type,
        ad_type
  );
END;