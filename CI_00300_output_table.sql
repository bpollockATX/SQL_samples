CREATE OR REPLACE PROCEDURE `project-name.dataset_name.content_insights_00300_output_table`()
BEGIN

  CREATE OR REPLACE TABLE `project-name.dataset_name.content_insights_00300_output_table`
  AS
  (
    SELECT
      summary_table.date AS pageview_date,
      CAST( TIMESTAMP(content_table.update_date) AS DATE ) AS published_from,
      CAST( TIMESTAMP(content_table.original_published_date) AS DATE ) AS original_published_date,
      '2000-01-01' AS last_modified_date,
      content_table.author_byline AS author,
      content_table.title AS title,
      site_table.name AS brand,
      content_table.section AS section,
      content_table.subsection AS subsection,
      content_table.display_type AS display_type,
      content_table.content_type AS content_type,
      content_table.content_id AS content_id,
      content_table.url AS url,
      content_table.embed_only AS embed_only,
      content_table.editorial_source AS editorial_source,
      content_table.status AS status,
      CASE
        WHEN ( 
                (NET.REG_DOMAIN(content_table.url) = NET.REG_DOMAIN(content_table.full_url)) 
              ) 
            AND ((DATE(content_table.update_date)) = (DATE(content_table.original_published_date))) THEN 'New'
        WHEN (DATE(content_table.update_date) = DATE(content_table.original_published_date)) THEN 'Updated'
        WHEN (
                (content_table.full_url IS NOT NULL) OR (NET.REG_DOMAIN(content_table.url) != NET.REG_DOMAIN(content_table.full_url))
              )  THEN 'Syndicated'
        ELSE 'Other'
    END
      AS content_classification,
      CASE
        WHEN (content_table.full_url IS NULL) OR ( NET.REG_DOMAIN(content_table.url) = NET.REG_DOMAIN(content_table.full_url) ) THEN 'Yes'
        ELSE 'No'
    END
      AS content_is_original,
      SUM(summary_table.paid_pageviews) AS paid_pageviews,
      SUM(summary_table.organic_pageviews) AS organic_pageviews,
      COALESCE( SUM( summary_table.paid_display_impressions ), 0 ) AS paid_display_impressions,
      COALESCE( SUM( summary_table.organic_programmatic_display_impressions ), 0 ) AS organic_programmatic_display_impressions,
      COALESCE( SUM( summary_table.paid_video_impressions ), 0 ) AS paid_video_impressions,
      COALESCE( SUM( summary_table.organic_programmatic_preroll_impressions ), 0 ) AS organic_programmatic_preroll_impressions,
      COALESCE( SUM( summary_table.paid_display_revenue ), 0 ) AS paid_display_revenue,
      COALESCE( SUM( summary_table.organic_programmatic_display_revenue ), 0 ) AS organic_programmatic_display_revenue,
      COALESCE( SUM( summary_table.paid_video_revenue ), 0 ) AS paid_video_revenue,
      COALESCE( SUM( summary_table.organic_programmatic_preroll_revenue ), 0 ) AS organic_programmatic_preroll_revenue,
      COALESCE( SUM( summary_table.paid_direct_display_impressions ), 0 ) AS paid_direct_display_impressions,
      COALESCE( SUM( summary_table.organic_direct_display_impressions ), 0 ) AS organic_direct_display_impressions,
      COALESCE( SUM( summary_table.paid_direct_video_impressions ), 0 ) AS paid_direct_video_impressions,
      COALESCE( SUM( summary_table.organic_direct_video_impressions ), 0 ) AS organic_direct_video_impressions,
      COALESCE( SUM( summary_table.paid_direct_display_revenue ), 0 ) AS paid_direct_display_revenue,
      COALESCE( SUM( summary_table.organic_direct_display_revenue ), 0 ) AS organic_direct_display_revenue,
      COALESCE( SUM( summary_table.paid_direct_video_revenue ), 0 ) AS paid_direct_video_revenue,
      COALESCE( SUM( summary_table.organic_direct_video_revenue ), 0 ) AS organic_direct_video_revenue,
      SUM(paid_affiliate_commission) AS paid_affiliate_commission,
      SUM(organic_affiliate_commission) AS organic_affiliate_commission,
      COUNT(DISTINCT(content_table.rover_content_id)) AS count_of_content,
      SUM(impressions) AS all_impressions,
      SUM(views) AS views,
      SUM(clicks) AS clicks,
      SUM(transactions) AS transactions,
      SUM(revenue) AS revenue
    FROM
    `project-name.dataset_name.content_insights_00100_build_master` summary_table
    LEFT JOIN
    `project-name.dataset_name.content_insights_00150_content_table` content_table
    ON
      summary_table.site_content_id = content_table.site_content_id
    LEFT JOIN
      `project-name.dataset_name.content_insights_00150_site_table` site_table
    ON
      summary_table.site_id = site_table.cid
    WHERE
      ( 
        summary_table.site_id NOT IN (12345, 678910)
      )
        )
      GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18
END;