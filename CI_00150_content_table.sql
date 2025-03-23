CREATE OR REPLACE PROCEDURE `project-name.dataset_name.content_insights_00150_content_table`()
BEGIN

  CREATE OR REPLACE TABLE `project-name.dataset_name.content_insights_00150_content_table`
      AS 
      (
        SELECT
          datetime_published,
          original_published_date,
          author_byline,
          title,
          brand_identifier,
          section,
          subsection,
          display_type,
          content_type,
          site_content_id,
          rover_content_id,
          url,
          embed_only,
          canonical,
          status,
          editorial_source,
          SPLIT(site_content_id, '|') [SAFE_OFFSET(ARRAY_LENGTH(SPLIT(site_content_id, '|')) - 1)] AS content_id
        FROM
          `project-name.dataset_name.content_metadata_table` a
        WHERE
          a.cid_country = 'US'
          AND a.business_unit = "business_unit"
          AND IFNULL(a.brand_identifier, '') NOT IN ('site name 1','site name 2') 
      );

  CREATE OR REPLACE TABLE `project-name.dataset_name.content_insights_00150_site_table`
      AS 
      (
        SELECT
          DISTINCT name,
          cid
        FROM
          `project-name.dataset_name.ga_site_mapping_table` 
      );
END;