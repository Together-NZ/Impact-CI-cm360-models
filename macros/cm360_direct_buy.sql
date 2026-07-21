{% macro cm360_direct_buy(source_name, table_name,lower_advertiser_name) %}
WITH cm360reference AS (
    SELECT
        JSON_VALUE(JSON_EXTRACT(data, "$.placementId")) AS placement_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.advertiser")) AS advertiser,
        JSON_VALUE(JSON_EXTRACT(data, "$.creativeType")) AS creative_type,
        JSON_VALUE(JSON_EXTRACT(data, "$.creativeId")) AS creative_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.creative")) AS creative_name,
        JSON_VALUE(JSON_EXTRACT(data, "$.dv360Creative")) AS dv360_creative_name,
        JSON_VALUE(JSON_EXTRACT(data, "$.dv360CreativeId")) AS dv360_creative_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.dv360LineItem")) AS dv360_line_item,
        JSON_VALUE(JSON_EXTRACT(data, "$.dv360LineItemId")) AS dv360_line_item_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.advertiserId")) AS advertiser_id,
        safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.campaignEndDate"))) AS campaign_end_date,
        JSON_VALUE(JSON_EXTRACT(data, "$.campaignId")) AS campaign_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.campaign")) AS campaign_name,
        safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.campaignStartDate"))) AS campaign_start_date,
        JSON_VALUE(JSON_EXTRACT(data, "$.clickThroughUrl")) AS click_through_url,
        safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.date"))) AS date,
        JSON_VALUE(JSON_EXTRACT(data, "$.placementCostStructure")) AS placement_cost_structure,
        safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.placementEndDate"))) AS placement_end_date,
        JSON_VALUE(JSON_EXTRACT(data, "$.placement")) AS placement,
        JSON_VALUE(JSON_EXTRACT(data, "$.packageRoadblockId")) AS package_roadblock_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.packageRoadblock")) AS package_roadblock,
        JSON_VALUE(JSON_EXTRACT(data, "$.placementSize")) AS placement_size,
        safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.placementStartDate"))) AS placement_start_date,
        JSON_VALUE(JSON_EXTRACT(data, "$.placementStrategy")) AS placement_strategy,
        JSON_VALUE(JSON_EXTRACT(data, "$.siteKeyname")) AS site_keyname,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.clicks")) AS INT64) AS clicks,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.impressions")) AS INT64) AS impressions,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.dv360Cost")) AS FLOAT64) AS dv360_cost,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.totalConversions")) AS FLOAT64) AS total_conversions,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.richMediaVideoFirstQuartileCompletes")) AS INT64) AS video_25_completion,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.richMediaVideoMidpoints")) AS INT64) AS video_50_completion,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.richMediaVideoThirdQuartileCompletes")) AS INT64) AS video_75_completion,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.richMediaVideoCompletions")) AS INT64) AS video_completion,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.richMediaVideoPlays")) AS INT64) AS video_plays,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.richMediaTrueViewViews")) AS INT64) AS video_views,
        JSON_VALUE(JSON_EXTRACT(data, "$.site")) AS site_name,
        ROW_NUMBER() OVER (
            PARTITION BY 
                JSON_VALUE(JSON_EXTRACT(data, "$.placementId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.advertiser")),
                JSON_VALUE(JSON_EXTRACT(data, "$.creativeType")),
                JSON_VALUE(JSON_EXTRACT(data, "$.creativeId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.creative")),
                JSON_VALUE(JSON_EXTRACT(data, "$.advertiserId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.dv360CreativeId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.dv360Creative")),
                JSON_VALUE(JSON_EXTRACT(data, "$.dv360LineItem")),
                JSON_VALUE(JSON_EXTRACT(data, "$.dv360LineItemId")),
                safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.campaignEndDate"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.campaignId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.campaign")),
                safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.campaignStartDate"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.clickThroughUrl")),
                safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.date"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.placementCostStructure")),
                safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.placementEndDate"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.placement")),
                JSON_VALUE(JSON_EXTRACT(data, "$.packageRoadblockId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.packageRoadblock")),
                JSON_VALUE(JSON_EXTRACT(data, "$.placementSize")),
                safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.placementStartDate"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.placementStrategy")),
                JSON_VALUE(JSON_EXTRACT(data, "$.siteKeyname"))
            ORDER BY 
                safe.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.date"))) DESC
        ) AS row_num
    FROM 
        {{ source(source_name, table_name) }}
    WHERE 
        LOWER(JSON_VALUE(JSON_EXTRACT(data, "$.site"))) NOT IN ('the trade desk', 'ttd', 'facebook', 'meta', 'dv360', 'dv_360', 'twitch', 'programmatic', 'dart', 'google ads', 'sem')
       AND LOWER(JSON_VALUE(JSON_EXTRACT(data, "$.advertiser")))  LIKE '%{{lower_advertiser_name}}%')

SELECT *,
    CASE 
        WHEN LOWER(site_name) LIKE '%nzme%' THEN 'Nzme'
        WHEN LOWER(site_name) LIKE '%spotify%' THEN 'Spotify'
        WHEN LOWER(site_name) LIKE '%tiktok%' THEN 'Tiktok'
        WHEN LOWER(site_name) LIKE '%youtube%' THEN 'Youtube'
        ELSE INITCAP(SPLIT(site_name, ' ')[OFFSET(0)])
    END AS publisher,
    CASE 
        WHEN ARRAY_LENGTH(SPLIT(placement, '_')) >= 5 THEN SPLIT(placement, '_')[OFFSET(4)]
        ELSE 'Other'
    END AS audience_name,
    CASE 
        WHEN 
            (LOWER(site_name) NOT LIKE '%dv360%' 
            OR LOWER(site_name) NOT LIKE '%dv_360%'
            OR LOWER(site_name) NOT LIKE '%ttd%'
            OR LOWER(site_name) NOT LIKE '%trade%'
            OR LOWER(site_name) NOT LIKE '%dart%'
            OR LOWER(site_name) NOT LIKE '%programmatic%'
            OR LOWER(site_name) NOT LIKE '%adobe%'
            OR LOWER(site_name) NOT LIKE '%outbrain%'
            OR LOWER(site_name) NOT LIKE '%version%')
            AND SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%display%' 
        THEN 'High Impact Display'
        WHEN 
            SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%display%'
            AND (
                SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%homepage%'
                OR SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%pagedown%'
                OR SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%mobile%'
                OR SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%hpto%'
                OR SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%interstitial%'
                OR SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%pushdown%'
                OR SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%direct%'
                OR SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%interscroller%'
                OR SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%static%'
            )
        THEN 'High Impact Display'
        WHEN 
            SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%social%' 
            AND LOWER(campaign_name) NOT LIKE '%vid%' 
            AND LOWER(creative_name) NOT LIKE '%vid%' 
            AND LOWER(placement) NOT LIKE '%vid%' 
            AND LOWER(click_through_url) NOT LIKE '%vid%' 
        THEN 'Social Display'
        WHEN 
            SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)] LIKE '%social%' 
            AND (
                LOWER(campaign_name) LIKE '%vid%' 
                OR LOWER(creative_name) LIKE '%vid%' 
                OR LOWER(placement) LIKE '%vid%' 
                OR LOWER(click_through_url) LIKE '%vid%'
            )
        THEN 'Social Video'
        ELSE SPLIT(REGEXP_EXTRACT(click_through_url, r'utm_medium=([^&]+)'),'-')[SAFE_OFFSET(1)]
    END AS media_format,
    CASE 
        WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) >= 6 THEN SPLIT(creative_name, '_')[SAFE_OFFSET(5)]
        ELSE 'Other'
    END AS ad_format_detail,
    SPLIT(creative_name, '_')[SAFE_OFFSET(6)] AS ad_format,
    SPLIT(creative_name, '_')[SAFE_OFFSET(7)] AS creative_descr,
    SPLIT(campaign_name, '_')[SAFE_OFFSET(2)] AS campaign_descr,
    0 AS media_cost
FROM cm360reference
WHERE row_num = 1

{% endmacro %}
