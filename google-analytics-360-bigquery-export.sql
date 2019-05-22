#standardSQL
with ga_data as (
  SELECT
    CONCAT(SAFE_CAST(visitId as STRING), ".", clientId) as session_id, -- because visit IDs aren't unique
    IF(totals.bounces=1, CONCAT(SAFE_CAST(visitId as STRING), '.', clientId), Null) as bounced_session_id,
    visitNumber as visit_number,
    visitId as visit_id,
    clientId as client_id,
    fullVisitorId as visitor_id,

    hits.hitNumber as hit_number,
    hits.type as hit_type,
    FORMAT_TIMESTAMP("%c", TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime + hits.time/1000 as INT64)), "GMT") as hit_date_time, -- https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#supported-format-elements-for-timestamp
    SAFE_CAST(visitStartTime + hits.time/1000 as INT64) as hit_timestamp,
    hits.time / 1000 as time,
    hits.hour as hour,
    hits.minute as minute,

    hits.page.pagePath as page_path,
    hits.page.hostname as hostname,
    hits.eventInfo.eventCategory as event_category,
    hits.eventInfo.eventAction as event_action,
    hits.eventInfo.eventLabel as event_label,
    hits.eventInfo.eventValue as event_value,

    trafficSource.campaign as campaign,
    trafficSource.source as source,
    trafficSource.medium as medium,
    CONCAT(trafficSource.source, " / ", trafficSource.medium) as source_medium,
    CONCAT(trafficSource.source, " / ", trafficSource.medium, " / ", trafficSource.campaign) as source_medium_campaign,
    trafficSource.isTrueDirect as is_true_direct,
    trafficSource.keyword as keyword,
    hits.referer as referrer,
    trafficSource.referralPath as referral_path,
    channelGrouping as default_channel_grouping,
    CASE
      WHEN channelGrouping="Social"
        THEN "Social"
      WHEN REGEXP_CONTAINS(trafficSource.medium, r"Paid Social|social_ads") OR channelGrouping="Paid Social"
        THEN "Paid Social"
      WHEN REGEXP_CONTAINS(trafficSource.medium, r"webmail|crm|sms|email") OR channelGrouping="CRM"
        THEN "CRM"
      WHEN trafficSource.medium="organic" OR channelGrouping="Organic Search"
        THEN "Organic Search"
      WHEN trafficSource.medium="cpc" OR channelGrouping="Paid Search"
        THEN "Paid Search"
      WHEN REGEXP_CONTAINS(trafficSource.medium, r"affiliation|affiliates") OR channelGrouping="Affiliates"
        THEN "Affiliates"
      WHEN REGEXP_CONTAINS(trafficSource.medium, r"display|video|vod|cpm") OR channelGrouping="Display"
        THEN "Display"
      WHEN (trafficSource.source="(direct)" AND trafficSource.medium="(none)") OR channelGrouping="Direct"
        THEN "Direct"
      WHEN trafficSource.medium="referral" OR channelGrouping="Referral"
        THEN "Referral"
      ELSE "(Other)"
      END as custom_channel_grouping,

    geoNetwork.country as country,
    CASE
      WHEN geoNetwork.country in ("United States", "Canada") 
        THEN geoNetwork.country
      ELSE "ROW"
      END as country_with_row, -- country from the provided list, otherwise "ROW"
    device.language as language,
    geoNetwork.continent as continent,
    geoNetwork.region as region,
    geoNetwork.metro as metro,
    geoNetwork.city as city,
    geoNetwork.latitude as latitude,
    geoNetwork.longitude as longitude,
    geoNetwork.networkDomain as network_domain,
    geoNetwork.networkLocation as network_location,
    device.deviceCategory as device,
    device.operatingSystem as os,
    device.operatingSystemVersion as os_version,
    device.browser as browser,
    device.browserVersion as browser_version,
    device.browserSize as browser_size,
    device.screenResolution as screen_resolution,

    ( SELECT experimentVariant FROM UNNEST(hits.experiment) WHERE experimentId="example" ) as experiment_variant,

    ( SELECT value FROM UNNEST(t.customDimensions) WHERE index=1 ) as custom_dimension_1_session_scope,
    ( SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1 ) as custom_dimension_1_hit_scope,

    -- custom dimension 1 (session scope) but only if it matches the provided regex:
    ( SELECT value FROM UNNEST(t.customDimensions) WHERE index=1 and regexp_contains(value, r"^\d+$") ) as custom_dimension_1_with_validation,

    ( SELECT value FROM UNNEST(t.customMetric) WHERE index=1 ) as custom_metric_1_session_scope,
    ( SELECT value FROM UNNEST(hits.customMetric) WHERE index=1 ) as custom_metric_1_hit_scope,

    -- escalates labels from "example" event actions to the session level:
    ( SELECT eventInfo.eventLabel FROM UNNEST(t.hits) WHERE eventInfo.eventAction like "example" limit 1 ) as escalate_hit_level_to_session_level,

    date

  FROM
    `project.dataset.ga_sessions_*` as t, UNNEST(hits) as hits

  WHERE
    _TABLE_SUFFIX BETWEEN "20190101" AND "20191231"

    -- only returns sessions containing a certain event:
    AND ( SELECT hitNumber FROM UNNEST(t.hits) WHERE eventInfo.eventAction like "example" limit 1 ) is not null 
)

SELECT * FROM ga_data ORDER BY session_id, hit_number
