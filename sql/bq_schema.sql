CREATE OR REPLACE VIEW `${GCP_PROJECT}.${BQ_DATASET}.v_sessions_daily` AS
SELECT eventDate AS date,
       SUM(sessions) AS sessions,
       SUM(activeUsers) AS active_users,
       SUM(totalUsers)  AS total_users
FROM `${GCP_PROJECT}.${BQ_DATASET}.events_daily_raw`
GROUP BY date;

CREATE OR REPLACE VIEW `${GCP_PROJECT}.${BQ_DATASET}.v_channels_daily` AS
SELECT eventDate AS date,
       sessionDefaultChannelGroup AS channel_group,
       SUM(sessions) AS sessions,
       SUM(conversions) AS conversions,
       SUM(purchaseRevenue) AS revenue
FROM `${GCP_PROJECT}.${BQ_DATASET}.events_daily_raw`
GROUP BY date, channel_group;

CREATE OR REPLACE VIEW `${GCP_PROJECT}.${BQ_DATASET}.v_source_medium_daily` AS
SELECT eventDate AS date,
       sourceMedium,
       SUM(sessions) AS sessions,
       SUM(conversions) AS conversions,
       SUM(purchaseRevenue) AS revenue
FROM `${GCP_PROJECT}.${BQ_DATASET}.events_daily_raw`
GROUP BY date, sourceMedium;
