-- DROP DATABASE `mt`;

-- CREATE DATABASE IF NOT EXISTS `mt`;

CREATE TABLE IF NOT EXISTS `matomo`.`dm_visits` (
    `row_created` DateTime DEFAULT now(),
    `idvisit` UInt64,
    `idsite` UInt32,
    `idvisitor` String,
    `visit_last_action_time` DateTime,
    `visit_first_action_time` DateTime
)
ENGINE = MergeTree()
ORDER BY (idvisit, row_created, visit_first_action_time)
PARTITION BY toYYYYMM(visit_first_action_time)
SETTINGS index_granularity = 8192;