CREATE TABLE logs.log
(
    `origin` String,
    `level` String,
    `msg` String,
    `time` String,
    `date` Date DEFAULT toDate(now(),'Asia/Shanghai')
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY time
SETTINGS index_granularity = 8192