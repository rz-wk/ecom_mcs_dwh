CREATE TABLE IF NOT EXISTS ecom_mcs_dwh_core.fact_events
(
    event_id        UInt64,

    event_time      DateTime64(3, 'UTC'),
    event_type      LowCardinality(String),

    product_id      UInt32,
    category_id     UInt64,
    category_code   LowCardinality(Nullable(String)),
    brand           LowCardinality(Nullable(String)),
    price           Decimal(12, 2),

    user_id         UInt32,
    user_session    Nullable(UUID),

    event_date      Date,

    _source_file    LowCardinality(String),
    _batch_date     Date,
    _ingested_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(event_date)
PRIMARY KEY (user_id, event_time)
ORDER BY (
    user_id,
    event_time,
    product_id,
    event_type,
    event_id
);