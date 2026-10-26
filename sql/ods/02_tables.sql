-- События. Партиционирование по месяцам event_time.
-- Партиции месяцев создаёт загрузчик непосредственно перед COPY дня,
-- Пересоздание с нуля разовое:
-- DROP TABLE IF EXISTS ods.events CASCADE;
CREATE TABLE IF NOT EXISTS ods.events (
    event_time      TIMESTAMPTZ   NOT NULL,
    event_type      VARCHAR(16)   NOT NULL,
    product_id      BIGINT        NOT NULL,
    category_id     BIGINT        NOT NULL,
    category_code   TEXT,
    brand           TEXT,
    price           NUMERIC(12,2) NOT NULL,
    user_id         BIGINT        NOT NULL,
    user_session    UUID,

    _ingested_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    _source_file    TEXT          NOT NULL,
    _batch_date     DATE          NOT NULL
) PARTITION BY RANGE (event_time);

-- Журнал выполнения загрузок. Единственный источник истины о состоянии
-- batch: проверка реелоада и точка возобновления после сбоя.
-- TODO(масштабирование): при мультиисточнике добавить
-- колонку table_name в PK — тривиальный ALTER TABLE, блокеров нет.
CREATE TABLE IF NOT EXISTS ingestion.journal (
    source_file     TEXT        NOT NULL,
    batch_date      DATE        NOT NULL,
    status          TEXT        NOT NULL,   -- running | loaded | failed
    rows_loaded     BIGINT,
    rows_rejected   BIGINT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    error           TEXT,

    PRIMARY KEY (source_file, batch_date)
);

-- Отключение autovacuum на период бэкфилла выполняется операционно
-- на каждой партиции сразу после её создания загрузчиком:
--     ALTER TABLE <partition> SET (autovacuum_enabled = false);
-- и включается обратно на этапе построения индексов.