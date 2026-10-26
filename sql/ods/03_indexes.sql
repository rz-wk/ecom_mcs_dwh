-- Индексы событий. Выполняются строго после завершения бэкфилла
-- (init_ods --stage indexes) и только на готовых данных.
--
-- Почему не до бэкфилла: индексы на партиционированной
-- таблице — partitioned index автоматически наследуются каждой
-- новой партицией, и вставка млн строк замедлится многократно.
-- Скрипт идемпотентен: повторный прогон безвреден.

-- CREATE INDEX IF NOT EXISTS idx_events_batch
--     ON ods.events (_source_file, _batch_date);

CREATE INDEX IF NOT EXISTS idx_events_batch_event_time
    ON ods.events (_source_file, _batch_date, event_time);

CREATE INDEX IF NOT EXISTS idx_events_event_time
    ON ods.events (event_time);

CREATE INDEX IF NOT EXISTS idx_events_user_id
    ON ods.events (user_id);