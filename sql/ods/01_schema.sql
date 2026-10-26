-- Схемы слоя ODS и служебной области загрузки (журнал).
-- Идемпотентен: повторный запуск безопасен.

CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS ingestion;