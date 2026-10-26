-- Возврат autovacuum на всех партициях после бэкфилла.
DO $$ DECLARE part record;
BEGIN
    FOR part IN
        SELECT c.relname FROM pg_catalog.pg_inherits i
        JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
        WHERE inhparent = 'ods.events'::regclass
    LOOP
        EXECUTE format(
            'ALTER TABLE ods.%I SET (autovacuum_enabled = true)',
            part.relname
        );
    END LOOP;
END $$;