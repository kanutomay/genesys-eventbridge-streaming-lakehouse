-- voice_mapper_queues_current: latest-snapshot view over the daily Genesys Queues API pull
-- The base table (voice_mapper_queues) is dt-partitioned, one snapshot per day the Mappers
-- Glue job runs; this view exposes only the most recent partition.

CREATE OR REPLACE VIEW "genesys_streaming_lakehouse_curated"."voice_mapper_queues_current" AS
SELECT *
FROM "genesys_streaming_lakehouse_curated"."voice_mapper_queues"
WHERE dt = (
    SELECT MAX(dt)
    FROM "genesys_streaming_lakehouse_curated"."voice_mapper_queues"
);
