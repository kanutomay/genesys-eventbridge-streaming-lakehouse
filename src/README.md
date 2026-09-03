# Source

- **`etl/mappers/`** — AWS Glue Python Shell job. Authenticates to the Genesys Cloud Platform API, paginates through Users and Queues, and writes date-partitioned Parquet to S3 with Glue Catalog integration via AWS Data Wrangler. Runs daily at 06:00 UTC.
- **`transform/<pipeline>/`** — one Kinesis Firehose transformation Lambda per event-driven pipeline. Each decodes the base64 Firehose record, handles Genesys's JSON-Lines-in-a-single-record format, flattens the EventBridge envelope + Genesys event body into a flat schema ready for Parquet, and emits structured JSON logs for CloudWatch-based monitoring.

Each handler reflects a real production transformation pattern, sanitized for publication: account IDs, ARNs, and org identifiers are replaced with synthetic examples, and the embedded local-testing payload at the bottom of each file uses conspicuously fabricated conversation/participant IDs (`test-*-id` strings) and reserved-range (`+1-555-…`) example phone numbers — never real subscriber data. The transformation code no longer logs the caller's ANI value itself to CloudWatch (a presence flag, `has_ani`, is kept for monitoring; the value isn't).
