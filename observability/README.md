# Observability

`dashboard.json` (a CloudWatch dashboard definition — import via `aws cloudwatch put-dashboard --dashboard-name genesys-eventbridge-streaming-lakehouse --dashboard-body file://dashboard.json`) renders a 21-widget operations view across all 8 pipelines (the 7 event-driven pipelines, grouped by domain — Transcription & Attributes, User/Customer/Flow, ACD — plus the batch Mappers pipeline). The 6 standard event-driven pipelines (`acd-end`, `attributes`, `customer-end`, `flow-end`, `flow-outcome`, `user-end`) each carry the same 5 alarms:

- **Data freshness** — most recent event age vs. SLA (target: <30 min)
- **Lambda error rate** — absolute error count over the period, target: 0
- **Lambda error percentage** — errors as a % of invocations, target: <1%
- **Lambda duration** — target: <25s
- **DLQ depth** — EventBridge delivery-failure queue, target: 0

`transcription` carries 9: the same data freshness / error-rate / error-percentage / duration alarms, each doubled for its two Firehose outputs (sessions, utterances), plus one shared DLQ depth alarm. `mappers` has no Lambda or EventBridge-delivery DLQ to alarm on — it gets its own Glue job status and execution-time dashboard widgets instead (failures surfaced via the job's state-change events, see [`docs/architecture/mappers-pattern.md`](../docs/architecture/mappers-pattern.md)), but does carry 2 CloudWatch alarms of its own: data freshness on each of the users/queues reference tables.

**41 alarms total** across the platform (6 × 5 + 9 + 2), each wired to a shared SNS topic for formatted alerting. The dashboard JSON uses example account/resource identifiers (`123456789012`, `ACME`) — swap in your own account ID and operating-company code before importing, or leave them as-is if you're using this as a starting template.
