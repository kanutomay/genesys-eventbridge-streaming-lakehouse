# Infrastructure (CloudFormation)

Each subfolder holds one deployable CloudFormation stack, following a shared shape:

`AWS::Events::Rule` (+ `AWS::Events::Archive` for replay, + `AWS::SQS::Queue` as the rule's own delivery-failure DLQ) → `AWS::KinesisFirehose::DeliveryStream` → `AWS::Lambda::Function` (transform) → S3 prefixes (raw / transformed / errors) → `AWS::Glue::Database` + `AWS::Glue::Table` → `AWS::LakeFormation::Permissions`, with `AWS::CloudWatch::Alarm` (freshness, error rate, duration, DLQ depth) watching across the stack and publishing to `AWS::SNS::Topic` (alerts) — note the DLQ only catches events EventBridge fails to deliver to Firehose, not Lambda/Firehose processing failures, which land in the S3 error prefix instead.

The `mappers` stack is the exception — it has no EventBridge rule, since Genesys does not publish a topic for user/queue reference data; it deploys a scheduled Glue Python Shell job instead.

| Folder | Pipeline |
|---|---|
| `mappers/` | Users & Queues reference data (daily API pull) |
| `acd-end/` | Queue routing outcomes |
| `attributes/` | Conversation attributes & customer identifiers |
| `customer-end/` | Customer journey completion |
| `flow-end/` | IVR flow execution tracking |
| `flow-outcome/` | IVR flow decisions & outcomes |
| `transcription/` | Voice-to-text sessions & utterances |
| `user-end/` | Agent session tracking |
| `s3-lifecycle/` | Bucket-wide lifecycle policy (Standard → Glacier Deep Archive → expire) |

Each `template.yaml` reflects a real production infrastructure pattern, sanitized for publication: account IDs, ARNs, and internal bucket/resource names are replaced with CloudFormation parameters, and the two Lake Formation grants specific to the original deployer's SSO setup are generalized into `DataLakeAdminRoleArn` / `DataLakeWorkloadRoleArn` parameters. The bucket-name parameters (`DataBucketName`/`TargetS3Bucket`, `LambdaCodeS3Bucket`/`GlueCodeS3Bucket`) deliberately have **no default** — they're account-specific resources you must own and supply yourself, not something safe to leave pointed at an example value. See [Security Considerations & Prerequisites](../README.md#security-considerations--prerequisites) in the root README for what's expected to already be true of that bucket (public access block, TLS-only policy, KMS encryption), and for the optional `DLQKmsKeyArn` parameter each event-pipeline stack exposes for customer-managed encryption on its dead-letter queue.
