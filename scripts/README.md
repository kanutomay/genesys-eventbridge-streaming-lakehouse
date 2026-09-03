# Scripts

`apply_s3_lifecycle.sh` applies the shared lifecycle policy in `../infra/s3-lifecycle/policy.json` to the platform's S3 bucket, via `aws s3api put-bucket-lifecycle-configuration`.

**`put-bucket-lifecycle-configuration` replaces a bucket's entire lifecycle configuration — it does not merge.** This isn't a routine, no-consequence step: run it against the wrong bucket, or a bucket with other lifecycle rules you didn't mean to touch, and those rules are gone. The script accounts for that:

- `DATA_BUCKET_NAME` is **required**, with no default — you must name the exact bucket you intend to modify.
- Before changing anything, it prints the resolved AWS account/caller identity and the target bucket/region so you can visually confirm it's pointed at the right place.
- It reads and backs up whatever lifecycle configuration already exists on the bucket (to `../.lifecycle-backups/`, gitignored) before touching anything, and lists the existing rule IDs so you can see what's about to be replaced.
- It asks for interactive confirmation before applying, unless you pass `--yes` (for CI/non-interactive use).

```bash
DATA_BUCKET_NAME=your-actual-bucket ./apply_s3_lifecycle.sh          # interactive
DATA_BUCKET_NAME=your-actual-bucket ./apply_s3_lifecycle.sh --yes    # non-interactive
```

One correction from the policy's own inline documentation: Glacier Instant Retrieval stays instantly Athena-queryable, but **Glacier Deep Archive does not** — an object there must be explicitly restored (`aws s3api restore-object`, 12–48 hours depending on retrieval tier) before Athena can read it. The 60–84 month tier is retrievable on request for audit/legal-hold purposes, not queryable by a background BI job the way everything above it is.
