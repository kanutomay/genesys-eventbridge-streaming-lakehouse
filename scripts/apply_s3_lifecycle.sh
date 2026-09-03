#!/bin/bash
set -euo pipefail

# Apply S3 Lifecycle Policy for all Genesys Voice Interactions data
# Applies the lifecycle configuration in ../infra/s3-lifecycle/policy.json to the platform's data bucket.
# Scope: genesys_voice_interactions/prod/ (all subdirectories)
#
# IMPORTANT: s3api put-bucket-lifecycle-configuration REPLACES the bucket's *entire* lifecycle
# configuration -- it does not merge with or append to whatever rules already exist. If the bucket
# has other lifecycle rules (for other prefixes, other teams, other data), a naive `aws s3api
# put-bucket-lifecycle-configuration --lifecycle-configuration file://policy.json` call silently
# deletes them. This script backs up whatever's currently configured, shows you what it's about to
# overwrite, and asks for explicit confirmation before touching anything.
#
# Usage:
#   ./apply_s3_lifecycle.sh                 # interactive: prompts for confirmation
#   ./apply_s3_lifecycle.sh --yes           # non-interactive: skip the confirmation prompt (CI use)
#   DATA_BUCKET_NAME=my-bucket ./apply_s3_lifecycle.sh
#
# Required:
#   DATA_BUCKET_NAME  -- no default on purpose. You must explicitly name the bucket you intend to
#                        modify; a plausible-looking default here is exactly how someone
#                        accidentally overwrites the wrong bucket's lifecycle rules.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLICY_FILE="$SCRIPT_DIR/../infra/s3-lifecycle/policy.json"
REGION="${AWS_REGION:-us-east-1}"
AUTO_YES=false

for arg in "$@"; do
  case "$arg" in
    --yes|-y) AUTO_YES=true ;;
  esac
done

if [ -z "${DATA_BUCKET_NAME:-}" ]; then
  echo "❌ DATA_BUCKET_NAME is not set. This script will not guess a bucket for you." >&2
  echo "   Set it explicitly:  DATA_BUCKET_NAME=your-actual-bucket ./apply_s3_lifecycle.sh" >&2
  exit 1
fi
BUCKET="$DATA_BUCKET_NAME"

echo "================================================================================"
echo "  S3 Lifecycle Policy — Genesys Voice Interactions Data"
echo "================================================================================"
echo ""

# --- Verify identity/target before touching anything -------------------------------------------
CALLER_IDENTITY="$(aws sts get-caller-identity --output json 2>&1)" || {
  echo "❌ Could not resolve AWS caller identity. Check your credentials/profile." >&2
  echo "$CALLER_IDENTITY" >&2
  exit 1
}
ACCOUNT_ID="$(echo "$CALLER_IDENTITY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Account"])' 2>/dev/null || echo "?")"
CALLER_ARN="$(echo "$CALLER_IDENTITY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Arn"])' 2>/dev/null || echo "?")"

echo "🎯 Target:"
echo "   Bucket:  $BUCKET"
echo "   Region:  $REGION"
echo "   Account: $ACCOUNT_ID"
echo "   Caller:  $CALLER_ARN"
echo ""
echo "   ⚠️  Double-check the account/bucket above before continuing — this is not a dry run."
echo ""

# --- Back up whatever lifecycle configuration already exists, before changing anything ----------
BACKUP_DIR="$SCRIPT_DIR/../.lifecycle-backups"
mkdir -p "$BACKUP_DIR"
BACKUP_FILE="$BACKUP_DIR/${BUCKET}-lifecycle-backup-$(date -u +%Y%m%dT%H%M%SZ).json"

echo "🔍 Reading the bucket's current lifecycle configuration (if any)..."
if aws s3api get-bucket-lifecycle-configuration --bucket "$BUCKET" --region "$REGION" \
     --output json > "$BACKUP_FILE" 2>/tmp/lifecycle-get-err.txt; then
  EXISTING_RULE_COUNT="$(python3 -c 'import json; print(len(json.load(open("'"$BACKUP_FILE"'")).get("Rules", [])))' 2>/dev/null || echo "?")"
  echo "   Found $EXISTING_RULE_COUNT existing rule(s). Backed up to:"
  echo "   $BACKUP_FILE"
  echo ""
  echo "   Existing rule IDs:"
  python3 -c 'import json; [print("     -", r.get("ID", "(unnamed)")) for r in json.load(open("'"$BACKUP_FILE"'")).get("Rules", [])]' 2>/dev/null
  echo ""
  if [ "${EXISTING_RULE_COUNT:-0}" != "0" ]; then
    echo "   ⚠️  put-bucket-lifecycle-configuration REPLACES the entire configuration above."
    echo "      If any of those rules aren't meant to be replaced by this policy, stop now, merge"
    echo "      them into $POLICY_FILE yourself (or edit the applied config afterward), and re-run."
    echo ""
  fi
else
  if grep -q "NoSuchLifecycleConfiguration" /tmp/lifecycle-get-err.txt 2>/dev/null; then
    echo "   No existing lifecycle configuration on this bucket. Nothing to back up or lose."
    rm -f "$BACKUP_FILE"
  else
    echo "❌ Could not read the bucket's current lifecycle configuration:" >&2
    cat /tmp/lifecycle-get-err.txt >&2
    exit 1
  fi
fi
rm -f /tmp/lifecycle-get-err.txt

# --- Confirm before applying ---------------------------------------------------------------------
if [ "$AUTO_YES" != true ]; then
  read -r -p "Apply $POLICY_FILE to s3://$BUCKET now, replacing the configuration above? [y/N] " REPLY
  case "$REPLY" in
    [yY][eE][sS]|[yY]) ;;
    *) echo "Aborted — no changes made."; exit 0 ;;
  esac
fi

# --- Apply -----------------------------------------------------------------------------------------
aws s3api put-bucket-lifecycle-configuration \
  --bucket "$BUCKET" \
  --lifecycle-configuration "file://$POLICY_FILE" \
  --region "$REGION"

echo "✅ Lifecycle policy applied successfully!"
echo ""
echo "================================================================================"
echo "  📊 LIFECYCLE SUMMARY"
echo "================================================================================"
echo ""
echo "📁 SCOPE: genesys_voice_interactions/prod/* (ALL DATA)"
echo "   • Transcription data (sessions + utterances)"
echo "   • Transformed data"
echo "   • Raw backup data"
echo "   • Error data"
echo "   • All future data under this path"
echo ""
echo "📅 RETENTION POLICY - 84 MONTHS (7 YEARS):"
echo "   ├─ 0-12 months (0-365 days):      ☁️  Standard Storage           (Athena: instant)"
echo "   ├─ 12-24 months (365-730 days):   📦 Standard-IA                 (Athena: instant)"
echo "   ├─ 24-60 months (730-1825 days): ❄️  Glacier Instant Retrieval   (Athena: instant, no restore)"
echo "   ├─ 60-84 months (1825-2555 days):🧊 Glacier Deep Archive        (Athena: NOT direct -- requires"
echo "   │                                                                 s3:RestoreObject first, 12-48h)"
echo "   └─ 84+ months (2555+ days):      🗑️  DELETE (irreversible)"
echo ""
echo "🧹 CLEANUP RULES:"
echo "   ├─ Noncurrent versions:          🗑️  Deleted after 90 days"
echo "   └─ Incomplete uploads:           🗑️  Aborted after 7 days"
echo ""
echo "ℹ️  NOTE: Glacier Instant Retrieval stays instantly Athena-queryable, same as Standard/Standard-IA."
echo "   Glacier Deep Archive does NOT -- an object there must be explicitly restored"
echo "   (aws s3api restore-object) and the restore must complete (12-48h depending on retrieval"
echo "   tier) before Athena can read it. Treat that range as 'retrievable on request for audit /"
echo "   legal-hold purposes', not as background-BI-queryable."
echo ""
echo "================================================================================"
echo ""

# Verify the policy was applied
echo "🔍 Verifying lifecycle configuration..."
echo ""
aws s3api get-bucket-lifecycle-configuration \
  --bucket "$BUCKET" \
  --region "$REGION" \
  --query 'Rules[?contains(Id, `GenesysVoice`)].{ID:Id,Status:Status,Prefix:Filter.Prefix}' \
  --output table

echo ""
echo "✅ Lifecycle policy verification complete!"
echo ""
echo "To view the full lifecycle configuration, run:"
echo "  aws s3api get-bucket-lifecycle-configuration --bucket $BUCKET --region $REGION"
echo ""
echo "The pre-change configuration (if any existed) is saved at:"
echo "  $BACKUP_FILE"
echo ""
