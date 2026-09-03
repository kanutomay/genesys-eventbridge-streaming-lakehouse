# Name: genesys_mappers_glue_job.py
# Revision: 1.0.0
# Author: Mario May
# Date: 2025-12-18
# Description: AWS Glue Python Shell job that fetches users and queues from Genesys Cloud API and writes date-partitioned Parquet files to S3 for analytical queries.
# Change log:
# v1.0.0 - 2025-12-18 by Mario May:
# - Initial release - migrated from Lambda to Glue Python Shell
# - Combined users and queues mappers into single job
# - Writes Parquet format with date partitioning (dt=YYYY-MM-DD)
# - Uses AWS Data Wrangler (comes pre-installed in Glue Python Shell 3.9+)

"""
Runtime settings: AWS Glue Python Shell 3.9
Job Type: Python Shell
Python Version: 3.9
Glue Version: 3.0 or higher (supports Python Shell)
Max Capacity: 0.0625 DPU (default for Python Shell)
Max Retries: 1
Timeout: 30 minutes

Libraries:
  - awswrangler (pre-installed in Glue)
  - pandas (pre-installed in Glue)
  - boto3 (pre-installed in Glue)
  - PureCloudPlatformClientV2 (custom upload as .whl or install via pip in job)

Trigger: Eventbridge Invocation
  Invoked by: prod-genesys-mappers-rule
  Schedule: 'cron(0 6 * * ? *)'  # Run at 6 AM UTC daily

IAM Role: arn:aws:iam::123456789012:role/genesys_mappers_glue_role
IAM Permissions:
  - AWSGlueServiceRole
  - s3:PutObject, s3:DeleteObject, s3:GetObject (resources: genesys-streaming-lakehouse/*)
  - glue:CreateTable, glue:UpdateTable, glue:GetTable, glue:GetPartitions, glue:CreatePartition (resources: genesys_streaming_lakehouse_curated/*)
  - secretsmanager:GetSecretValue (resources: arn:aws:secretsmanager:us-east-1:123456789012:secret:genesys-mappers/PureCloudPlatformClientV2-XXXXXX)
  - sns:Publish (resources: arn:aws:sns:us-east-1:123456789012:genesys-pipeline-alerts)
  - logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents

Job Parameters (passed as --key value):
    - --SECRET_ARN: arn:aws:secretsmanager:us-east-1:123456789012:secret:genesys-mappers/PureCloudPlatformClientV2-XXXXXX
    - --S3_BUCKET: genesys-streaming-lakehouse
    - --USERS_S3_PREFIX: genesys_voice_interactions/prod/mappers/users
    - --QUEUES_S3_PREFIX: genesys_voice_interactions/prod/mappers/queues
    - --SNS_ARN: arn:aws:sns:us-east-1:123456789012:genesys-pipeline-alerts
    - --GLUE_DATABASE: genesys_streaming_lakehouse_curated
    - --LOG_LEVEL: INFO (optional)

External Dependencies:
  - PureCloudPlatformClientV2==246.0.0 (Genesys Python SDK)
  Install via: --additional-python-modules PureCloudPlatformClientV2==246.0.0

"""

import sys
import time
import json
import boto3
import logging
import awswrangler as wr
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError

# Import Glue utils for job arguments
from awsglue.utils import getResolvedOptions

# Import Genesys SDK
try:
    import PureCloudPlatformClientV2
    from PureCloudPlatformClientV2.rest import ApiException
except ImportError as e:
    print(f"ERROR: Failed to import PureCloudPlatformClientV2: {e}")
    print("Make sure to add --additional-python-modules PureCloudPlatformClientV2==246.0.0 to job parameters")
    sys.exit(1)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Cache for credentials
_cached_credentials = None

# Parse job arguments
args = getResolvedOptions(sys.argv, [
    'SECRET_ARN',
    'S3_BUCKET',
    'USERS_S3_PREFIX',
    'QUEUES_S3_PREFIX',
    'SNS_ARN',
    'GLUE_DATABASE'
])

# Optional arguments with defaults
LOG_LEVEL = args.get('LOG_LEVEL', 'INFO')
logger.setLevel(getattr(logging, LOG_LEVEL))

# Configuration from job parameters
SECRET_ARN = args['SECRET_ARN']
S3_BUCKET = args['S3_BUCKET']
USERS_S3_PREFIX = args['USERS_S3_PREFIX']
QUEUES_S3_PREFIX = args['QUEUES_S3_PREFIX']
SNS_ARN = args['SNS_ARN']
GLUE_DATABASE = args['GLUE_DATABASE']

# AWS clients
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns')

logger.info("="*80)
logger.info("Genesys Voice Mappers Glue Job Started")
logger.info(f"Configuration:")
logger.info(f"  S3 Bucket: {S3_BUCKET}")
logger.info(f"  Users Prefix: {USERS_S3_PREFIX}")
logger.info(f"  Queues Prefix: {QUEUES_S3_PREFIX}")
logger.info(f"  Glue Database: {GLUE_DATABASE}")
logger.info("="*80)


def get_genesys_credentials():
    """
    Retrieve Genesys credentials from AWS Secrets Manager with caching.
    Returns tuple of (client_id, client_secret)
    """
    global _cached_credentials

    if _cached_credentials is not None:
        logger.info("Using cached Genesys credentials")
        return _cached_credentials

    try:
        logger.info(f"Retrieving Genesys credentials from Secrets Manager: {SECRET_ARN}")
        response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        secret = json.loads(response['SecretString'])

        client_id = secret.get('api_client_id')
        client_secret = secret.get('api_client_token')

        if not client_id or not client_secret:
            raise ValueError("Secret does not contain required keys: api_client_id, api_client_token")

        _cached_credentials = (client_id, client_secret)
        logger.info("Successfully retrieved Genesys credentials")
        return _cached_credentials

    except Exception as e:
        logger.error(f"Failed to retrieve credentials from Secrets Manager: {e}")
        raise


def authenticate_genesys(client_id, client_secret):
    """
    Authenticate with Genesys Cloud API and set access token.
    """
    logger.info("Authenticating with Genesys Cloud API...")
    try:
        apiclient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(
            client_id, client_secret
        )
        PureCloudPlatformClientV2.configuration.access_token = apiclient.access_token
        logger.info("Successfully authenticated with Genesys Cloud")
    except Exception as e:
        logger.error(f"Failed to authenticate with Genesys Cloud: {e}")
        raise


def fetch_users(current_date):
    """
    Fetch all users from Genesys Cloud API with pagination.
    Returns list of user dictionaries.
    """
    logger.info("="*80)
    logger.info("FETCHING USERS FROM GENESYS CLOUD")
    logger.info("="*80)

    api_instance = PureCloudPlatformClientV2.UsersApi()
    users_data = []
    page_number = 1
    page_size = 500  # Max allowed by Genesys API
    total_pages = None
    total_users = 0

    while True:
        try:
            logger.info(f"Fetching users page {page_number}...")
            api_response = api_instance.get_users(
                page_size=page_size,
                page_number=page_number,
                state='active', #alternative values: 'active', 'inactive', 'deleted', 'any'. We want active users only.
                sort_order='ASC'
            )

            if total_pages is None:
                total_users = api_response.total
                total_pages = api_response.page_count
                logger.info(f"Total users: {total_users}, Total pages: {total_pages}")

            for user in api_response.entities:
                users_data.append({
                    'id': user.id,
                    'name': user.name,
                    'email': user.email if hasattr(user, 'email') else None,
                    'department': user.department if hasattr(user, 'department') else None,
                    'division_id': user.division.id if hasattr(user, 'division') and user.division else None,
                    'division_name': user.division.name if hasattr(user, 'division') and user.division else None,
                    'dt': current_date
                })

            logger.info(f"Processed page {page_number}/{total_pages} - Users collected: {len(users_data)}")

            if page_number >= total_pages:
                break

            page_number += 1

        except ApiException as e:
            logger.error(f"API Exception on page {page_number}: {e}")
            if e.status == 429:  # Rate limit
                retry_after = int(e.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue
            else:
                raise

    logger.info(f"Successfully fetched {len(users_data)} users")

    # Check collection completeness
    if len(users_data) < total_users * 0.9:
        logger.warning(f"Only {len(users_data)}/{total_users} users collected")
        message = (f"Warning: Only {len(users_data)}/{total_users} users were collected "
                   f"from Genesys Cloud API. Please investigate.")
        sns_client.publish(
            TopicArn=SNS_ARN,
            Message=message,
            Subject="Genesys Users Mapping Warning"
        )

    return users_data, total_users


def fetch_queues(current_date):
    """
    Fetch all queues from Genesys Cloud API with pagination.
    Returns list of queue dictionaries.
    """
    logger.info("="*80)
    logger.info("FETCHING QUEUES FROM GENESYS CLOUD")
    logger.info("="*80)

    api_instance = PureCloudPlatformClientV2.RoutingApi()
    queues_data = []
    page_number = 1
    page_size = 300
    total_pages = None
    total_queues = 0

    while True:
        try:
            logger.info(f"Fetching queues page {page_number}...")
            api_response = api_instance.get_routing_queues_divisionviews_all(
                page_size=page_size,
                page_number=page_number,
                sort_order='asc'
            )

            if total_pages is None:
                total_queues = api_response.total
                total_pages = api_response.page_count
                logger.info(f"Total queues: {total_queues}, Total pages: {total_pages}")

            for queue in api_response.entities:
                queues_data.append({
                    'id': queue.id,
                    'name': queue.name,
                    'division_id': queue.division.id if queue.division else None,
                    'division_name': queue.division.name if queue.division else None,
                    'dt': current_date
                })

            logger.info(f"Processed page {page_number}/{total_pages} - Queues collected: {len(queues_data)}")

            if page_number >= total_pages:
                break

            page_number += 1

        except ApiException as e:
            logger.error(f"API Exception on page {page_number}: {e}")
            if e.status == 429:
                retry_after = int(e.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited. Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue
            else:
                raise

    logger.info(f"Successfully fetched {len(queues_data)} queues")

    # Check collection completeness
    if len(queues_data) < total_queues * 0.9:
        logger.warning(f"Only {len(queues_data)}/{total_queues} queues collected")
        message = (f"Warning: Only {len(queues_data)}/{total_queues} queues were collected "
                   f"from Genesys Cloud API. Please investigate.")
        sns_client.publish(
            TopicArn=SNS_ARN,
            Message=message,
            Subject="Genesys Queues Mapping Warning"
        )

    return queues_data, total_queues


def write_parquet_to_s3(df, s3_prefix, table_name, entity_type):
    """
    Write DataFrame to S3 as Parquet with Glue Catalog integration.
    """
    logger.info(f"Writing {entity_type} data to S3...")
    s3_path = f"s3://{S3_BUCKET}/{s3_prefix}/"
    logger.info(f"S3 path: {s3_path}")

    result = wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        partition_cols=['dt'],
        mode='overwrite_partitions',
        database=GLUE_DATABASE,
        table=table_name,
        compression='snappy',
        sanitize_columns=False
    )

    logger.info(f"Successfully wrote {len(result['paths'])} Parquet file(s)")
    logger.info(f"Files: {result['paths']}")
    return result


def main():
    """
    Main execution function for Glue job.
    """
    start_time = time.time()

    try:
        # Get current date for partitioning
        current_date = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Processing date partition: {current_date}")

        # Get credentials and authenticate
        client_id, client_secret = get_genesys_credentials()
        authenticate_genesys(client_id, client_secret)

        # Fetch users
        users_data, total_users = fetch_users(current_date)
        users_df = pd.DataFrame(users_data)
        logger.info(f"Created users DataFrame with {len(users_df)} rows")

        # Write users to S3
        users_result = write_parquet_to_s3(
            users_df,
            USERS_S3_PREFIX,
            'voice_mapper_users',
            'users'
        )

        # Fetch queues
        queues_data, total_queues = fetch_queues(current_date)
        queues_df = pd.DataFrame(queues_data)
        logger.info(f"Created queues DataFrame with {len(queues_df)} rows")

        # Write queues to S3
        queues_result = write_parquet_to_s3(
            queues_df,
            QUEUES_S3_PREFIX,
            'voice_mapper_queues',
            'queues'
        )

        # Calculate execution time
        execution_time = time.time() - start_time

        logger.info("="*80)
        logger.info("JOB COMPLETED SUCCESSFULLY")
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        logger.info(f"Users: {len(users_df)}/{total_users} fetched, {len(users_result['paths'])} files written")
        logger.info(f"Queues: {len(queues_df)}/{total_queues} fetched, {len(queues_result['paths'])} files written")
        logger.info(f"Partition: dt={current_date}")
        logger.info("="*80)

        return {
            'statusCode': 200,
            'message': 'Mappers job completed successfully',
            'users_count': len(users_df),
            'queues_count': len(queues_df),
            'partition': f"dt={current_date}",
            'execution_time_seconds': execution_time
        }

    except ClientError as e:
        logger.error(f"AWS ClientError: {e}")
        raise

    except ApiException as e:
        logger.error(f"Genesys API Exception: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        result = main()
        logger.info(f"Final result: {json.dumps(result, indent=2)}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Job failed with error: {e}")
        sys.exit(1)
