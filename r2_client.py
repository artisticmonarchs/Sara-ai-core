# r2_client.py
"""
Cloudflare R2 Client Utilities
Handles connection checks and basic R2 health diagnostics.
"""

import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def get_r2_client():
    """Create and return a Cloudflare R2-compatible boto3 client."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name="auto",  # Cloudflare R2 ignores region, but boto3 requires it
    )

def check_r2_connection():
    """
    Verify R2 connectivity by attempting to list buckets.
    Returns True if successful, False otherwise.
    """
    try:
        client = get_r2_client()
        client.list_buckets()
        return True
    except (ClientError, NoCredentialsError, Exception) as e:
        print(f"[R2] Connection check failed: {e}")
        return False
