import os

import dagster as dg

from .s3 import S3Resource
from .postgres import PostgresResource
from .directory_processing import DirectoryProcessingResource

# Re-export for convenience
__all__ = ["S3Resource", "PostgresResource", "DirectoryProcessingResource"]

# Define resources with environment variables

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "s3": S3Resource(
                endpoint=os.getenv("S3_ENDPOINT_URL"),
                access_key=os.getenv("S3_ACCESS_KEY"),
                secret_key=os.getenv("S3_SECRET_KEY"),
                region=os.getenv("S3_REGION", "us-east-1"),
            ),
            "pg": PostgresResource(
                host=os.getenv("POSTGRES_HOST"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=os.getenv("POSTGRES_DB"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
            ),
            "dir_proc": DirectoryProcessingResource(),
        }
    )
