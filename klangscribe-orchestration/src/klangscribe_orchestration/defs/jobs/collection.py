import os
import shutil

import dagster as dg

from ..resources import S3Resource, DirectoryProcessingResource

class DirConfig(dg.Config):
    """Configuration for file processing job."""

    dirname: str
    dirpath: str

@dg.op(out=dg.Out(dict))
def process_directory(
    context: dg.OpExecutionContext,
    config: DirConfig,
    s3: S3Resource,
    dir_proc: DirectoryProcessingResource,
) -> dict:
    """Move directory from watched directory to S3-compatible storage and log metadata to Postgres"""

    dirpath = config.dirpath
    dirname = config.dirname

    context.log.info(f"Processing directory: {dirname} at {dirpath}")

    try:
        # Get all files in directory
        files = [
            f for f in os.listdir(dirpath)
            if os.path.isfile(os.path.join(dirpath, f))
        ]

        context.log.info(f"Found {len(files)} files in {dirname}")

        uploaded_files = []
        total_size = 0

        # Upload each file in the directory
        for filename in files:
            filepath = os.path.join(dirpath, filename)
            file_size = os.path.getsize(filepath)

            context.log.info(f"  Uploading: {filename} ({file_size} bytes)")

            bucket_name = "data-collection"
            object_key = f"collected/{dirname}/{filename}"

            context.log.info(f"    Bucket:     {bucket_name}")
            context.log.info(f"    Object Key: {object_key}")
            context.log.info(f"    Filepath:   {filepath}")

            context.log.info(f"S3_ENDPOINT_URL: {s3.endpoint}")
            context.log.info(f"S3_ACCESS_KEY: {s3.access_key}")
            context.log.info(f"S3_SECRET_KEY: {s3.secret_key}")
            context.log.info(f"S3_REGION: {s3.region}")

            storage_path = s3.upload_file(bucket_name, object_key, filepath)
            context.log.info(f"  Uploaded to S3: {storage_path}")

            uploaded_files.append({
                "filename": filename,
                "storage_path": storage_path,
                "file_size": file_size
            })

            total_size += file_size

        # Store metadata in PostgreSQL
        dir_proc.store_directory_metadata(
            dirname=dirname,
            file_count=len(uploaded_files),
            total_size=total_size,
            files=uploaded_files
        )
        context.log.info(f"Stored metadata in PostgreSQL")

        # Mark as completed in processing state table
        dir_proc.mark_directory_as_completed(dirname)
        context.log.info(f"Marked directory as completed")

        return {
            "dirname": dirname,
            "file_count": len(uploaded_files),
            "total_size": total_size,
            "uploaded_files": uploaded_files
        }

    except Exception as e:
        # Mark as failed if processing fails
        context.log.error(f"Failed to process directory: {e}")
        dir_proc.mark_directory_as_failed(dirname)
        raise dg.Failure(f"Failed to process directory {dirname}: {e}")


@dg.op
def delete_src_dir(context: dg.OpExecutionContext, config: DirConfig, dir_info: dict):
    """Delete source directory after successful upload"""
    dirpath = config.dirpath

    try:
        shutil.rmtree(dirpath)
        context.log.info(f"Deleted source directory: {dirpath}")
    except Exception as e:
        context.log.error(f"Failed to delete directory: {e}")
        raise


@dg.op
def send_notification(context: dg.OpExecutionContext, dir_info: dict):
    """Log notification about successful processing"""
    context.log.info(f"-> Directory processing complete: {dir_info['dirname']}")
    context.log.info(f"   Files processed:               {dir_info['file_count']}")
    context.log.info(f"   Total size:                    {dir_info['total_size']} bytes")
    for file_info in dir_info['uploaded_files']:
        context.log.info(f"     - {file_info['filename']}   ->   {file_info['storage_path']} ({file_info['file_size']} bytes)")


@dg.job
def dir_collection_job():
    """Job that moves new directories in a parent watch directory to S3-compatible storage and logs metadata to PostgreSQL."""
    dir_info = process_directory()
    delete_src_dir(dir_info=dir_info)
    send_notification(dir_info=dir_info)
