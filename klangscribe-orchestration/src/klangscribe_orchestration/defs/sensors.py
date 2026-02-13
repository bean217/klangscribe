import os

import dagster as dg

from .jobs.collection import DirConfig
from .resources import DirectoryProcessingResource


@dg.sensor(
    job_name="dir_collection_job",
    minimum_interval_seconds=2,
    default_status=dg.DefaultSensorStatus.STOPPED
)
def new_file_sensor(context: dg.SensorEvaluationContext, dir_proc: DirectoryProcessingResource) -> dg.SensorResult:
    """
    Check for new directories every 10 seconds.
    Uses PostgreSQL to track which directories have been processed.
    """
    
    watch_dir = "/mnt/watch_dir"
    max_dirs_per_run = int(os.getenv("SENSOR_BATCH_SIZE", "20"))

    # Get current directories on disk
    try:
        all_items = os.listdir(watch_dir)
        current_dirs = set([
            item for item in all_items
            if os.path.isdir(os.path.join(watch_dir, item))
        ])
    except FileNotFoundError:
        context.log.warning(f"Directory not found: {watch_dir}")
        return dg.SkipReason(f"Directory not found: {watch_dir}")

    if not current_dirs:
        return dg.SkipReason("No directories found in watched folder")
    
    # Get directories already in database (processing, completed, or failed)
    processed_dirs = dir_proc.get_processed_directories()

    # Find directories that exist but haven't been processed
    new_dirs = current_dirs - processed_dirs

    if not new_dirs:
        return dg.SkipReason(f"No new directories (found {len(current_dirs)} existing)")

    # Sort and limit
    new_dirs_list = sorted(new_dirs)[:min(max_dirs_per_run, len(new_dirs))]

    context.log.info(f"Found {len(new_dirs_list)} new directories: {new_dirs_list}")

    # Trigger job for each new directory
    for dirname in sorted(new_dirs_list):    # Sort for consistent ordering
        dirpath = os.path.join(watch_dir, dirname)

        # Mark as processing in database (prevents duplicate processing)
        # This is atomic - uses database constraint
        if dir_proc.mark_directory_as_processing(dirname, run_id=None):
            context.log.info(f"Triggering job for: {dirname}")

            yield dg.RunRequest(
                run_key=f"dir_{dirname}_{os.path.getmtime(dirpath)}",
                run_config=dg.RunConfig(
                    ops={
                        "process_directory": {
                            "config": {
                                "dirname": dirname,
                                "dirpath": dirpath
                            }
                        },
                        "delete_src_dir": {
                            "config": {
                                "dirname": dirname,
                                "dirpath": dirpath
                            }
                        }
                    }
                )
            )
        else:
            context.log.info(f"Directory {dirname} already queued/processing, skipping")




    # cursor = context.cursor or ""
    # processed_dirs = set(cursor.split(",")) if cursor else set()

    # # Get current directories
    # try:
    #     all_items = os.listdir(watch_dir)
    #     current_dirs = set([
    #         item for item in all_items
    #         if os.path.isdir(os.path.join(watch_dir, item))
    #     ])
    # except FileNotFoundError:
    #     context.log.warning(f"Directory not found: {watch_dir}")
    #     return

    # # Find new files
    # new_dirs = current_dirs - processed_dirs

    # # Trigger job for each new file
    # for dirname in new_dirs:
    #     dirpath = os.path.join(watch_dir, dirname)

    #     context.log.info(f"New directory detected: {dirname}")

    #     # Get directory info
    #     try:
    #         # Count files in directory
    #         file_count = sum(1 for item in os.listdir(dirpath) if os.path.isfile(os.path.join(dirpath, item)))

    #         # Calculate total size
    #         total_size = sum(
    #             os.path.getsize(os.path.join(dirpath, f))
    #             for f in os.listdir(dirpath)
    #             if os.path.isfile(os.path.join(dirpath, f))
    #         )

    #     except Exception as e:
    #         context.log.warning(f"Could not get info for {dirname}: {e}")
    #         file_count = 0
    #         total_size = 0
        
    #     yield dg.RunRequest(
    #         run_key=f"dir_{dirname}_{os.path.getmtime(dirpath)}",
    #         run_config=dg.RunConfig(
    #             ops={
    #                 "move_dir_to_storage": {
    #                     "config": {
    #                         "dirname": dirname,
    #                         "dirpath": dirpath,
    #                     }
    #                 },
    #                 # "delete_src_dir": {
    #                 #     "config": {
    #                 #         "dirname": dirname,
    #                 #         "dirpath": dirpath,
    #                 #     }
    #                 # }
    #             }
    #         )
    #     )

    #     # Update cursor
    #     updated_cursor = ",".join(current_dirs)
    #     context.update_cursor(updated_cursor)