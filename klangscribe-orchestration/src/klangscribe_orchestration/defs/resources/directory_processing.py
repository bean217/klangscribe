import json
from typing import Optional

import dagster as dg
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from .postgres import PostgresResource


class DirectoryProcessingResource(dg.ConfigurableResource):
    """Resource for directory processing state and metadata operations."""

    pg: dg.ResourceDependency[PostgresResource]

    def _ensure_tables_exist(self, conn):
        """Create tables if they don't exist"""
        # Directory processing state table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS directory_processing_state (
                dirname VARCHAR(255) PRIMARY KEY,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR(50) DEFAULT 'processing',
                run_id VARCHAR(255)
            )
        """))

        # Directory metadata table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS directory_metadata (
                id SERIAL PRIMARY KEY,
                dirname VARCHAR(255),
                file_count INTEGER,
                total_size BIGINT,
                files_json JSONB,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Create index for faster lookups
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_directory_processing_state_status
            ON directory_processing_state(status)
        """))

    def mark_directory_as_processing(self, dirname: str, run_id: Optional[str] = None):
        """
        Mark a directory as being processed.
        Returns True if successfully marked (wasn't already processing).
        Returns False if already marked (prevents duplicate processing).
        Uses database constraint to ensure atomicity.
        """
        with self.pg.get_connection() as conn:
            self._ensure_tables_exist(conn)

            try:
                # Try to insert (fails if the directory already exists)
                conn.execute(
                    text("""
                        INSERT INTO directory_processing_state (dirname, status, run_id)
                        VALUES (:dirname, 'processing', :run_id)
                    """),
                    {"dirname": dirname, "run_id": run_id}
                )
                return True
            except IntegrityError:
                # Directory already being processed
                return False

    def mark_directory_as_completed(self, dirname: str):
        """Mark a directory as completed"""
        with self.pg.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE directory_processing_state
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                    WHERE dirname = :dirname
                """),
                {"dirname": dirname}
            )

    def mark_directory_as_failed(self, dirname: str):
        """Mark a directory as failed"""
        with self.pg.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE directory_processing_state
                    SET status = 'failed', completed_at = CURRENT_TIMESTAMP
                    WHERE dirname = :dirname
                """),
                {"dirname": dirname}
            )

    def get_processed_directories(self) -> set:
        """Get all directories that have been processed or are being processed"""
        with self.pg.get_connection() as conn:
            self._ensure_tables_exist(conn)

            result = conn.execute(text("""SELECT dirname FROM directory_processing_state"""))
            return {row[0] for row in result.fetchall()}

    def get_stuck_directories(self, timeout_hours: int = 24) -> list:
        """Get directories stuck in 'processing' state for too long"""
        with self.pg.get_connection() as conn:
            self._ensure_tables_exist(conn)

            result = conn.execute(
                text("""
                    SELECT dirname, started_at, run_id
                    FROM directory_processing_state
                    WHERE status = 'processing'
                    AND started_at < NOW() - :timeout_hours * INTERVAL '1 hour'
                """),
                {"timeout_hours": timeout_hours}
            )
            return result.fetchall()

    def cleanup_stuck_directories(self, timeout_hours: int = 24) -> int:
        """Mark old 'processing' directories as failed and return count"""
        with self.pg.get_connection() as conn:
            self._ensure_tables_exist(conn)

            result = conn.execute(
                text("""
                    UPDATE directory_processing_state
                    SET status = 'failed', completed_at = CURRENT_TIMESTAMP
                    WHERE status = 'processing'
                    AND started_at < NOW() - :timeout_hours * INTERVAL '1 hour'
                """),
                {"timeout_hours": timeout_hours}
            )
            return result.rowcount

    def store_directory_metadata(self, dirname: str, file_count: int, total_size: int, files: list):
        """Store directory metadata in PostgreSQL"""
        with self.pg.get_connection() as conn:
            self._ensure_tables_exist(conn)

            conn.execute(
                text("""
                    INSERT INTO directory_metadata (dirname, file_count, total_size, files_json)
                    VALUES (:dirname, :file_count, :total_size, :files_json)
                """),
                {"dirname": dirname, "file_count": file_count, "total_size": total_size, "files_json": json.dumps(files)}
            )

    def get_directory_metadata(self, dirname: Optional[str] = None) -> list:
        """Retrieve directory metadata from PostgreSQL"""
        with self.pg.get_connection() as conn:
            self._ensure_tables_exist(conn)

            if dirname:
                # retrieve a specific directory
                result = conn.execute(
                    text("""
                        SELECT id, dirname, file_count, total_size, files_json, uploaded_at
                        FROM directory_metadata
                        WHERE dirname = :dirname
                    """),
                    {"dirname": dirname}
                )
            else:
                # retrieve all directories
                result = conn.execute(
                    text("""
                        SELECT id, dirname, file_count, total_size, files_json, uploaded_at
                        FROM directory_metadata
                        ORDER BY uploaded_at DESC
                    """)
                )

            return result.fetchall()

    def get_processing_stats(self) -> dict:
        """Get statistics about directory processing"""
        with self.pg.get_connection() as conn:
            self._ensure_tables_exist(conn)

            result = conn.execute(text("""
                SELECT
                    status,
                    COUNT(*) as count
                FROM directory_processing_state
                GROUP BY status
            """))

            stats = {row[0]: row[1] for row in result.fetchall()}

            result = conn.execute(text("""
                SELECT COUNT(*) FROM directory_metadata
            """))
            stats['total_processed'] = result.fetchone()[0]

            return stats
