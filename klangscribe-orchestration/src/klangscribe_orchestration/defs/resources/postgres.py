import json

import psycopg2
from contextlib import contextmanager
from typing import Optional

import dagster as dg


class PostgresResource(dg.ConfigurableResource):
    """PostgreSQL database resource"""

    host: str
    user: str
    password: str
    database: str
    port: int = 5432

    @contextmanager
    def get_connection(self):
        """Get database connection with automatic commit/rollback"""
        conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_tables_exist(self, cursor):
        """Create tables if they don't exist"""
        # Directory processing state table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS directory_processing_state (
                dirname VARCHAR(255) PRIMARY KEY,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR(50) DEFAULT 'processing',
                run_id VARCHAR(255)
            )
        """)

        # Directory metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS directory_metadata (
                id SERIAL PRIMARY KEY,
                dirname VARCHAR(255),
                file_count INTEGER,
                total_size BIGINT,
                files_json JSONB,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Ceate index for faster lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_directory_processing_state_status
            ON directory_processing_state(status)
        """)

    def mark_directory_as_processing(self, dirname: str, run_id: Optional[str] = None):
        """
        Mark a directory as being processed
        Returns True if successfully marked (wasn't already processing)
        Returns False if already marked (prevents duplicate processing)
        Uses database constraint to ensure atomicity.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self._ensure_tables_exist(cursor)

            try:
                # Try to insert (fails if the directory already exists)
                cursor.execute(
                    """
                    INSERT INTO directory_processing_state (dirname, status, run_id)
                    VALUES (%s, 'processing', %s)
                    """,
                    (dirname, run_id)
                )
                return True
            except psycopg2.IntegrityError:
                # Directory already being processed
                return False

    def mark_directory_as_completed(self, dirname: str):
        """Mark a directory as completed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE directory_processing_state
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                WHERE dirname = %s
                """,
                (dirname,)
            )

    def mark_directory_as_failed(self, dirname: str):
        """Mark a directory as failed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE directory_processing_state
                SET status = 'failed', completed_at = CURRENT_TIMESTAMP
                WHERE dirname = %s
                """,
                (dirname,)
            )

    def get_processed_directories(self) -> set:
        """Get all directories that have been processed or are being processed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self._ensure_tables_exist(cursor)

            cursor.execute("""SELECT dirname FROM directory_processing_state""")
            return {row[0] for row in cursor.fetchall()}

    def get_stuck_directories(self, timeout_hours: int = 24) -> list:
        """Get directories stuck in 'processing' state for too long"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self._ensure_tables_exist(cursor)

            cursor.execute(
                """
                SELECT dirname, started_at, run_id
                FROM directory_processing_state
                WHERE status = 'processing'
                AND started_at < NOW() - INTERVAL '%s hours'
                """,
                (timeout_hours,)
            )
            return cursor.fetchall()

    def cleanup_stuck_directories(self, timeout_hours: int = 24) -> int:
        """Mark old 'processing' directories as failed and return count"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self._ensure_tables_exist(cursor)

            cursor.execute(
                """
                UPDATE directory_processing_state
                SET status = 'failed', completed_at = CURRENT_TIMESTAMP
                WHERE status = 'processing'
                AND started_at < NOW() - INTERVAL '%s hours'
                """,
                (timeout_hours,)
            )
            return cursor.rowcount

    def store_directory_metadata(self, dirname: str, file_count: int, total_size: int, files: list):
        """Store directory metadata in PostgreSQL"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self._ensure_tables_exist(cursor)

            # Insert metadata
            cursor.execute(
                """
                INSERT INTO directory_metadata (dirname, file_count, total_size, files_json)
                VALUES (%s, %s, %s, %s)
                """,
                (dirname, file_count, total_size, json.dumps(files))
            )

    def get_directory_metadata(self, dirname: Optional[str] = None) -> list:
        """Retrieve directory metadata from PostgreSQL"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self._ensure_tables_exist(cursor)

            if dirname:
                # retrieve a specific directory
                cursor.execute(
                    """
                    SELECT id, dirname, file_count, total_size, files_json, uploaded_at
                    FROM directory_metadata
                    WHERE dirname = %s
                    """,
                    (dirname,)
                )
            else:
                # retrieve all directories
                cursor.execute(
                    """
                    SELECT id, dirname, file_count, total_size, files_json, uploaded_at
                    FROM directory_metadata
                    ORDER BY uploaded_at DESC
                    """
                )

            return cursor.fetchall()

    def get_processing_stats(self) -> dict:
        """Get statistics about directory processing"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            self._ensure_tables_exist(cursor)

            cursor.execute("""
                SELECT
                    status,
                    COUNT(*) as count
                FROM directory_processing_state
                GROUP BY status
            """)

            stats = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute("""
                SELECT COUNT(*) FROM directory_metadata
            """)
            stats['total_processed'] = cursor.fetchone()[0]

            return stats
