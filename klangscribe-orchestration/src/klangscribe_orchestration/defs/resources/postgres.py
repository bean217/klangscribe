from contextlib import contextmanager
from typing import Optional, Dict, Any
from urllib.parse import quote_plus

import dagster as dg
from pydantic import PrivateAttr
from sqlalchemy import create_engine, text


class PostgresResource(dg.ConfigurableResource):
    """PostgreSQL database resource"""

    host: str
    user: str
    password: str
    database: str
    port: int = 5432

    _engine: Optional[object] = PrivateAttr(default=None)

    def _get_engine(self):
        if self._engine is None:
            url = f"postgresql+psycopg2://{quote_plus(self.user)}:{quote_plus(self.password)}@{self.host}:{self.port}/{self.database}"
            self._engine = create_engine(url)
        return self._engine

    @contextmanager
    def get_connection(self):
        """Get database connection with automatic commit/rollback"""
        engine = self._get_engine()
        with engine.connect() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def fetchall(self, query: str, params: Optional[Dict[str, Any]] = None) -> list:
        with self.get_connection() as conn:
            result = conn.execute(text(query), params or {})
            return list(result.fetchall())
