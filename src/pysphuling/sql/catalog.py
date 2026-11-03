"""Catalog interface for managing metadata, databases, tables, and views in PySphuling."""

from __future__ import annotations
from typing import List, NamedTuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pysphuling.sql.session import SphulingSession


class TableMetadata(NamedTuple):
    name: str
    database: Optional[str]
    description: Optional[str]
    tableType: str
    isTemporary: bool


class DatabaseMetadata(NamedTuple):
    name: str
    description: Optional[str]
    locationUri: str


class Catalog:
    """User-facing catalog interface."""

    def __init__(self, session: SphulingSession) -> None:
        self._session = session

    def currentDatabase(self) -> str:
        return "default"

    def setCurrentDatabase(self, dbName: str) -> None:
        pass

    def listDatabases(self) -> List[DatabaseMetadata]:
        return [DatabaseMetadata("default", "Default in-memory database", "memory://")]

    def listTables(self, dbName: Optional[str] = None) -> List[TableMetadata]:
        """Returns a list of tables/views in the specified database."""
        # Query DataFusion context catalog
        ctx = self._session._engine.ctx
        # In DataFusion: ctx.catalog()
        try:
            cat = ctx.catalog("datafusion")
            schema = cat.schema("public")
            names = schema.names()
            return [TableMetadata(name, "default", None, "MANAGED", False) for name in names]
        except Exception:
            return []

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        """Check if the table or view with the specified name exists."""
        tables = [t.name for t in self.listTables(dbName)]
        return tableName in tables

    def dropTempView(self, viewName: str) -> bool:
        """Drops the local temporary view with the given view name in the catalog."""
        try:
            self._session._engine.ctx.deregister_table(viewName)
            return True
        except Exception:
            return False

    def dropGlobalTempView(self, viewName: str) -> bool:
        return self.dropTempView(viewName)

    def isCached(self, tableName: str) -> bool:
        return self.tableExists(tableName)

    def cacheTable(self, tableName: str) -> None:
        pass

    def uncacheTable(self, tableName: str) -> None:
        pass

    def clearCache(self) -> None:
        pass

