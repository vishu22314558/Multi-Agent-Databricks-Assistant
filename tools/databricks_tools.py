"""
Databricks SDK wrapper for common operations.
"""

import logging
from typing import Any, Dict, List, Optional
import os

logger = logging.getLogger(__name__)


class DatabricksTools:
    """
    Wrapper for Databricks SDK operations.
    
    Provides:
    - Connection management
    - SQL execution
    - Table operations
    - Catalog operations
    """
    
    def __init__(self):
        self._client = None
        self._sql_client = None
        self._host = os.getenv("DATABRICKS_HOST", "")
        self._token = os.getenv("DATABRICKS_TOKEN", "")
        self._warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        self._catalog = os.getenv("DATABRICKS_CATALOG", "main")
        self._schema = os.getenv("DATABRICKS_SCHEMA", "default")
    
    @property
    def is_configured(self) -> bool:
        """Check if Databricks credentials are configured."""
        return bool(self._host and self._token)
    
    def _get_client(self):
        """Get or create Databricks client."""
        if self._client is None and self.is_configured:
            try:
                from databricks.sdk import WorkspaceClient
                self._client = WorkspaceClient(
                    host=self._host,
                    token=self._token,
                )
                logger.info("Databricks client initialized successfully")
            except ImportError:
                logger.warning("databricks-sdk not installed")
            except Exception as e:
                logger.error(f"Failed to initialize Databricks client: {e}")
        return self._client
    
    def _get_sql_client(self):
        """Get or create SQL client."""
        if self._sql_client is None and self.is_configured and self._warehouse_id:
            try:
                from databricks import sql
                self._sql_client = sql.connect(
                    server_hostname=self._host.replace("https://", ""),
                    http_path=f"/sql/1.0/warehouses/{self._warehouse_id}",
                    access_token=self._token,
                )
                logger.info("SQL client initialized successfully")
            except ImportError:
                logger.warning("databricks-sql-connector not installed")
            except Exception as e:
                logger.error(f"Failed to initialize SQL client: {e}")
        return self._sql_client
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the Databricks connection.
        
        Returns:
            Dictionary with connection status
        """
        if not self.is_configured:
            return {
                "success": False,
                "error": "Databricks credentials not configured",
            }
        
        client = self._get_client()
        if client is None:
            return {
                "success": False,
                "error": "Failed to create Databricks client",
            }
        
        try:
            # Try to list catalogs as a connection test
            catalogs = list(client.catalogs.list())
            return {
                "success": True,
                "catalogs": [c.name for c in catalogs[:5]],
                "message": f"Connected successfully. Found {len(catalogs)} catalogs.",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }
    
    def execute_sql(
        self,
        sql: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a SQL statement.
        
        Args:
            sql: SQL statement to execute
            catalog: Optional catalog override
            schema: Optional schema override
            
        Returns:
            Dictionary with results or error
        """
        sql_client = self._get_sql_client()
        
        if sql_client is None:
            return {
                "success": False,
                "error": "SQL client not available",
            }
        
        try:
            cursor = sql_client.cursor()
            
            # Set catalog and schema if specified
            if catalog or self._catalog:
                cursor.execute(f"USE CATALOG {catalog or self._catalog}")
            if schema or self._schema:
                cursor.execute(f"USE SCHEMA {schema or self._schema}")
            
            # Execute the main query
            cursor.execute(sql)
            
            # Fetch results if it's a SELECT
            if sql.strip().upper().startswith("SELECT"):
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return {
                    "success": True,
                    "columns": columns,
                    "rows": [dict(zip(columns, row)) for row in rows],
                    "row_count": len(rows),
                }
            else:
                return {
                    "success": True,
                    "message": "Statement executed successfully",
                    "rows_affected": cursor.rowcount,
                }
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
            }
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def list_tables(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        List tables in a schema.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            List of table info dictionaries
        """
        client = self._get_client()
        if client is None:
            return []
        
        try:
            tables = client.tables.list(
                catalog_name=catalog or self._catalog,
                schema_name=schema or self._schema,
            )
            return [
                {
                    "name": t.name,
                    "type": t.table_type.value if t.table_type else "UNKNOWN",
                    "full_name": f"{t.catalog_name}.{t.schema_name}.{t.name}",
                }
                for t in tables
            ]
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def get_table_schema(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Table name
            catalog: Optional catalog override
            schema: Optional schema override
            
        Returns:
            Dictionary with schema information
        """
        client = self._get_client()
        if client is None:
            return {"error": "Client not available"}
        
        try:
            table = client.tables.get(
                full_name=f"{catalog or self._catalog}.{schema or self._schema}.{table_name}"
            )
            
            columns = []
            if table.columns:
                for col in table.columns:
                    columns.append({
                        "name": col.name,
                        "type": col.type_name.value if col.type_name else "UNKNOWN",
                        "comment": col.comment,
                        "nullable": col.nullable,
                    })
            
            return {
                "name": table.name,
                "full_name": table.full_name,
                "table_type": table.table_type.value if table.table_type else "UNKNOWN",
                "columns": columns,
                "comment": table.comment,
                "properties": table.properties,
            }
        except Exception as e:
            logger.error(f"Failed to get table schema: {e}")
            return {"error": str(e)}
    
    def create_table(
        self,
        ddl: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a table using DDL.
        
        Args:
            ddl: CREATE TABLE statement
            catalog: Optional catalog override
            schema: Optional schema override
            
        Returns:
            Result dictionary
        """
        return self.execute_sql(ddl, catalog, schema)
    
    def get_catalogs(self) -> List[str]:
        """Get list of available catalogs."""
        client = self._get_client()
        if client is None:
            return []
        
        try:
            return [c.name for c in client.catalogs.list()]
        except Exception as e:
            logger.error(f"Failed to get catalogs: {e}")
            return []
    
    def get_schemas(self, catalog: Optional[str] = None) -> List[str]:
        """Get list of schemas in a catalog."""
        client = self._get_client()
        if client is None:
            return []
        
        try:
            schemas = client.schemas.list(catalog_name=catalog or self._catalog)
            return [s.name for s in schemas]
        except Exception as e:
            logger.error(f"Failed to get schemas: {e}")
            return []
