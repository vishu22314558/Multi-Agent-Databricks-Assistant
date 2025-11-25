"""
Input validation utilities.
"""

import re
from typing import Any, Dict, List, Optional, Tuple


class Validators:
    """
    Collection of validation utilities.
    """
    
    # Patterns for validation
    TABLE_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    CATALOG_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    FULL_TABLE_PATTERN = re.compile(
        r'^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$'
    )
    
    @classmethod
    def validate_table_name(cls, name: str) -> Tuple[bool, Optional[str]]:
        """
        Validate a table name.
        
        Args:
            name: Table name to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not name:
            return False, "Table name cannot be empty"
        
        if len(name) > 255:
            return False, "Table name too long (max 255 characters)"
        
        # Check for full name (catalog.schema.table)
        if "." in name:
            if cls.FULL_TABLE_PATTERN.match(name):
                return True, None
            else:
                parts = name.split(".")
                if len(parts) != 3:
                    return False, "Full table name must have format: catalog.schema.table"
                
                for i, part in enumerate(parts):
                    if not cls.TABLE_NAME_PATTERN.match(part):
                        labels = ["Catalog", "Schema", "Table"]
                        return False, f"{labels[i]} name '{part}' contains invalid characters"
                
                return True, None
        
        # Simple table name
        if not cls.TABLE_NAME_PATTERN.match(name):
            return False, "Table name must start with letter/underscore and contain only alphanumeric/underscore"
        
        return True, None
    
    @classmethod
    def validate_column_name(cls, name: str) -> Tuple[bool, Optional[str]]:
        """
        Validate a column name.
        
        Args:
            name: Column name to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not name:
            return False, "Column name cannot be empty"
        
        if len(name) > 255:
            return False, "Column name too long (max 255 characters)"
        
        if not cls.TABLE_NAME_PATTERN.match(name):
            return False, "Column name must start with letter/underscore and contain only alphanumeric/underscore"
        
        # Check for reserved words
        reserved_words = {
            "select", "from", "where", "and", "or", "not", "in", "is",
            "null", "true", "false", "as", "on", "join", "left", "right",
            "inner", "outer", "cross", "full", "group", "by", "order",
            "having", "limit", "offset", "union", "intersect", "except",
            "insert", "update", "delete", "create", "alter", "drop",
            "table", "database", "schema", "column", "index", "view",
        }
        
        if name.lower() in reserved_words:
            return False, f"'{name}' is a reserved word and should be quoted"
        
        return True, None
    
    @classmethod
    def validate_databricks_token(cls, token: str) -> Tuple[bool, Optional[str]]:
        """
        Validate Databricks token format.
        
        Args:
            token: Token to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not token:
            return False, "Token cannot be empty"
        
        # Databricks tokens typically start with 'dapi'
        if not token.startswith("dapi"):
            return False, "Databricks token should start with 'dapi'"
        
        if len(token) < 20:
            return False, "Token appears too short"
        
        return True, None
    
    @classmethod
    def validate_databricks_host(cls, host: str) -> Tuple[bool, Optional[str]]:
        """
        Validate Databricks host URL.
        
        Args:
            host: Host URL to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not host:
            return False, "Host URL cannot be empty"
        
        if not host.startswith("https://"):
            return False, "Host URL must start with 'https://'"
        
        if not (host.endswith(".cloud.databricks.com") or 
                host.endswith(".azuredatabricks.net") or
                host.endswith(".gcp.databricks.com")):
            # Could be on-premise or custom domain
            pass
        
        return True, None
    
    @classmethod
    def validate_sql(cls, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Basic SQL validation.
        
        Args:
            sql: SQL to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not sql or not sql.strip():
            return False, "SQL statement cannot be empty"
        
        # Check for unbalanced parentheses
        if sql.count("(") != sql.count(")"):
            return False, "Unbalanced parentheses"
        
        # Check for unbalanced quotes
        quote_count = sql.count("'") - sql.count("\\'") * 2
        if quote_count % 2 != 0:
            return False, "Unbalanced quotes"
        
        return True, None
    
    @classmethod
    def sanitize_input(cls, text: str, max_length: int = 10000) -> str:
        """
        Sanitize user input.
        
        Args:
            text: Text to sanitize
            max_length: Maximum allowed length
            
        Returns:
            Sanitized text
        """
        if not text:
            return ""
        
        # Truncate if too long
        if len(text) > max_length:
            text = text[:max_length]
        
        # Remove null bytes
        text = text.replace("\x00", "")
        
        return text
    
    @classmethod
    def validate_file_upload(
        cls,
        filename: str,
        file_size: int,
        allowed_extensions: Optional[List[str]] = None,
        max_size_mb: int = 100,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate file upload.
        
        Args:
            filename: Name of the file
            file_size: Size in bytes
            allowed_extensions: List of allowed extensions
            max_size_mb: Maximum file size in MB
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not filename:
            return False, "Filename cannot be empty"
        
        # Check extension
        if allowed_extensions is None:
            allowed_extensions = [".csv", ".tsv", ".txt", ".json", ".parquet"]
        
        ext = filename.lower().split(".")[-1] if "." in filename else ""
        if f".{ext}" not in allowed_extensions:
            return False, f"File type '.{ext}' not allowed. Allowed: {allowed_extensions}"
        
        # Check size
        max_size_bytes = max_size_mb * 1024 * 1024
        if file_size > max_size_bytes:
            return False, f"File too large ({file_size / 1024 / 1024:.1f}MB). Max: {max_size_mb}MB"
        
        return True, None


def validate_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate configuration dictionary.
    
    Args:
        config: Configuration to validate
        
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    # Check Databricks config
    if "databricks" in config:
        db_config = config["databricks"]
        
        if db_config.get("host"):
            is_valid, error = Validators.validate_databricks_host(db_config["host"])
            if not is_valid:
                errors.append(f"Databricks host: {error}")
        
        if db_config.get("token"):
            is_valid, error = Validators.validate_databricks_token(db_config["token"])
            if not is_valid:
                errors.append(f"Databricks token: {error}")
    
    return errors
