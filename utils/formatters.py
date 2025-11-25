"""
Output formatting utilities.
"""

from typing import Any, Dict, List, Optional
import json


class Formatters:
    """
    Collection of output formatting utilities.
    """
    
    @staticmethod
    def format_sql(sql: str, indent: int = 4) -> str:
        """
        Format SQL for display.
        
        Args:
            sql: SQL to format
            indent: Indentation spaces
            
        Returns:
            Formatted SQL string
        """
        # Simple formatting - uppercase keywords
        keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE",
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
            "ON", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
            "UNION", "INTERSECT", "EXCEPT", "AS", "DISTINCT", "ALL",
            "INSERT INTO", "UPDATE", "DELETE FROM", "CREATE TABLE",
            "ALTER TABLE", "DROP TABLE", "CASE", "WHEN", "THEN", "ELSE", "END",
            "WITH", "OVER", "PARTITION BY",
        ]
        
        result = sql
        for kw in keywords:
            import re
            pattern = re.compile(rf'\b{kw}\b', re.IGNORECASE)
            result = pattern.sub(kw, result)
        
        return result
    
    @staticmethod
    def format_table_data(
        data: List[Dict[str, Any]],
        max_rows: int = 20,
        max_col_width: int = 50,
    ) -> str:
        """
        Format tabular data for display.
        
        Args:
            data: List of row dictionaries
            max_rows: Maximum rows to display
            max_col_width: Maximum column width
            
        Returns:
            Formatted table string
        """
        if not data:
            return "No data"
        
        # Get columns
        columns = list(data[0].keys())
        
        # Calculate column widths
        widths = {}
        for col in columns:
            col_values = [str(row.get(col, ""))[:max_col_width] for row in data[:max_rows]]
            widths[col] = max(len(col), max(len(v) for v in col_values) if col_values else 0)
        
        # Build header
        header = " | ".join(col.ljust(widths[col]) for col in columns)
        separator = "-+-".join("-" * widths[col] for col in columns)
        
        # Build rows
        rows = []
        for row in data[:max_rows]:
            row_str = " | ".join(
                str(row.get(col, ""))[:max_col_width].ljust(widths[col])
                for col in columns
            )
            rows.append(row_str)
        
        result = f"{header}\n{separator}\n" + "\n".join(rows)
        
        if len(data) > max_rows:
            result += f"\n... and {len(data) - max_rows} more rows"
        
        return result
    
    @staticmethod
    def format_schema(schema: Dict[str, str]) -> str:
        """
        Format schema information.
        
        Args:
            schema: Dictionary mapping column names to types
            
        Returns:
            Formatted schema string
        """
        if not schema:
            return "No schema"
        
        max_name_len = max(len(name) for name in schema.keys())
        
        lines = []
        for name, dtype in schema.items():
            lines.append(f"  {name.ljust(max_name_len)}  {dtype}")
        
        return "\n".join(lines)
    
    @staticmethod
    def format_code_block(code: str, language: str = "sql") -> str:
        """
        Format code in a markdown code block.
        
        Args:
            code: Code to format
            language: Language for syntax highlighting
            
        Returns:
            Markdown code block string
        """
        return f"```{language}\n{code}\n```"
    
    @staticmethod
    def format_agent_response(
        agent_name: str,
        result: str,
        data: Optional[Dict[str, Any]] = None,
        execution_time_ms: Optional[int] = None,
    ) -> str:
        """
        Format an agent response for display.
        
        Args:
            agent_name: Name of the agent
            result: Response content
            data: Optional additional data
            execution_time_ms: Optional execution time
            
        Returns:
            Formatted response string
        """
        parts = [result]
        
        if execution_time_ms:
            parts.append(f"\n\n*Processed by {agent_name} in {execution_time_ms}ms*")
        
        return "\n".join(parts)
    
    @staticmethod
    def format_validation_result(validation: Dict[str, Any]) -> str:
        """
        Format validation results.
        
        Args:
            validation: Validation result dictionary
            
        Returns:
            Formatted validation string
        """
        parts = []
        
        if validation.get("is_valid"):
            parts.append("✅ Validation passed")
        else:
            parts.append("❌ Validation failed")
        
        if validation.get("errors"):
            parts.append("\n**Errors:**")
            for error in validation["errors"]:
                parts.append(f"  - {error}")
        
        if validation.get("warnings"):
            parts.append("\n**Warnings:**")
            for warning in validation["warnings"]:
                parts.append(f"  - {warning}")
        
        return "\n".join(parts)
    
    @staticmethod
    def format_diff(old: str, new: str) -> str:
        """
        Format a simple diff between two strings.
        
        Args:
            old: Original string
            new: New string
            
        Returns:
            Formatted diff string
        """
        import difflib
        
        old_lines = old.splitlines(keepends=True)
        new_lines = new.splitlines(keepends=True)
        
        diff = difflib.unified_diff(
            old_lines, new_lines,
            fromfile="original",
            tofile="modified"
        )
        
        return "".join(diff)
    
    @staticmethod
    def truncate_text(text: str, max_length: int = 500, suffix: str = "...") -> str:
        """
        Truncate text to a maximum length.
        
        Args:
            text: Text to truncate
            max_length: Maximum length
            suffix: Suffix to add if truncated
            
        Returns:
            Truncated text
        """
        if len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)] + suffix
    
    @staticmethod
    def format_json(data: Any, indent: int = 2) -> str:
        """
        Format data as pretty JSON.
        
        Args:
            data: Data to format
            indent: Indentation level
            
        Returns:
            Formatted JSON string
        """
        return json.dumps(data, indent=indent, default=str)
    
    @staticmethod
    def format_bytes(size_bytes: int) -> str:
        """
        Format bytes as human-readable string.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Human-readable size string
        """
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size_bytes) < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
    
    @staticmethod
    def format_duration(ms: int) -> str:
        """
        Format milliseconds as human-readable duration.
        
        Args:
            ms: Duration in milliseconds
            
        Returns:
            Human-readable duration string
        """
        if ms < 1000:
            return f"{ms}ms"
        elif ms < 60000:
            return f"{ms / 1000:.1f}s"
        elif ms < 3600000:
            return f"{ms / 60000:.1f}min"
        else:
            return f"{ms / 3600000:.1f}h"
