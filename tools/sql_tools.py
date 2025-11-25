"""
SQL validation and analysis tools.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class SQLTools:
    """
    Tools for SQL validation and analysis.
    
    Provides:
    - SQL syntax validation
    - Query analysis
    - Safety checks for destructive operations
    """
    
    # Dangerous keywords that require extra caution
    DANGEROUS_KEYWORDS = [
        "DROP TABLE", "DROP DATABASE", "DROP SCHEMA", "TRUNCATE",
        "DELETE FROM", "DROP VIEW", "DROP FUNCTION",
    ]
    
    # Read-only keywords
    READ_ONLY_KEYWORDS = ["SELECT", "DESCRIBE", "SHOW", "EXPLAIN"]
    
    def __init__(self):
        self.sql_patterns = self._compile_sql_patterns()
    
    def _compile_sql_patterns(self) -> Dict[str, re.Pattern]:
        """Compile SQL regex patterns."""
        return {
            "select": re.compile(r'^\s*SELECT\s', re.IGNORECASE | re.MULTILINE),
            "create_table": re.compile(r'^\s*CREATE\s+TABLE', re.IGNORECASE | re.MULTILINE),
            "alter_table": re.compile(r'^\s*ALTER\s+TABLE', re.IGNORECASE | re.MULTILINE),
            "drop_table": re.compile(r'^\s*DROP\s+TABLE', re.IGNORECASE | re.MULTILINE),
            "insert": re.compile(r'^\s*INSERT\s+INTO', re.IGNORECASE | re.MULTILINE),
            "update": re.compile(r'^\s*UPDATE\s', re.IGNORECASE | re.MULTILINE),
            "delete": re.compile(r'^\s*DELETE\s+FROM', re.IGNORECASE | re.MULTILINE),
            "merge": re.compile(r'^\s*MERGE\s+INTO', re.IGNORECASE | re.MULTILINE),
            "cte": re.compile(r'^\s*WITH\s+\w+\s+AS\s*\(', re.IGNORECASE | re.MULTILINE),
            "table_name": re.compile(r'(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([`"\[]?[\w\.]+[`"\]]?)', re.IGNORECASE),
            "comment": re.compile(r'--.*$|/\*[\s\S]*?\*/', re.MULTILINE),
        }
    
    def validate_sql(self, sql: str) -> Dict[str, Any]:
        """
        Validate SQL syntax and check for issues.
        
        Args:
            sql: SQL statement to validate
            
        Returns:
            Dictionary with validation results
        """
        result = {
            "is_valid": True,
            "warnings": [],
            "errors": [],
            "statement_type": None,
            "is_destructive": False,
            "tables_referenced": [],
        }
        
        # Remove comments for analysis
        sql_clean = self.sql_patterns["comment"].sub("", sql)
        
        # Check for empty SQL
        if not sql_clean.strip():
            result["is_valid"] = False
            result["errors"].append("SQL statement is empty")
            return result
        
        # Detect statement type
        result["statement_type"] = self._detect_statement_type(sql_clean)
        
        # Check for dangerous operations
        is_destructive, warning = self._check_destructive(sql_clean)
        result["is_destructive"] = is_destructive
        if warning:
            result["warnings"].append(warning)
        
        # Extract referenced tables
        result["tables_referenced"] = self._extract_tables(sql_clean)
        
        # Basic syntax checks
        syntax_errors = self._check_basic_syntax(sql_clean)
        result["errors"].extend(syntax_errors)
        
        if result["errors"]:
            result["is_valid"] = False
        
        return result
    
    def _detect_statement_type(self, sql: str) -> str:
        """Detect the type of SQL statement."""
        type_patterns = [
            ("select", "SELECT"),
            ("cte", "WITH (CTE)"),
            ("create_table", "CREATE TABLE"),
            ("alter_table", "ALTER TABLE"),
            ("drop_table", "DROP TABLE"),
            ("insert", "INSERT"),
            ("update", "UPDATE"),
            ("delete", "DELETE"),
            ("merge", "MERGE"),
        ]
        
        for pattern_name, statement_type in type_patterns:
            if self.sql_patterns[pattern_name].search(sql):
                return statement_type
        
        return "UNKNOWN"
    
    def _check_destructive(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Check if SQL contains destructive operations."""
        sql_upper = sql.upper()
        
        for keyword in self.DANGEROUS_KEYWORDS:
            if keyword in sql_upper:
                return True, f"Warning: Statement contains {keyword} operation"
        
        return False, None
    
    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL."""
        matches = self.sql_patterns["table_name"].findall(sql)
        # Clean up table names
        tables = [
            m.strip("`\"[]").strip()
            for m in matches
            if not m.upper() in ("FROM", "JOIN", "INTO", "UPDATE", "TABLE")
        ]
        return list(set(tables))
    
    def _check_basic_syntax(self, sql: str) -> List[str]:
        """Check for basic syntax issues."""
        errors = []
        
        # Check for unbalanced parentheses
        if sql.count("(") != sql.count(")"):
            errors.append("Unbalanced parentheses")
        
        # Check for unbalanced quotes
        single_quotes = sql.count("'") - sql.count("\\'") * 2
        if single_quotes % 2 != 0:
            errors.append("Unbalanced single quotes")
        
        # Check for common typos
        common_typos = {
            "SLECT": "SELECT",
            "FORM": "FROM",
            "WHER": "WHERE",
            "GRUOP": "GROUP",
            "ODER": "ORDER",
        }
        
        sql_upper = sql.upper()
        for typo, correct in common_typos.items():
            if f" {typo} " in sql_upper:
                errors.append(f"Possible typo: '{typo}' should be '{correct}'")
        
        return errors
    
    def is_read_only(self, sql: str) -> bool:
        """
        Check if SQL is read-only (no modifications).
        
        Args:
            sql: SQL statement to check
            
        Returns:
            True if the statement is read-only
        """
        sql_clean = self.sql_patterns["comment"].sub("", sql)
        sql_upper = sql_clean.strip().upper()
        
        # Check if starts with read-only keyword
        return any(sql_upper.startswith(kw) for kw in self.READ_ONLY_KEYWORDS)
    
    def format_sql(self, sql: str) -> str:
        """
        Basic SQL formatting.
        
        Args:
            sql: SQL to format
            
        Returns:
            Formatted SQL string
        """
        # Keywords to capitalize
        keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE",
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
            "ON", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
            "UNION", "INTERSECT", "EXCEPT", "AS", "DISTINCT", "ALL",
            "INSERT INTO", "UPDATE", "DELETE FROM", "CREATE TABLE",
            "ALTER TABLE", "DROP TABLE", "CASE", "WHEN", "THEN", "ELSE", "END",
            "WITH", "OVER", "PARTITION BY", "ROWS", "BETWEEN", "UNBOUNDED",
        ]
        
        result = sql
        for keyword in keywords:
            pattern = re.compile(rf'\b{keyword}\b', re.IGNORECASE)
            result = pattern.sub(keyword, result)
        
        return result
    
    def analyze_query_complexity(self, sql: str) -> Dict[str, Any]:
        """
        Analyze query complexity.
        
        Args:
            sql: SQL query to analyze
            
        Returns:
            Dictionary with complexity metrics
        """
        sql_upper = sql.upper()
        
        analysis = {
            "join_count": len(re.findall(r'\bJOIN\b', sql_upper)),
            "subquery_count": len(re.findall(r'\(\s*SELECT\b', sql_upper)),
            "cte_count": len(re.findall(r'\bWITH\b.*?\bAS\s*\(', sql_upper)),
            "union_count": len(re.findall(r'\bUNION\b', sql_upper)),
            "aggregation_count": len(re.findall(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', sql_upper)),
            "window_function_count": len(re.findall(r'\bOVER\s*\(', sql_upper)),
            "case_statement_count": len(re.findall(r'\bCASE\b', sql_upper)),
        }
        
        # Calculate complexity score
        score = (
            analysis["join_count"] * 2 +
            analysis["subquery_count"] * 3 +
            analysis["cte_count"] * 1 +
            analysis["union_count"] * 2 +
            analysis["aggregation_count"] * 1 +
            analysis["window_function_count"] * 2 +
            analysis["case_statement_count"] * 1
        )
        
        if score <= 3:
            complexity = "simple"
        elif score <= 8:
            complexity = "moderate"
        elif score <= 15:
            complexity = "complex"
        else:
            complexity = "very_complex"
        
        analysis["complexity_score"] = score
        analysis["complexity_level"] = complexity
        
        return analysis
    
    def suggest_optimizations(self, sql: str) -> List[str]:
        """
        Suggest optimizations for a SQL query.
        
        Args:
            sql: SQL query to analyze
            
        Returns:
            List of optimization suggestions
        """
        suggestions = []
        sql_upper = sql.upper()
        
        # Check for SELECT *
        if "SELECT *" in sql_upper:
            suggestions.append("Consider selecting only needed columns instead of SELECT *")
        
        # Check for missing WHERE clause in UPDATE/DELETE
        if ("UPDATE " in sql_upper or "DELETE FROM" in sql_upper) and "WHERE" not in sql_upper:
            suggestions.append("WARNING: UPDATE/DELETE without WHERE clause affects all rows")
        
        # Check for DISTINCT without clear need
        if "SELECT DISTINCT" in sql_upper:
            suggestions.append("Verify DISTINCT is necessary; it can be expensive")
        
        # Check for multiple JOINs
        join_count = len(re.findall(r'\bJOIN\b', sql_upper))
        if join_count > 5:
            suggestions.append(f"Query has {join_count} JOINs; consider breaking into CTEs or temp tables")
        
        # Check for correlated subqueries (simplified check)
        if "SELECT" in sql_upper and sql_upper.count("SELECT") > 2:
            suggestions.append("Multiple nested SELECTs detected; consider using JOINs or CTEs")
        
        # Check for functions in WHERE clause
        if re.search(r'WHERE.*\b(UPPER|LOWER|SUBSTR|DATE)\s*\(', sql_upper):
            suggestions.append("Functions in WHERE clause may prevent index usage")
        
        # Check for LIKE with leading wildcard
        if re.search(r"LIKE\s+['\"]%", sql_upper):
            suggestions.append("LIKE with leading wildcard (%) prevents index usage")
        
        # Check for OR conditions
        if sql_upper.count(" OR ") > 2:
            suggestions.append("Multiple OR conditions may benefit from UNION or IN clause")
        
        return suggestions
