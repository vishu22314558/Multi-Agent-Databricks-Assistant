"""
Schema inference tools for analyzing data and generating DDL.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class SchemaInferenceTools:
    """
    Tools for inferring schemas from data sources.
    
    Provides:
    - Pandas to Spark SQL type mapping
    - Schema inference from CSV/DataFrames
    - Column statistics and profiling
    """
    
    # Pandas dtype to Spark SQL type mapping
    DTYPE_MAPPING = {
        "int64": "BIGINT",
        "int32": "INT",
        "int16": "SMALLINT",
        "int8": "TINYINT",
        "float64": "DOUBLE",
        "float32": "FLOAT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "datetime64": "TIMESTAMP",
        "timedelta64[ns]": "STRING",
        "object": "STRING",
        "string": "STRING",
        "category": "STRING",
    }
    
    def __init__(self):
        self.type_patterns = self._compile_type_patterns()
    
    def _compile_type_patterns(self) -> Dict[str, re.Pattern]:
        """Compile regex patterns for type detection."""
        return {
            "email": re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$'),
            "phone": re.compile(r'^[\d\-\+\(\)\s]+$'),
            "date_iso": re.compile(r'^\d{4}-\d{2}-\d{2}$'),
            "datetime_iso": re.compile(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'),
            "uuid": re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I),
            "decimal": re.compile(r'^-?\d+\.\d+$'),
            "integer": re.compile(r'^-?\d+$'),
            "boolean": re.compile(r'^(true|false|yes|no|0|1)$', re.I),
        }
    
    def infer_schema(
        self,
        df: pd.DataFrame,
        sample_size: int = 1000,
    ) -> Dict[str, str]:
        """
        Infer Spark SQL schema from a pandas DataFrame.
        
        Args:
            df: Input DataFrame
            sample_size: Number of rows to sample for inference
            
        Returns:
            Dictionary mapping column names to Spark SQL types
        """
        schema = {}
        sample = df.head(sample_size) if len(df) > sample_size else df
        
        for column in df.columns:
            spark_type = self._infer_column_type(sample[column])
            schema[column] = spark_type
        
        return schema
    
    def _infer_column_type(self, series: pd.Series) -> str:
        """
        Infer the best Spark SQL type for a pandas Series.
        
        Args:
            series: Pandas Series to analyze
            
        Returns:
            Spark SQL type string
        """
        # Handle all-null columns
        if series.isna().all():
            return "STRING"
        
        # Get non-null values
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return "STRING"
        
        # Check pandas dtype first
        dtype_str = str(series.dtype)
        if dtype_str in self.DTYPE_MAPPING:
            base_type = self.DTYPE_MAPPING[dtype_str]
            
            # Refine object/string types by inspecting values
            if base_type == "STRING":
                return self._refine_string_type(non_null)
            
            return base_type
        
        # Fallback to STRING
        return "STRING"
    
    def _refine_string_type(self, series: pd.Series) -> str:
        """
        Refine STRING type based on actual values.
        
        Args:
            series: Series to analyze
            
        Returns:
            Refined Spark SQL type
        """
        sample_values = series.astype(str).head(100)
        
        # Check for date/datetime patterns
        if self._matches_pattern(sample_values, "datetime_iso"):
            return "TIMESTAMP"
        
        if self._matches_pattern(sample_values, "date_iso"):
            return "DATE"
        
        # Check for numeric patterns
        if self._matches_pattern(sample_values, "decimal"):
            # Determine precision
            max_precision = max(
                len(v.replace("-", "").replace(".", ""))
                for v in sample_values if self.type_patterns["decimal"].match(v)
            )
            max_scale = max(
                len(v.split(".")[1]) if "." in v else 0
                for v in sample_values if self.type_patterns["decimal"].match(v)
            )
            return f"DECIMAL({max_precision}, {max_scale})"
        
        if self._matches_pattern(sample_values, "integer"):
            # Determine integer size
            try:
                max_val = max(abs(int(v)) for v in sample_values if self.type_patterns["integer"].match(v))
                if max_val < 128:
                    return "TINYINT"
                elif max_val < 32768:
                    return "SMALLINT"
                elif max_val < 2147483648:
                    return "INT"
                else:
                    return "BIGINT"
            except (ValueError, OverflowError):
                return "STRING"
        
        if self._matches_pattern(sample_values, "boolean"):
            return "BOOLEAN"
        
        # Calculate string length statistics
        lengths = series.astype(str).str.len()
        max_len = lengths.max()
        
        if max_len <= 255:
            return "STRING"
        else:
            return "STRING"  # Could use TEXT for very long strings
    
    def _matches_pattern(self, series: pd.Series, pattern_name: str, threshold: float = 0.9) -> bool:
        """
        Check if most values match a pattern.
        
        Args:
            series: Series to check
            pattern_name: Name of pattern to match
            threshold: Minimum fraction of values that must match
            
        Returns:
            True if threshold is met
        """
        if pattern_name not in self.type_patterns:
            return False
        
        pattern = self.type_patterns[pattern_name]
        matches = sum(1 for v in series if pattern.match(str(v)))
        
        return (matches / len(series)) >= threshold
    
    def profile_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate a profile of the DataFrame.
        
        Args:
            df: DataFrame to profile
            
        Returns:
            Dictionary with profiling information
        """
        profile = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": {},
        }
        
        for column in df.columns:
            col_profile = self._profile_column(df[column])
            profile["columns"][column] = col_profile
        
        return profile
    
    def _profile_column(self, series: pd.Series) -> Dict[str, Any]:
        """
        Generate profile for a single column.
        
        Args:
            series: Series to profile
            
        Returns:
            Dictionary with column profile
        """
        profile = {
            "dtype": str(series.dtype),
            "null_count": int(series.isna().sum()),
            "null_percentage": round(series.isna().sum() / len(series) * 100, 2),
            "unique_count": int(series.nunique()),
        }
        
        # Add statistics for numeric columns
        if np.issubdtype(series.dtype, np.number):
            profile.update({
                "min": float(series.min()) if not series.isna().all() else None,
                "max": float(series.max()) if not series.isna().all() else None,
                "mean": float(series.mean()) if not series.isna().all() else None,
                "std": float(series.std()) if not series.isna().all() else None,
            })
        
        # Add string statistics for object columns
        if series.dtype == "object":
            non_null = series.dropna().astype(str)
            if len(non_null) > 0:
                profile.update({
                    "min_length": int(non_null.str.len().min()),
                    "max_length": int(non_null.str.len().max()),
                    "avg_length": round(non_null.str.len().mean(), 2),
                })
        
        return profile
    
    def suggest_partition_columns(
        self,
        df: pd.DataFrame,
        schema: Dict[str, str],
    ) -> List[str]:
        """
        Suggest columns for partitioning.
        
        Args:
            df: DataFrame to analyze
            schema: Inferred schema
            
        Returns:
            List of suggested partition column names
        """
        suggestions = []
        
        for column, spark_type in schema.items():
            # Date/timestamp columns are good partition candidates
            if spark_type in ("DATE", "TIMESTAMP"):
                suggestions.append(column)
                continue
            
            # Low cardinality string columns might be good
            if spark_type == "STRING":
                unique_ratio = df[column].nunique() / len(df)
                if unique_ratio < 0.01 and df[column].nunique() < 100:
                    suggestions.append(column)
        
        return suggestions[:3]  # Return top 3 suggestions
    
    def suggest_zorder_columns(
        self,
        df: pd.DataFrame,
        schema: Dict[str, str],
    ) -> List[str]:
        """
        Suggest columns for Z-ordering.
        
        Args:
            df: DataFrame to analyze
            schema: Inferred schema
            
        Returns:
            List of suggested Z-order column names
        """
        suggestions = []
        
        for column, spark_type in schema.items():
            col_name_lower = column.lower()
            
            # ID columns are often filtered
            if col_name_lower.endswith("_id") or col_name_lower == "id":
                suggestions.append(column)
                continue
            
            # Date columns for time-based queries
            if spark_type in ("DATE", "TIMESTAMP"):
                suggestions.append(column)
                continue
            
            # High cardinality columns with good selectivity
            if spark_type in ("STRING", "BIGINT", "INT"):
                unique_ratio = df[column].nunique() / len(df)
                if 0.1 < unique_ratio < 0.9:
                    suggestions.append(column)
        
        return suggestions[:4]  # Z-order typically uses up to 4 columns
