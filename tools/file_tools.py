"""
File processing utilities for handling uploads.
"""

import logging
import io
from typing import Any, Dict, List, Optional, Tuple, BinaryIO
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class FileTools:
    """
    Tools for processing uploaded files.
    
    Provides:
    - CSV parsing and validation
    - File type detection
    - Data preview generation
    """
    
    SUPPORTED_EXTENSIONS = {
        ".csv": "csv",
        ".tsv": "tsv",
        ".txt": "text",
        ".json": "json",
        ".parquet": "parquet",
        ".xlsx": "excel",
        ".xls": "excel",
    }
    
    MAX_PREVIEW_ROWS = 100
    MAX_FILE_SIZE_MB = 100
    
    def __init__(self):
        self.encoding_options = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
    
    def detect_file_type(self, filename: str) -> Optional[str]:
        """
        Detect file type from filename.
        
        Args:
            filename: Name of the file
            
        Returns:
            File type string or None if unsupported
        """
        path = Path(filename)
        ext = path.suffix.lower()
        return self.SUPPORTED_EXTENSIONS.get(ext)
    
    def read_csv(
        self,
        file_content: BinaryIO,
        filename: str = "data.csv",
        delimiter: Optional[str] = None,
        encoding: Optional[str] = None,
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Read CSV file content into DataFrame.
        
        Args:
            file_content: File content as binary IO
            filename: Original filename
            delimiter: Optional delimiter override
            encoding: Optional encoding override
            
        Returns:
            Tuple of (DataFrame, error_message)
        """
        # Try to detect delimiter and encoding
        if encoding:
            encodings_to_try = [encoding]
        else:
            encodings_to_try = self.encoding_options
        
        content = file_content.read()
        
        for enc in encodings_to_try:
            try:
                text_content = content.decode(enc)
                
                # Detect delimiter if not specified
                if delimiter is None:
                    delimiter = self._detect_delimiter(text_content)
                
                df = pd.read_csv(
                    io.StringIO(text_content),
                    delimiter=delimiter,
                    low_memory=False,
                )
                
                logger.info(f"Successfully read CSV with encoding {enc}")
                return df, None
                
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading CSV with encoding {enc}: {e}")
                continue
        
        return None, "Failed to read CSV file with supported encodings"
    
    def _detect_delimiter(self, content: str) -> str:
        """Detect CSV delimiter from content."""
        first_lines = content.split("\n")[:5]
        sample = "\n".join(first_lines)
        
        delimiters = [",", "\t", ";", "|"]
        counts = {d: sample.count(d) for d in delimiters}
        
        # Return delimiter with highest count
        return max(counts, key=counts.get)
    
    def validate_csv(
        self,
        df: pd.DataFrame,
        required_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Validate CSV data quality.
        
        Args:
            df: DataFrame to validate
            required_columns: Optional list of required column names
            
        Returns:
            Dictionary with validation results
        """
        validation = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "stats": {
                "row_count": len(df),
                "column_count": len(df.columns),
                "null_counts": df.isnull().sum().to_dict(),
                "duplicate_rows": int(df.duplicated().sum()),
            },
        }
        
        # Check for required columns
        if required_columns:
            missing = set(required_columns) - set(df.columns)
            if missing:
                validation["is_valid"] = False
                validation["errors"].append(f"Missing required columns: {missing}")
        
        # Check for empty DataFrame
        if len(df) == 0:
            validation["is_valid"] = False
            validation["errors"].append("CSV file is empty")
        
        # Check for high null percentages
        for col, null_count in validation["stats"]["null_counts"].items():
            null_pct = null_count / len(df) * 100
            if null_pct > 50:
                validation["warnings"].append(
                    f"Column '{col}' has {null_pct:.1f}% null values"
                )
        
        # Check for duplicate rows
        if validation["stats"]["duplicate_rows"] > 0:
            dup_pct = validation["stats"]["duplicate_rows"] / len(df) * 100
            validation["warnings"].append(
                f"Found {validation['stats']['duplicate_rows']} duplicate rows ({dup_pct:.1f}%)"
            )
        
        return validation
    
    def generate_preview(
        self,
        df: pd.DataFrame,
        max_rows: int = 10,
        max_columns: int = 20,
    ) -> Dict[str, Any]:
        """
        Generate a preview of DataFrame content.
        
        Args:
            df: DataFrame to preview
            max_rows: Maximum rows to include
            max_columns: Maximum columns to include
            
        Returns:
            Dictionary with preview data
        """
        preview_df = df.head(max_rows)
        
        if len(df.columns) > max_columns:
            preview_df = preview_df.iloc[:, :max_columns]
            column_note = f"Showing {max_columns} of {len(df.columns)} columns"
        else:
            column_note = None
        
        return {
            "columns": preview_df.columns.tolist(),
            "data": preview_df.to_dict(orient="records"),
            "row_count": len(df),
            "column_count": len(df.columns),
            "preview_rows": len(preview_df),
            "column_note": column_note,
        }
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean column names for SQL compatibility.
        
        Args:
            df: DataFrame with columns to clean
            
        Returns:
            DataFrame with cleaned column names
        """
        import re
        
        def clean_name(name: str) -> str:
            # Convert to string
            name = str(name)
            # Replace spaces and special chars with underscore
            name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
            # Remove leading digits
            name = re.sub(r'^[0-9]+', '', name)
            # Remove consecutive underscores
            name = re.sub(r'_+', '_', name)
            # Remove leading/trailing underscores
            name = name.strip('_')
            # Lowercase
            name = name.lower()
            # Ensure not empty
            if not name:
                name = "column"
            return name
        
        new_columns = [clean_name(col) for col in df.columns]
        
        # Handle duplicates
        seen = {}
        for i, col in enumerate(new_columns):
            if col in seen:
                seen[col] += 1
                new_columns[i] = f"{col}_{seen[col]}"
            else:
                seen[col] = 0
        
        df_copy = df.copy()
        df_copy.columns = new_columns
        return df_copy
    
    def get_column_statistics(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed statistics for each column.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary mapping column names to statistics
        """
        stats = {}
        
        for column in df.columns:
            col_stats = {
                "dtype": str(df[column].dtype),
                "null_count": int(df[column].isnull().sum()),
                "null_pct": round(df[column].isnull().sum() / len(df) * 100, 2),
                "unique_count": int(df[column].nunique()),
                "unique_pct": round(df[column].nunique() / len(df) * 100, 2),
            }
            
            # Numeric statistics
            if pd.api.types.is_numeric_dtype(df[column]):
                col_stats.update({
                    "min": float(df[column].min()) if not df[column].isnull().all() else None,
                    "max": float(df[column].max()) if not df[column].isnull().all() else None,
                    "mean": round(float(df[column].mean()), 4) if not df[column].isnull().all() else None,
                    "median": round(float(df[column].median()), 4) if not df[column].isnull().all() else None,
                })
            
            # String statistics
            if df[column].dtype == "object":
                non_null = df[column].dropna().astype(str)
                if len(non_null) > 0:
                    col_stats.update({
                        "min_length": int(non_null.str.len().min()),
                        "max_length": int(non_null.str.len().max()),
                        "avg_length": round(non_null.str.len().mean(), 2),
                    })
                    
                    # Top values
                    top_values = df[column].value_counts().head(5).to_dict()
                    col_stats["top_values"] = top_values
            
            stats[column] = col_stats
        
        return stats
