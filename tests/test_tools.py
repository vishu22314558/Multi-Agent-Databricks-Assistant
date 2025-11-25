"""
Tests for tool modules.
"""

import pytest
import pandas as pd
import numpy as np

from tools.schema_tools import SchemaInferenceTools
from tools.sql_tools import SQLTools
from tools.file_tools import FileTools


class TestSchemaInferenceTools:
    """Tests for SchemaInferenceTools."""
    
    @pytest.fixture
    def schema_tools(self):
        return SchemaInferenceTools()
    
    def test_infer_integer_types(self, schema_tools):
        """Test integer type inference."""
        df = pd.DataFrame({
            "small_int": [1, 2, 3],
            "big_int": [1000000000, 2000000000, 3000000000],
        })
        
        schema = schema_tools.infer_schema(df)
        
        assert "INT" in schema["small_int"] or "BIGINT" in schema["small_int"]
        assert schema["big_int"] == "BIGINT"
    
    def test_infer_string_types(self, schema_tools):
        """Test string type inference."""
        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["a@b.com", "c@d.com", "e@f.com"],
        })
        
        schema = schema_tools.infer_schema(df)
        
        assert schema["name"] == "STRING"
    
    def test_infer_boolean_types(self, schema_tools):
        """Test boolean type inference."""
        df = pd.DataFrame({
            "flag": [True, False, True],
        })
        
        schema = schema_tools.infer_schema(df)
        
        assert schema["flag"] == "BOOLEAN"
    
    def test_profile_dataframe(self, schema_tools):
        """Test DataFrame profiling."""
        df = pd.DataFrame({
            "id": [1, 2, 3, None],
            "name": ["A", "B", "C", "D"],
        })
        
        profile = schema_tools.profile_dataframe(df)
        
        assert profile["row_count"] == 4
        assert profile["column_count"] == 2
        assert profile["columns"]["id"]["null_count"] == 1


class TestSQLTools:
    """Tests for SQLTools."""
    
    @pytest.fixture
    def sql_tools(self):
        return SQLTools()
    
    def test_validate_valid_sql(self, sql_tools):
        """Test validation of valid SQL."""
        result = sql_tools.validate_sql("SELECT * FROM table")
        
        assert result["is_valid"] == True
        assert result["statement_type"] == "SELECT"
    
    def test_validate_empty_sql(self, sql_tools):
        """Test validation of empty SQL."""
        result = sql_tools.validate_sql("")
        
        assert result["is_valid"] == False
    
    def test_detect_destructive_operations(self, sql_tools):
        """Test detection of destructive SQL."""
        result = sql_tools.validate_sql("DROP TABLE important_data")
        
        assert result["is_destructive"] == True
    
    def test_is_read_only(self, sql_tools):
        """Test read-only detection."""
        assert sql_tools.is_read_only("SELECT * FROM table") == True
        assert sql_tools.is_read_only("INSERT INTO table VALUES (1)") == False
        assert sql_tools.is_read_only("DESCRIBE table") == True
    
    def test_analyze_query_complexity(self, sql_tools):
        """Test query complexity analysis."""
        simple = sql_tools.analyze_query_complexity("SELECT * FROM t")
        assert simple["complexity_level"] == "simple"
        
        complex_query = """
        WITH cte AS (SELECT * FROM a JOIN b ON a.id = b.id)
        SELECT *, COUNT(*) OVER (PARTITION BY x)
        FROM cte
        UNION ALL
        SELECT * FROM c
        """
        complex_result = sql_tools.analyze_query_complexity(complex_query)
        assert complex_result["join_count"] >= 1
        assert complex_result["cte_count"] >= 1
    
    def test_suggest_optimizations(self, sql_tools):
        """Test optimization suggestions."""
        suggestions = sql_tools.suggest_optimizations("SELECT * FROM large_table")
        
        assert any("SELECT *" in s for s in suggestions)


class TestFileTools:
    """Tests for FileTools."""
    
    @pytest.fixture
    def file_tools(self):
        return FileTools()
    
    def test_detect_file_type(self, file_tools):
        """Test file type detection."""
        assert file_tools.detect_file_type("data.csv") == "csv"
        assert file_tools.detect_file_type("data.json") == "json"
        assert file_tools.detect_file_type("data.parquet") == "parquet"
        assert file_tools.detect_file_type("data.unknown") is None
    
    def test_clean_column_names(self, file_tools):
        """Test column name cleaning."""
        df = pd.DataFrame({
            "Column Name": [1],
            "123start": [2],
            "special!@#chars": [3],
        })
        
        cleaned = file_tools.clean_column_names(df)
        
        assert "column_name" in cleaned.columns
        assert "start" in cleaned.columns or "_start" in cleaned.columns
        assert "special_chars" in cleaned.columns
    
    def test_validate_csv(self, file_tools):
        """Test CSV validation."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", None, "C"],
        })
        
        validation = file_tools.validate_csv(df)
        
        assert validation["is_valid"] == True
        assert validation["stats"]["row_count"] == 3
    
    def test_validate_csv_empty(self, file_tools):
        """Test validation of empty CSV."""
        df = pd.DataFrame()
        
        validation = file_tools.validate_csv(df)
        
        assert validation["is_valid"] == False
    
    def test_generate_preview(self, file_tools):
        """Test preview generation."""
        df = pd.DataFrame({
            "id": range(100),
            "name": [f"name_{i}" for i in range(100)],
        })
        
        preview = file_tools.generate_preview(df, max_rows=10)
        
        assert preview["preview_rows"] == 10
        assert preview["row_count"] == 100
        assert len(preview["data"]) == 10
