"""
Configuration management for the Databricks Multi-Agent System.
"""

import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class OllamaConfig(BaseModel):
    """Ollama LLM configuration."""
    model: str = Field(default="llama3.1", description="Ollama model name")
    base_url: str = Field(default="http://localhost:11434", description="Ollama server URL")
    temperature: float = Field(default=0.1, description="Model temperature")
    num_ctx: int = Field(default=8192, description="Context window size")
    timeout: int = Field(default=120, description="Request timeout in seconds")


class DatabricksConfig(BaseModel):
    """Databricks connection configuration."""
    host: str = Field(default="", description="Databricks workspace URL")
    token: str = Field(default="", description="Databricks access token")
    catalog: str = Field(default="main", description="Default catalog")
    schema_name: str = Field(default="default", description="Default schema")
    warehouse_id: Optional[str] = Field(default=None, description="SQL Warehouse ID")
    
    @property
    def is_configured(self) -> bool:
        """Check if Databricks is properly configured."""
        return bool(self.host and self.token)


class AgentConfig(BaseModel):
    """Agent behavior configuration."""
    orchestrator_model: str = Field(default="llama3.1", description="Model for orchestrator")
    specialized_model: str = Field(default="llama3.1", description="Model for specialized agents")
    max_iterations: int = Field(default=5, description="Max agent iterations per query")
    timeout: int = Field(default=120, description="Agent timeout in seconds")
    enable_memory: bool = Field(default=True, description="Enable conversation memory")
    memory_window: int = Field(default=10, description="Number of messages to remember")


class FeatureFlags(BaseModel):
    """Feature toggles."""
    enable_sql_execution: bool = Field(default=True, description="Allow SQL execution")
    enable_file_upload: bool = Field(default=True, description="Allow file uploads")
    enable_agent_memory: bool = Field(default=True, description="Enable agent memory")
    enable_streaming: bool = Field(default=True, description="Enable streaming responses")
    enable_debugging: bool = Field(default=False, description="Enable debug mode")


class Settings(BaseModel):
    """Main settings container."""
    ollama: OllamaConfig = Field(default_factory=OllamaConfig)
    databricks: DatabricksConfig = Field(default_factory=DatabricksConfig)
    agents: AgentConfig = Field(default_factory=AgentConfig)
    features: FeatureFlags = Field(default_factory=FeatureFlags)
    
    # Paths
    project_root: Path = Field(default_factory=lambda: Path(__file__).parent.parent)
    prompts_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "prompts")
    logs_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent / "logs")
    
    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from environment variables."""
        return cls(
            ollama=OllamaConfig(
                model=os.getenv("OLLAMA_MODEL", "llama3.1"),
                base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
            ),
            databricks=DatabricksConfig(
                host=os.getenv("DATABRICKS_HOST", ""),
                token=os.getenv("DATABRICKS_TOKEN", ""),
                catalog=os.getenv("DATABRICKS_CATALOG", "main"),
                schema_name=os.getenv("DATABRICKS_SCHEMA", "default"),
                warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
            ),
            agents=AgentConfig(
                orchestrator_model=os.getenv("ORCHESTRATOR_MODEL", "llama3.1"),
                specialized_model=os.getenv("SPECIALIZED_AGENT_MODEL", "llama3.1"),
                max_iterations=int(os.getenv("MAX_AGENT_ITERATIONS", "5")),
                timeout=int(os.getenv("AGENT_TIMEOUT", "120")),
            ),
            features=FeatureFlags(
                enable_sql_execution=os.getenv("ENABLE_SQL_EXECUTION", "true").lower() == "true",
                enable_file_upload=os.getenv("ENABLE_FILE_UPLOAD", "true").lower() == "true",
                enable_agent_memory=os.getenv("ENABLE_AGENT_MEMORY", "true").lower() == "true",
            ),
        )


# Global settings instance
settings = Settings.from_env()


def get_settings() -> Settings:
    """Get the global settings instance."""
    return settings


def reload_settings() -> Settings:
    """Reload settings from environment."""
    global settings
    load_dotenv(override=True)
    settings = Settings.from_env()
    return settings
