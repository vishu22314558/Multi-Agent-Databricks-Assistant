# Databricks Multi-Agent System

A sophisticated multi-agent architecture for Databricks data engineering tasks, powered by local LLMs (Ollama) with specialized agents for different domains.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Streamlit UI                                 │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Orchestrator Agent                              │
│  • Routes queries to specialized agents                             │
│  • Manages conversation context                                      │
│  • Coordinates multi-agent workflows                                 │
└──────┬──────────┬──────────┬──────────┬──────────┬─────────────────┘
       │          │          │          │          │
       ▼          ▼          ▼          ▼          ▼
┌──────────┐┌──────────┐┌──────────┐┌──────────┐┌──────────┐
│  Schema  ││   SQL    ││ Pipeline ││ Metadata ││   Chat   │
│  Agent   ││  Agent   ││  Agent   ││  Agent   ││  Agent   │
│          ││          ││          ││          ││          │
│• DDL Gen ││• Query   ││• DLT     ││• Tags    ││• Q&A     │
│• Alter   ││• Optimize││• Stream  ││• Comments││• Explain │
│• Infer   ││• Debug   ││• Bronze/ ││• Props   ││• Best    │
│          ││          ││  Silver/ ││          ││  Practice│
│          ││          ││  Gold    ││          ││          │
└────┬─────┘└────┬─────┘└────┬─────┘└────┬─────┘└────┬─────┘
     │           │           │           │           │
     └───────────┴───────────┼───────────┴───────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │      Shared Tool Layer       │
              │  • Databricks SDK            │
              │  • SQL Execution             │
              │  • File Processing           │
              │  • Schema Inference          │
              └──────────────────────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │     LLM Layer (Ollama)       │
              │  • llama3.1 / llama3.2       │
              │  • codellama                 │
              │  • mistral                   │
              └──────────────────────────────┘
```

## 🌟 Features

### Specialized Agents

| Agent | Responsibilities |
|-------|-----------------|
| **Orchestrator** | Routes queries, manages context, coordinates workflows |
| **Schema Agent** | DDL generation, ALTER statements, type inference |
| **SQL Agent** | Query optimization, debugging, performance tuning |
| **Pipeline Agent** | DLT pipelines, Bronze/Silver/Gold layers, streaming |
| **Metadata Agent** | Tags, comments, table properties, governance |
| **Chat Agent** | General Q&A, best practices, explanations |

### Key Capabilities

- 🔄 **Intelligent Routing**: Automatically routes queries to the most appropriate agent
- 🤝 **Agent Collaboration**: Agents can delegate tasks and share context
- 📊 **CSV to Table**: Upload CSV files and auto-generate DDL with type inference
- ⚡ **Delta Live Tables**: Generate Bronze/Silver/Gold pipeline code
- 🏷️ **Metadata Management**: Comprehensive tagging and documentation
- 🔍 **Query Optimization**: SQL analysis and performance recommendations

## 📁 Project Structure

```
databricks-multiagent-system/
├── app.py                      # Main Streamlit application
├── config/
│   ├── __init__.py
│   ├── settings.py             # Configuration management
│   └── prompts.yaml            # Agent prompt templates
├── core/
│   ├── __init__.py
│   ├── base_agent.py           # Base agent class
│   ├── orchestrator.py         # Main orchestrator agent
│   ├── agent_registry.py       # Agent registration and discovery
│   └── message.py              # Message types and routing
├── agents/
│   ├── __init__.py
│   ├── schema_agent.py         # Schema/DDL operations
│   ├── sql_agent.py            # SQL query operations
│   ├── pipeline_agent.py       # DLT pipeline generation
│   ├── metadata_agent.py       # Metadata management
│   └── chat_agent.py           # General chat/Q&A
├── tools/
│   ├── __init__.py
│   ├── databricks_tools.py     # Databricks SDK wrapper
│   ├── sql_tools.py            # SQL execution tools
│   ├── file_tools.py           # File processing utilities
│   └── schema_tools.py         # Schema inference tools
├── utils/
│   ├── __init__.py
│   ├── llm_client.py           # Ollama client wrapper
│   ├── validators.py           # Input validation
│   └── formatters.py           # Output formatting
├── prompts/
│   ├── orchestrator.txt        # Orchestrator system prompt
│   ├── schema_agent.txt        # Schema agent prompt
│   ├── sql_agent.txt           # SQL agent prompt
│   ├── pipeline_agent.txt      # Pipeline agent prompt
│   ├── metadata_agent.txt      # Metadata agent prompt
│   └── chat_agent.txt          # Chat agent prompt
├── ui/
│   ├── __init__.py
│   ├── components.py           # Reusable UI components
│   ├── tabs.py                 # Tab implementations
│   └── styles.py               # Custom CSS styles
├── tests/
│   ├── __init__.py
│   ├── test_agents.py          # Agent unit tests
│   ├── test_tools.py           # Tool tests
│   └── test_orchestrator.py    # Orchestrator tests
├── requirements.txt            # Python dependencies
├── .env.example                # Environment template
└── README.md                   # This file
```

## 🚀 Quick Start

### Prerequisites

1. **Install Ollama**:
   ```bash
   curl -fsSL https://ollama.ai/install.sh | sh
   ```

2. **Pull a model**:
   ```bash
   ollama pull llama3.1
   ```

3. **Start Ollama server**:
   ```bash
   ollama serve
   ```

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repo-url>
   cd databricks-multiagent-system
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your Databricks credentials
   ```

5. **Run the application**:
   ```bash
   streamlit run app.py
   ```

## ⚙️ Configuration

Create a `.env` file with the following variables:

```env
# Ollama Configuration
OLLAMA_MODEL=llama3.1
OLLAMA_BASE_URL=http://localhost:11434

# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token-here
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
DATABRICKS_WAREHOUSE_ID=your-warehouse-id

# Agent Configuration
ORCHESTRATOR_MODEL=llama3.1
SPECIALIZED_AGENT_MODEL=llama3.1
MAX_AGENT_ITERATIONS=5
AGENT_TIMEOUT=120
```

## 🔧 Usage Examples

### 1. Schema Operations
```
User: Create a customer table from this CSV file
→ Orchestrator routes to Schema Agent
→ Schema Agent infers types and generates DDL
→ Returns CREATE TABLE statement
```

### 2. Query Optimization
```
User: How can I optimize this slow query?
→ Orchestrator routes to SQL Agent
→ SQL Agent analyzes query and suggests improvements
→ Returns optimization recommendations
```

### 3. Pipeline Generation
```
User: Create a DLT pipeline for customer data
→ Orchestrator routes to Pipeline Agent
→ Pipeline Agent generates Bronze/Silver/Gold layers
→ Returns complete DLT Python code
```

### 4. Multi-Agent Workflow
```
User: Create a table from CSV, add PII tags, and create a DLT pipeline
→ Orchestrator coordinates multiple agents:
  1. Schema Agent creates DDL
  2. Metadata Agent adds tags
  3. Pipeline Agent generates DLT
→ Returns complete solution
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_agents.py

# Run with coverage
pytest --cov=. tests/
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📜 License

MIT License - See LICENSE file for details.

## 🙏 Acknowledgments

- Built with [Streamlit](https://streamlit.io/)
- Powered by [Ollama](https://ollama.ai/)
- Uses [Databricks SDK](https://docs.databricks.com/dev-tools/sdk-python.html)
- LLM integration via [LangChain](https://www.langchain.com/)
