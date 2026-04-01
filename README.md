# Data Catalog Generator

An AI-powered agent that automatically generates comprehensive data catalogs from Amazon Athena databases. The system extracts database schemas, uses large language models to infer semantic descriptions, includes foreign key hints extracted from source metadata, and produces structured, human-readable documentation in multiple formats.

## 🎯 Features

- **Automated Schema Extraction**: Connects to Amazon Athena and extracts complete database metadata (tables, columns, data types)
- **AI-Powered Semantic Analysis**: Uses LLMs (Claude or GPT-4) to generate human-readable descriptions for tables and columns
- **Foreign Key Hints from Source Metadata**: Captures FK references directly from table metadata and column comments when available
- **Multiple Output Formats**: Generates catalogs in JSON, Markdown, and HTML formats
- **Configurable Pipeline**: Flexible configuration via YAML and environment variables
- **Traceable Hints**: Each FK hint includes its extraction source (table parameter JSON/delimited or column comment)

## 📋 Prerequisites

- Python 3.8+
- AWS Account with Athena access
- API key for Anthropic Claude or OpenAI GPT
- AWS credentials configured

## 🚀 Quick Start

### 1. Installation

```bash
# Clone or download this repository
cd schema_generator

# Run the setup script (creates virtual environment and installs dependencies)
./setup.sh

# Or manually:
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration

Copy the example environment file and configure your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```env
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
ATHENA_DATABASE=your_database_name
ATHENA_S3_OUTPUT=s3://your-bucket/athena-results/

# AI Provider (choose one: anthropic, openai, or gemini)
# Option 1: Anthropic Claude
ANTHROPIC_API_KEY=your_anthropic_api_key
AI_PROVIDER=anthropic
AI_MODEL=claude-3-5-sonnet-20241022

# Option 2: Google Gemini
# GEMINI_API_KEY=your_gemini_api_key
# AI_PROVIDER=gemini
# AI_MODEL=gemini-1.5-pro

# Option 3: OpenAI
# OPENAI_API_KEY=your_openai_api_key
# AI_PROVIDER=openai
# AI_MODEL=gpt-4

# Output Configuration
OUTPUT_FORMATS=json,markdown,html
OUTPUT_DIR=./output
```

### 3. Run the Generator

```bash
# Activate the virtual environment (if not already activated)
source venv/bin/activate

# Run the generator
python run.py

# With custom config file
python run.py --config my_config.yaml

# With verbose logging
python run.py --verbose

# When done, deactivate the virtual environment
deactivate
```

## 📁 Project Structure

```
schema_generator/
├── src/
│   ├── __init__.py
│   ├── main.py                  # Main orchestrator
│   ├── config.py                # Configuration manager
│   ├── schema_extractor.py      # Athena schema extraction
│   ├── semantic_analyzer.py     # AI-powered semantic analysis
│   └── catalog_generator.py     # Multi-format catalog generator
├── output/                      # Generated catalogs (created automatically)
├── config.yaml                  # YAML configuration
├── .env.example                 # Example environment variables
├── requirements.txt             # Python dependencies
├── run.py                       # Quick start script
└── README.md                    # This file
```
| `ATHENA_S3_OUTPUT` | S3 location for query results | No |
| `ANTHROPIC_API_KEY` | Anthropic API key | Yes** |
| `OPENAI_API_KEY` | OpenAI API key | Yes** |
| `GEMINI_API_KEY` | Google Gemini API key | Yes** |
| `AI_PROVIDER` | AI provider: `anthropic`, `openai`, or `gemini` | Yes |
| `AI_MODEL` | Model name | Yes |
| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_REGION` | AWS region for Athena | Yes |
| `AWS_ACCESS_KEY_ID` | AWS access key | Yes* |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Yes* |
| `ATHENA_DATABASE` | Name of Athena database | Yes |
| `ATHENA_S3_OUTPUT` | S3 location for query results | No |
| `ANTHROPIC_API_KEY` | Anthropic API key | Yes** |
| `OPENAI_API_KEY` | OpenAI API key | Yes** |
| `AI_PROVIDER` | AI provider: `anthropic` or `openai` | Yes |
| `AI_MODEL` | Model name | Yes |
| `OUTPUT_FORMATS` | Comma-separated formats | No |
| `OUTPUT_DIR` | Output directory path | No |

\* Can use default AWS credentials if not provided  
\** Only one AI provider key is required

### YAML Configuration (config.yaml)

```yaml
extraction:
  # Tables to include (pattern matching with * wildcard)
  # If specified, ONLY these tables will be processed
  include_tables:
    - magento_*
  
  # Tables to exclude from catalog generation
  exclude_tables:
    - temp_*
    - test_*

  # Maximum number of tables to process (null for no limit)
  max_tables: null

# AI Analysis settings
ai_analysis:
  temperature: 0.3
  max_tokens: 2000
  batch_size: 5

# Output settings
output:
  formats:
    - json
    - markdown
    - html
  directory: ./output
  include_confidence: true
  timestamp_filenames: true
```

## 📊 Output Examples

The generator produces three types of output files:

### JSON Catalog
Structured data with complete metadata:
```json
{
  "metadata": {
    "database_name": "analytics_db",
    "generated_at": "2026-01-29T10:30:00",
    "table_count": 15,
    "foreign_key_hint_count": 23
  },
  "tables": [
    {
      "name": "users",
      "description": "Core user account information",
      "business_context": "User management and authentication",
      "tags": ["users", "authentication", "core"],
      "columns": [...]
    }
  ],
  "foreign_key_hints": [
    {
      "source_table": "orders",
      "source_column": "user_id",
      "target_table": "users",
      "target_column": "id",
      "constraint_name": "fk_orders_user",
      "hint_source": "table_parameter_json"
    }
  ]
}
```

### Markdown Catalog
Human-readable documentation with tables and navigation

### HTML Catalog
Interactive web page with styled tables and navigation

## 🔍 How It Works

### 1. Schema Extraction
- Connects to AWS Athena using boto3
- Queries AWS Glue Data Catalog for table and column metadata
- Extracts data types, comments, and structural information

### 2. Semantic Analysis
- Sends table and column information to LLM
- Generates business-friendly descriptions
- Infers business context and domain
- Suggests tags and data quality notes

### 3. Foreign Key Hint Extraction
Extracts hints from source metadata using these strategies:

- **Table Parameters (JSON)**: Reads keys like `foreign_keys` and `foreignKeys`
- **Table Parameters (Delimited)**: Parses values like `order_id:orders.id;user_id:users.id`
- **Column Comments**: Parses patterns like `references users(id)`

### 4. Catalog Generation
- Aggregates all metadata and descriptions
- Generates output in requested formats
- Adds styling and navigation for HTML/Markdown

## 🔧 Advanced Usage
### Custom AI Provider

The system supports Anthropic Claude, OpenAI GPT, and Google Gemini:

```python
from src.semantic_analyzer import create_ai_provider, SemanticAnalyzer

# Anthropic Claude
provider = create_ai_provider('anthropic', api_key='...', model='claude-3-5-sonnet-20241022')

# Google Gemini
provider = create_ai_provider('gemini', api_key='...', model='gemini-1.5-pro')

# OpenAI GPT
provider = create_ai_provider('openai', api_key='...', model='gpt-4')

analyzer = SemanticAnalyzer(provider, temperature=0.3)
```

### Programmatic Usage

```python
from src.main import CatalogPipeline

# Initialize pipeline
pipeline = CatalogPipeline(config_file='config.yaml', env_file='.env')

# Run pipeline
output_files = pipeline.run()

# Access generated files
print(output_files)
# {'json': './output/catalog_mydb_20260129_103000.json', ...}
```

### Testing with Subset of Tables

```yaml
# config.yaml
extraction:
  max_tables: 5  # Only process first 5 tables for testing
```

## 🛠️ Troubleshooting

### Common Issues

**AWS Authentication Error**
```
Solution: Ensure AWS credentials are configured correctly in .env or use AWS CLI configuration
```

**API Rate Limits**
```
Solution: Reduce batch_size in config.yaml or add delays between API calls
```

**No Foreign Key Hints Found**
```
Solution: Ensure source metadata includes FK references in table parameters or column comments
```

**Import Errors**
```bash
# Ensure all dependencies are installed
pip install -r requirements.txt
```

## 📝 Development

### Running Tests
```bash
# Add pytest to requirements
pip install pytest

# Run tests (if test suite is added)
pytest tests/
```

### Extending the System

**Add Custom Foreign Key Metadata Parsers**
Edit `schema_extractor.py` to parse custom FK formats in `_extract_foreign_key_hints`.

**Add New Output Format**
Extend `catalog_generator.py` with a new method:

```python
def _generate_custom_format(self, catalog_data, database_name, suffix):
    # Your custom format logic
    pass
```

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

This project is provided as-is for educational and commercial use.

## 🙏 Acknowledgments

- Uses AWS Athena and Glue for schema metadata
- Powered by Anthropic Claude and OpenAI GPT models
- Built with Python, boto3, and Jinja2

## 📞 Support

For issues or questions:
1. Check the troubleshooting section
2. Review configuration files
3. Enable verbose logging: `python run.py --verbose`
4. Check `catalog_generator.log` for detailed error messages

---

**Built with ❤️ for data teams who value good documentation**
