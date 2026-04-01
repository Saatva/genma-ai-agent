"""
Data Catalog Generator

This module aggregates all metadata and generates the final data catalog
in multiple formats (JSON, Markdown, HTML).
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
from jinja2 import Template

logger = logging.getLogger(__name__)


class CatalogGenerator:
    """Generates data catalog in multiple formats"""
    
    def __init__(self, output_dir: str = "./output"):
        """
        Initialize the catalog generator
        
        Args:
            output_dir: Directory to save catalog files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized CatalogGenerator with output directory: {output_dir}")
    
    def generate_catalog(
        self,
        database_name: str,
        tables_metadata: Dict[str, Any],
        table_descriptions: Dict[str, Any],
        column_descriptions: Dict[str, Dict[str, Any]],
        foreign_key_hints: List[Dict[str, Any]],
        formats: List[str] = None,
        include_confidence: bool = True,
        timestamp_filenames: bool = True
    ) -> Dict[str, str]:
        """
        Generate complete catalog in specified formats
        
        Args:
            database_name: Name of the database
            tables_metadata: Raw table metadata
            table_descriptions: AI-generated table descriptions
            column_descriptions: AI-generated column descriptions
            foreign_key_hints: Foreign key hints extracted from source metadata
            formats: List of formats to generate ('json', 'markdown', 'html')
            include_confidence: Include confidence scores in output
            timestamp_filenames: Add timestamp to filenames
        
        Returns:
            Dictionary mapping format to output file path
        """
        if formats is None:
            formats = ['json', 'markdown', 'html']
        
        # Build complete catalog data structure
        catalog_data = self._build_catalog_data(
            database_name=database_name,
            tables_metadata=tables_metadata,
            table_descriptions=table_descriptions,
            column_descriptions=column_descriptions,
            foreign_key_hints=foreign_key_hints,
            include_confidence=include_confidence
        )
        
        # Generate timestamp suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if timestamp_filenames else ""
        suffix = f"_{timestamp}" if timestamp else ""
        
        output_files = {}
        
        # Generate each requested format
        if 'json' in formats:
            json_file = self._generate_json(catalog_data, database_name, suffix)
            output_files['json'] = str(json_file)
        
        if 'markdown' in formats:
            md_file = self._generate_markdown(catalog_data, database_name, suffix)
            output_files['markdown'] = str(md_file)
        
        if 'html' in formats:
            html_file = self._generate_html(catalog_data, database_name, suffix)
            output_files['html'] = str(html_file)
        
        logger.info(f"Generated catalog in {len(output_files)} format(s)")
        return output_files
    
    def _build_catalog_data(
        self,
        database_name: str,
        tables_metadata: Dict[str, Any],
        table_descriptions: Dict[str, Any],
        column_descriptions: Dict[str, Dict[str, Any]],
        foreign_key_hints: List[Dict[str, Any]],
        include_confidence: bool
    ) -> Dict[str, Any]:
        """Build complete catalog data structure"""
        catalog = {
            'metadata': {
                'database_name': database_name,
                'generated_at': datetime.now().isoformat(),
                'table_count': len(tables_metadata),
                'foreign_key_hint_count': len(foreign_key_hints)
            },
            'tables': [],
            'foreign_key_hints': []
        }
        
        # Process each table
        for table_name, metadata in tables_metadata.items():
            table_desc = table_descriptions.get(table_name)
            col_descs = column_descriptions.get(table_name, {})
            
            table_info = {
                'name': table_name,
                'description': table_desc.description if table_desc else None,
                'business_context': table_desc.business_context if table_desc else None,
                'tags': table_desc.suggested_tags if table_desc else [],
                'data_quality_notes': table_desc.data_quality_notes if table_desc else None,
                'table_type': metadata.table_type,
                'location': metadata.location,
                'primary_keys': getattr(metadata, 'primary_keys', []),
                'foreign_key_hints': getattr(metadata, 'foreign_key_hints', []),
                'columns': []
            }
            
            # Process columns
            for column in metadata.columns:
                col_desc = col_descs.get(column.name)
                
                column_info = {
                    'name': column.name,
                    'data_type': column.data_type,
                    'description': col_desc.description if col_desc else None,
                    'business_context': col_desc.business_context if col_desc else None,
                    'tags': col_desc.suggested_tags if col_desc else [],
                    'data_quality_notes': col_desc.data_quality_notes if col_desc else None,
                    'is_nullable': column.is_nullable,
                    'ordinal_position': column.ordinal_position
                }
                
                table_info['columns'].append(column_info)
            
            catalog['tables'].append(table_info)
        
        # Process foreign key hints
        for hint in foreign_key_hints:
            hint_info = {
                'source_table': hint.get('source_table'),
                'source_column': hint.get('source_column'),
                'target_table': hint.get('target_table'),
                'target_column': hint.get('target_column'),
                'constraint_name': hint.get('constraint_name'),
                'hint_source': hint.get('hint_source', 'source_metadata')
            }
            catalog['foreign_key_hints'].append(hint_info)
        
        # Sort tables by name
        catalog['tables'].sort(key=lambda t: t['name'])
        
        return catalog
    
    def _generate_json(
        self,
        catalog_data: Dict[str, Any],
        database_name: str,
        suffix: str
    ) -> Path:
        """Generate JSON catalog file"""
        filename = f"catalog_{database_name}{suffix}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(catalog_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Generated JSON catalog: {filepath}")
        return filepath
    
    def _generate_markdown(
        self,
        catalog_data: Dict[str, Any],
        database_name: str,
        suffix: str
    ) -> Path:
        """Generate Markdown catalog file"""
        filename = f"catalog_{database_name}{suffix}.md"
        filepath = self.output_dir / filename
        
        md_template = """# Data Catalog: {{ metadata.database_name }}

**Generated:** {{ metadata.generated_at }}  
**Tables:** {{ metadata.table_count }}  
**Foreign Key Hints:** {{ metadata.foreign_key_hint_count }}

---

## Table of Contents

{% for table in tables %}
- [{{ table.name }}](#{{ table.name | lower | replace('_', '-') }})
{% endfor %}

---

## Tables

{% for table in tables %}
### {{ table.name }}

**Description:** {{ table.description or 'N/A' }}

{% if table.business_context %}
**Business Context:** {{ table.business_context }}
{% endif %}

{% if table.tags %}
**Tags:** {{ table.tags | join(', ') }}
{% endif %}

{% if table.data_quality_notes %}
**Data Quality Notes:** {{ table.data_quality_notes }}
{% endif %}

**Table Type:** {{ table.table_type or 'N/A' }}

**Primary Keys:** {% if table.primary_keys %}`{{ table.primary_keys | join('`, `') }}`{% else %}N/A{% endif %}

#### Columns

| Column Name | Data Type | Description | Tags |
|-------------|-----------|-------------|------|
{% for column in table.columns %}
| `{{ column.name }}` | {{ column.data_type }} | {{ column.description or 'N/A' }} | {{ column.tags | join(', ') if column.tags else '' }} |
{% endfor %}

{% if table.columns %}
**Total Columns:** {{ table.columns | length }}
{% endif %}

---

{% endfor %}

## Foreign Key Hints

{% if foreign_key_hints %}
| Source | Target | Constraint | Source |
|--------|--------|------------|--------|
{% for hint in foreign_key_hints %}
| `{{ hint.source_table }}.{{ hint.source_column }}` | `{{ hint.target_table }}.{{ hint.target_column }}` | {{ hint.constraint_name or 'N/A' }} | {{ hint.hint_source }} |
{% endfor %}
{% else %}
No foreign key hints found in source metadata.
{% endif %}

---

*Generated by Data Catalog Generator*
"""
        
        template = Template(md_template)
        content = template.render(**catalog_data)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Generated Markdown catalog: {filepath}")
        return filepath
    
    def _generate_html(
        self,
        catalog_data: Dict[str, Any],
        database_name: str,
        suffix: str
    ) -> Path:
        """Generate HTML catalog file"""
        filename = f"catalog_{database_name}{suffix}.html"
        filepath = self.output_dir / filename
        
        html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Catalog: {{ metadata.database_name }}</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 40px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }
        h2 {
            color: #2c3e50;
            margin-top: 40px;
            margin-bottom: 20px;
            padding-bottom: 8px;
            border-bottom: 2px solid #ecf0f1;
        }
        h3 {
            color: #34495e;
            margin-top: 30px;
            margin-bottom: 15px;
        }
        h4 {
            color: #7f8c8d;
            margin-top: 20px;
            margin-bottom: 10px;
        }
        .metadata {
            background: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 30px;
        }
        .metadata p {
            margin: 5px 0;
        }
        .table-card {
            background: #fafafa;
            border: 1px solid #e0e0e0;
            border-radius: 5px;
            padding: 20px;
            margin-bottom: 30px;
        }
        .tags {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin: 10px 0;
        }
        .tag {
            background: #3498db;
            color: white;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            background: white;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background: #34495e;
            color: white;
            font-weight: 600;
        }
        tr:hover {
            background: #f5f5f5;
        }
        .code {
            font-family: 'Courier New', monospace;
            background: #f8f8f8;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 0.9em;
        }
        .confidence-high { color: #27ae60; font-weight: bold; }
        .confidence-medium { color: #f39c12; font-weight: bold; }
        .confidence-low { color: #e74c3c; font-weight: bold; }
        .toc {
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 5px;
            padding: 20px;
            margin: 20px 0;
        }
        .toc ul {
            list-style: none;
            columns: 2;
        }
        .toc li {
            margin: 5px 0;
        }
        .toc a {
            color: #3498db;
            text-decoration: none;
        }
        .toc a:hover {
            text-decoration: underline;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Data Catalog: {{ metadata.database_name }}</h1>
        
        <div class="metadata">
            <p><strong>Generated:</strong> {{ metadata.generated_at }}</p>
            <p><strong>Tables:</strong> {{ metadata.table_count }}</p>
            <p><strong>Foreign Key Hints:</strong> {{ metadata.foreign_key_hint_count }}</p>
        </div>

        <div class="toc">
            <h2>📑 Table of Contents</h2>
            <ul>
                {% for table in tables %}
                <li><a href="#{{ table.name }}">{{ table.name }}</a></li>
                {% endfor %}
            </ul>
        </div>

        <h2>📋 Tables</h2>

        {% for table in tables %}
        <div class="table-card" id="{{ table.name }}">
            <h3>{{ table.name }}</h3>
            
            <p><strong>Description:</strong> {{ table.description or 'N/A' }}</p>
            
            {% if table.business_context %}
            <p><strong>Business Context:</strong> {{ table.business_context }}</p>
            {% endif %}
            
            {% if table.tags %}
            <div class="tags">
                {% for tag in table.tags %}
                <span class="tag">{{ tag }}</span>
                {% endfor %}
            </div>
            {% endif %}
            
            {% if table.data_quality_notes %}
            <p><strong>Data Quality Notes:</strong> {{ table.data_quality_notes }}</p>
            {% endif %}
            
            <p><strong>Table Type:</strong> {{ table.table_type or 'N/A' }}</p>
            <p>
                <strong>Primary Keys:</strong>
                {% if table.primary_keys %}
                    <span class="code">{{ table.primary_keys | join('</span>, <span class="code">') }}</span>
                {% else %}
                    N/A
                {% endif %}
            </p>

            <h4>Columns ({{ table.columns | length }})</h4>
            <table>
                <thead>
                    <tr>
                        <th>Column Name</th>
                        <th>Data Type</th>
                        <th>Description</th>
                        <th>Tags</th>
                    </tr>
                </thead>
                <tbody>
                    {% for column in table.columns %}
                    <tr>
                        <td><span class="code">{{ column.name }}</span></td>
                        <td>{{ column.data_type }}</td>
                        <td>{{ column.description or 'N/A' }}</td>
                        <td>
                            {% if column.tags %}
                                {{ column.tags | join(', ') }}
                            {% endif %}
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endfor %}

        <h2>🔗 Foreign Key Hints</h2>

        {% if foreign_key_hints %}
        <table>
            <thead>
                <tr>
                    <th>Source</th>
                    <th>Target</th>
                    <th>Constraint</th>
                    <th>Hint Source</th>
                </tr>
            </thead>
            <tbody>
                {% for hint in foreign_key_hints %}
                <tr>
                    <td><span class="code">{{ hint.source_table }}.{{ hint.source_column }}</span></td>
                    <td><span class="code">{{ hint.target_table }}.{{ hint.target_column }}</span></td>
                    <td>{{ hint.constraint_name or 'N/A' }}</td>
                    <td>{{ hint.hint_source }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <p>No foreign key hints found in source metadata.</p>
        {% endif %}

        <hr style="margin-top: 40px; border: none; border-top: 1px solid #ddd;">
        <p style="text-align: center; color: #7f8c8d; margin-top: 20px;">
            <em>Generated by Data Catalog Generator</em>
        </p>
    </div>
</body>
</html>
"""
        
        template = Template(html_template)
        content = template.render(**catalog_data)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Generated HTML catalog: {filepath}")
        return filepath
