"""
Configuration Manager

Handles loading and managing configuration from environment variables and YAML files.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass
class AWSConfig:
    """AWS configuration"""
    region: str
    access_key_id: Optional[str]
    secret_access_key: Optional[str]
    athena_database: str
    athena_s3_output: Optional[str]


@dataclass
class AIConfig:
    """AI provider configuration"""
    provider: str  # 'anthropic' or 'openai'
    api_key: str
    model: str
    temperature: float = 0.3
    max_tokens: int = 8000  # Output token limit
    max_chunk_tokens: int = 2000  # Total tokens per chunk (input + output)
    batch_size: int = 5


@dataclass
class ExtractionConfig:
    """Schema extraction configuration"""
    include_tables: list
    exclude_tables: list
    max_tables: Optional[int]


@dataclass
class ConfluenceConfig:
    """Confluence publishing configuration"""
    enabled: bool = False
    base_url: str = ""
    space_key: str = ""
    username: str = ""
    api_token: str = ""
    folder_name: str = "Data Catalogs"
    parent_page_id: Optional[str] = None
    page_title_prefix: str = "Catalog"


class ConfigManager:
    """Manages application configuration"""
    
    def __init__(self, config_file: Optional[str] = None, env_file: Optional[str] = None):
        """
        Initialize configuration manager
        
        Args:
            config_file: Path to YAML config file
            env_file: Path to .env file
        """
        # Load environment variables
        if env_file and Path(env_file).exists():
            load_dotenv(env_file)
        else:
            load_dotenv()  # Load from default .env
        
        # Load YAML config
        self.yaml_config = {}
        if config_file and Path(config_file).exists():
            with open(config_file, 'r') as f:
                self.yaml_config = yaml.safe_load(f) or {}
        elif Path('config.yaml').exists():
            with open('config.yaml', 'r') as f:
                self.yaml_config = yaml.safe_load(f) or {}
        
        logger.info("Configuration loaded successfully")
    
    def get_aws_config(self) -> AWSConfig:
        """Get AWS configuration"""
        return AWSConfig(
            region=os.getenv('AWS_REGION', 'us-east-1'),
            access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            athena_database=os.getenv('ATHENA_DATABASE', ''),
            athena_s3_output=os.getenv('ATHENA_S3_OUTPUT')
        )
    
    def get_ai_config(self) -> AIConfig:
        """Get AI provider configuration"""
        provider = os.getenv('AI_PROVIDER', 'anthropic').lower()
        
        if provider == 'anthropic':
            api_key = os.getenv('ANTHROPIC_API_KEY', '')
            model = os.getenv('AI_MODEL', 'claude-3-5-sonnet-20241022')
        elif provider == 'openai':
            api_key = os.getenv('OPENAI_API_KEY', '')
            model = os.getenv('AI_MODEL', 'gpt-4')
        elif provider == 'gemini':
            api_key = os.getenv('GEMINI_API_KEY', '')
            model = os.getenv('AI_MODEL', 'gemini-1.5-pro')
        else:
            raise ValueError(f"Unknown AI provider: {provider}. Supported: anthropic, openai, gemini")
        
        ai_settings = self.yaml_config.get('ai_analysis', {})
        
        return AIConfig(
            provider=provider,
            api_key=api_key,
            model=model,
            temperature=ai_settings.get('temperature', 0.3),
            max_tokens=ai_settings.get('max_tokens', 8000),
            max_chunk_tokens=ai_settings.get('max_chunk_tokens', 2000),
            batch_size=ai_settings.get('batch_size', 5)
        )
    
    def get_extraction_config(self) -> ExtractionConfig:
        """Get schema extraction configuration"""
        extraction_settings = self.yaml_config.get('extraction', {})
        
        return ExtractionConfig(
            include_tables=extraction_settings.get('include_tables', []),
            exclude_tables=extraction_settings.get('exclude_tables', []),
            max_tables=extraction_settings.get('max_tables')
        )

    def get_confluence_config(self) -> ConfluenceConfig:
        """Get Confluence publishing configuration"""
        enabled = os.getenv('CONFLUENCE_ENABLED', 'false').lower() in {
            '1', 'true', 'yes', 'on'
        }

        parent_page_id_raw = (os.getenv('CONFLUENCE_PARENT_PAGE_ID', '') or '').strip()
        parent_page_id = None if parent_page_id_raw.lower() in {'', 'none', 'null'} else parent_page_id_raw

        return ConfluenceConfig(
            enabled=enabled,
            base_url=os.getenv('CONFLUENCE_BASE_URL', '').rstrip('/'),
            space_key=os.getenv('CONFLUENCE_SPACE_KEY', ''),
            username=os.getenv('CONFLUENCE_USERNAME', ''),
            api_token=os.getenv('CONFLUENCE_API_TOKEN', ''),
            folder_name=os.getenv('CONFLUENCE_FOLDER_NAME', 'Data Catalogs'),
            parent_page_id=parent_page_id,
            page_title_prefix=os.getenv('CONFLUENCE_PAGE_TITLE_PREFIX', 'Catalog')
        )
    
    def validate(self) -> bool:
        """
        Validate configuration
        
        Returns:
            True if configuration is valid
        
        Raises:
            ValueError if configuration is invalid
        """
        aws_config = self.get_aws_config()
        if not aws_config.athena_database:
            raise ValueError("ATHENA_DATABASE is required")
        
        ai_config = self.get_ai_config()
        if not ai_config.api_key:
            raise ValueError(f"{ai_config.provider.upper()}_API_KEY is required")

        confluence_config = self.get_confluence_config()
        if confluence_config.enabled:
            required_fields = {
                'CONFLUENCE_BASE_URL': confluence_config.base_url,
                'CONFLUENCE_SPACE_KEY': confluence_config.space_key,
                'CONFLUENCE_USERNAME': confluence_config.username,
                'CONFLUENCE_API_TOKEN': confluence_config.api_token,
                'CONFLUENCE_FOLDER_NAME': confluence_config.folder_name,
            }
            missing = [name for name, value in required_fields.items() if not value]
            if missing:
                raise ValueError(
                    "Confluence publishing is enabled but required settings are missing: "
                    + ", ".join(missing)
                )
        
        logger.info("Configuration validation passed")
        return True
