"""
Main Orchestrator

Coordinates the entire data catalog generation pipeline.
"""

import logging
import sys
import csv
from typing import Optional, Dict, Any, List
from pathlib import Path

from .config import ConfigManager
from .schema_extractor import AthenaSchemaExtractor
from .semantic_analyzer import SemanticAnalyzer, create_ai_provider
from .confluence_publisher import ConfluencePublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('catalog_pipeline.log')
    ]
)

logger = logging.getLogger(__name__)


class CatalogPipeline:
    """Orchestrates the complete catalog generation pipeline"""
    
    def __init__(self, config_file: Optional[str] = None, env_file: Optional[str] = None):
        """
        Initialize the catalog pipeline
        
        Args:
            config_file: Path to YAML configuration file
            env_file: Path to .env file
        """
        logger.info("Initializing Data Catalog Generator Pipeline")
        
        # Load configuration
        self.config_manager = ConfigManager(config_file, env_file)
        self.config_manager.validate()
        
        # Get configuration objects
        self.aws_config = self.config_manager.get_aws_config()
        self.ai_config = self.config_manager.get_ai_config()
        self.extraction_config = self.config_manager.get_extraction_config()
        self.confluence_config = self.config_manager.get_confluence_config()
        
        # Initialize components
        self._initialize_components()
        
        logger.info("Pipeline initialization complete")
    
    def _initialize_components(self):
        """Initialize all pipeline components"""
        # Schema extractor
        self.schema_extractor = AthenaSchemaExtractor(
            database=self.aws_config.athena_database,
            region=self.aws_config.region,
            s3_output_location=self.aws_config.athena_s3_output,
            aws_access_key_id=self.aws_config.access_key_id,
            aws_secret_access_key=self.aws_config.secret_access_key
        )
        
        # AI provider and semantic analyzer
        ai_provider = create_ai_provider(
            provider_name=self.ai_config.provider,
            api_key=self.ai_config.api_key,
            model=self.ai_config.model
        )
        
        self.semantic_analyzer = SemanticAnalyzer(
            ai_provider=ai_provider,
            temperature=self.ai_config.temperature,
            max_tokens=self.ai_config.max_tokens
        )
        
        if not self.confluence_config.enabled:
            raise ValueError("Confluence publishing must be enabled")

        self.confluence_publisher = ConfluencePublisher(
            base_url=self.confluence_config.base_url,
            space_key=self.confluence_config.space_key,
            username=self.confluence_config.username,
            api_token=self.confluence_config.api_token,
            folder_name=self.confluence_config.folder_name,
            parent_page_id=self.confluence_config.parent_page_id,
            page_title_prefix=self.confluence_config.page_title_prefix,
        )

    def _load_primary_key_map(self, csv_path: str = 'src/primary_keys/magento_primary_keys.csv') -> Dict[str, List[str]]:
        """Load table -> primary key columns map from CSV file."""
        resolved_path = Path(csv_path)
        if not resolved_path.exists():
            logger.warning("Primary key CSV not found at %s. Continuing without primary key enrichment.", csv_path)
            return {}

        primary_key_map: Dict[str, List[str]] = {}

        with resolved_path.open('r', encoding='utf-8', newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                table_name = (row.get('table_name') or '').strip().strip('"')
                pk_value = (row.get('primary_key_columns') or '').strip()

                if not table_name or not pk_value:
                    continue

                # Handles both single key values and quoted comma-separated composite keys.
                columns = [
                    col.strip() for col in pk_value.replace('"', '').split(',') if col.strip()
                ]

                if columns:
                    primary_key_map[table_name] = columns

        logger.info("Loaded primary keys for %d tables from %s", len(primary_key_map), csv_path)
        return primary_key_map

    def _normalize_table_name_for_primary_key_lookup(self, table_name: str) -> str:
        """Normalize extracted table names to match CSV table_name values."""
        return table_name[len('magento_'):] if table_name.startswith('magento_') else table_name

    def _attach_primary_keys(
        self,
        tables_metadata: Dict[str, Any],
        primary_key_map: Dict[str, List[str]]
    ) -> None:
        """Attach primary key columns to each table metadata object."""
        matched = 0

        for table_name, metadata in tables_metadata.items():
            normalized_table_name = self._normalize_table_name_for_primary_key_lookup(table_name)
            primary_keys = primary_key_map.get(normalized_table_name, [])
            metadata.primary_keys = primary_keys
            if primary_keys:
                matched += 1
    
    def run(self):
        """Execute the complete catalog generation pipeline"""
        try:
            logger.info("=" * 80)
            logger.info("Starting Data Catalog Generation Pipeline")
            logger.info("=" * 80)

            # Step 1: Extract schema from Athena
            logger.info(f"\nExtracting schema from Athena...")
            tables_metadata = self.schema_extractor.get_all_tables_metadata(
                include_patterns=self.extraction_config.include_tables,
                exclude_patterns=self.extraction_config.exclude_tables,
                max_tables=self.extraction_config.max_tables
            )
            logger.info(f"Extracted metadata for {len(tables_metadata)} tables")
            
            if not tables_metadata:
                logger.warning("No tables found. Exiting.")
                return

            # Enrich tables with primary keys from CSV
            primary_key_map = self._load_primary_key_map()
            self._attach_primary_keys(tables_metadata, primary_key_map)
            
            # Step 2: Generate semantic descriptions with AI
            logger.info(f"\nGenerating semantic descriptions with AI...")
            table_descriptions = {}
            column_descriptions = {}
            
            for idx, (table_name, metadata) in enumerate(tables_metadata.items(), 1):
                logger.info(f"  Processing table {idx}/{len(tables_metadata)}: {table_name}")
                
                # Analyze table
                columns_list = [
                    {'name': col.name, 'data_type': col.data_type}
                    for col in metadata.columns
                ]
                
                table_desc = self.semantic_analyzer.analyze_table(
                    table_name=table_name,
                    columns=columns_list,
                    table_comment=metadata.comment
                )
                table_descriptions[table_name] = table_desc
                
                # Analyze columns in batch
                col_descs = self.semantic_analyzer.analyze_columns_batch(
                    table_name=table_name,
                    columns=columns_list,
                    table_context=table_desc.description,
                    max_tokens_per_chunk=self.ai_config.max_chunk_tokens
                )
                column_descriptions[table_name] = col_descs
            
            logger.info(f"Generated descriptions for {len(table_descriptions)} tables")

            logger.info("\nPublishing one Confluence page per table...")
            publish_result = self.confluence_publisher.publish_tables(
                database_name=self.aws_config.athena_database,
                tables_metadata=tables_metadata,
                table_descriptions=table_descriptions,
                column_descriptions=column_descriptions,
            )
            logger.info(
                "Published %d table pages under folder page ID %s",
                len(publish_result.get('published_pages', [])),
                publish_result.get('folder_page_id')
            )
            
            logger.info("Catalog generation complete!")
            
            logger.info("\n" + "=" * 80)
            logger.info("Pipeline completed successfully!")
            logger.info("=" * 80)
            
            return publish_result
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Generate AI-powered data catalog from Amazon Athena database'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to YAML configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--env',
        type=str,
        help='Path to .env file (default: .env)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run pipeline
    try:
        pipeline = CatalogPipeline(
            config_file=args.config,
            env_file=args.env
        )
        pipeline.run()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
