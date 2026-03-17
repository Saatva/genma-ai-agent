"""
Main Orchestrator

Coordinates the entire data catalog generation pipeline.
"""

import logging
import sys
from typing import Optional
from pathlib import Path

from .config import ConfigManager
from .schema_extractor import AthenaSchemaExtractor
from .semantic_analyzer import SemanticAnalyzer, create_ai_provider
from .relationship_detector import RelationshipDetector
from .catalog_generator import CatalogGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('catalog_generator.log')
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
        self.output_config = self.config_manager.get_output_config()
        self.relationship_config = self.config_manager.get_relationship_config()
        self.extraction_config = self.config_manager.get_extraction_config()
        
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
        
        # Relationship detector
        self.relationship_detector = RelationshipDetector(
            id_patterns=self.relationship_config.id_patterns,
            common_suffixes=self.relationship_config.common_suffixes,
            min_confidence=self.relationship_config.min_confidence
        )
        
        # Catalog generator
        self.catalog_generator = CatalogGenerator(
            output_dir=self.output_config.directory
        )
    
    def run(self):
        """Execute the complete catalog generation pipeline"""
        try:
            logger.info("=" * 80)
            logger.info("Starting Data Catalog Generation Pipeline")
            logger.info("=" * 80)
            
            # Step 1: Extract schema from Athena
            logger.info("\n[1/4] Extracting schema from Athena...")
            tables_metadata = self.schema_extractor.get_all_tables_metadata(
                include_patterns=self.extraction_config.include_tables,
                exclude_patterns=self.extraction_config.exclude_tables,
                max_tables=self.extraction_config.max_tables
            )
            logger.info(f"✓ Extracted metadata for {len(tables_metadata)} tables")
            
            if not tables_metadata:
                logger.warning("No tables found. Exiting.")
                return
            
            # Step 2: Generate semantic descriptions with AI
            logger.info("\n[2/4] Generating semantic descriptions with AI...")
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
            
            logger.info(f"✓ Generated descriptions for {len(table_descriptions)} tables")
            
            # Step 3: Detect relationships
            logger.info("\n[3/4] Detecting relationships between tables...")
            relationships = self.relationship_detector.detect_relationships(tables_metadata)
            logger.info(f"✓ Detected {len(relationships)} relationships")
            
            # Log top relationships
            if relationships:
                logger.info("  Top relationships:")
                for rel in relationships[:5]:
                    logger.info(f"    {rel}")
            
            # Step 4: Generate catalog files
            logger.info("\n[4/4] Generating catalog files...")
            output_files = self.catalog_generator.generate_catalog(
                database_name=self.aws_config.athena_database,
                tables_metadata=tables_metadata,
                table_descriptions=table_descriptions,
                column_descriptions=column_descriptions,
                relationships=relationships,
                formats=self.output_config.formats,
                include_confidence=self.output_config.include_confidence,
                timestamp_filenames=self.output_config.timestamp_filenames
            )
            
            logger.info("✓ Catalog generation complete!")
            logger.info("\n" + "=" * 80)
            logger.info("Generated Files:")
            logger.info("=" * 80)
            for format_type, filepath in output_files.items():
                logger.info(f"  {format_type.upper():10s}: {filepath}")
            
            logger.info("\n" + "=" * 80)
            logger.info("Pipeline completed successfully!")
            logger.info("=" * 80)
            
            return output_files
            
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
