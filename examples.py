"""
Example script demonstrating programmatic usage of the catalog generator
"""

from src.main import CatalogPipeline
from src.config import ConfigManager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_basic_usage():
    """Basic usage example"""
    print("\n" + "="*80)
    print("Example 1: Basic Usage")
    print("="*80)
    
    # Initialize and run pipeline
    pipeline = CatalogPipeline()
    output_files = pipeline.run()
    
    print("\nGenerated files:")
    for format_type, filepath in output_files.items():
        print(f"  - {format_type}: {filepath}")


def example_custom_config():
    """Example with custom configuration"""
    print("\n" + "="*80)
    print("Example 2: Custom Configuration")
    print("="*80)
    
    # Initialize with custom config files
    pipeline = CatalogPipeline(
        config_file='config.yaml',
        env_file='.env'
    )
    
    # Run the pipeline
    output_files = pipeline.run()
    
    print(f"\nCatalog generated successfully!")


def example_config_inspection():
    """Example showing how to inspect configuration"""
    print("\n" + "="*80)
    print("Example 3: Configuration Inspection")
    print("="*80)
    
    config_manager = ConfigManager()
    
    # Get configuration objects
    aws_config = config_manager.get_aws_config()
    ai_config = config_manager.get_ai_config()
    output_config = config_manager.get_output_config()
    
    print(f"\nAWS Configuration:")
    print(f"  Database: {aws_config.athena_database}")
    print(f"  Region: {aws_config.region}")
    
    print(f"\nAI Configuration:")
    print(f"  Provider: {ai_config.provider}")
    print(f"  Model: {ai_config.model}")
    print(f"  Temperature: {ai_config.temperature}")
    
    print(f"\nOutput Configuration:")
    print(f"  Formats: {output_config.formats}")
    print(f"  Directory: {output_config.directory}")


def example_step_by_step():
    """Example showing step-by-step pipeline execution"""
    print("\n" + "="*80)
    print("Example 4: Step-by-Step Execution")
    print("="*80)
    
    from src.schema_extractor import AthenaSchemaExtractor
    from src.semantic_analyzer import SemanticAnalyzer, create_ai_provider
    from src.relationship_detector import RelationshipDetector
    from src.catalog_generator import CatalogGenerator
    
    # Initialize configuration
    config_manager = ConfigManager()
    aws_config = config_manager.get_aws_config()
    ai_config = config_manager.get_ai_config()
    
    # Step 1: Extract schema
    print("\nStep 1: Extracting schema...")
    extractor = AthenaSchemaExtractor(
        database=aws_config.athena_database,
        region=aws_config.region
    )
    tables_metadata = extractor.get_all_tables_metadata(max_tables=3)
    print(f"  Found {len(tables_metadata)} tables")
    
    # Step 2: Analyze with AI
    print("\nStep 2: Generating descriptions...")
    ai_provider = create_ai_provider(
        ai_config.provider,
        ai_config.api_key,
        ai_config.model
    )
    analyzer = SemanticAnalyzer(ai_provider)
    
    table_descriptions = {}
    for table_name, metadata in tables_metadata.items():
        columns = [{'name': col.name, 'data_type': col.data_type} 
                  for col in metadata.columns]
        desc = analyzer.analyze_table(table_name, columns)
        table_descriptions[table_name] = desc
        print(f"  Analyzed: {table_name}")
    
    # Step 3: Detect relationships
    print("\nStep 3: Detecting relationships...")
    detector = RelationshipDetector()
    relationships = detector.detect_relationships(tables_metadata)
    print(f"  Found {len(relationships)} relationships")
    
    # Step 4: Generate catalog
    print("\nStep 4: Generating catalog...")
    generator = CatalogGenerator()
    output_files = generator.generate_catalog(
        database_name=aws_config.athena_database,
        tables_metadata=tables_metadata,
        table_descriptions=table_descriptions,
        column_descriptions={},  # Simplified for example
        relationships=relationships,
        formats=['json', 'markdown']
    )
    
    print("\nGenerated files:")
    for format_type, filepath in output_files.items():
        print(f"  - {filepath}")


if __name__ == '__main__':
    import sys
    
    print("\n" + "="*80)
    print("Data Catalog Generator - Usage Examples")
    print("="*80)
    print("\nNote: Make sure to configure .env and config.yaml before running!")
    print("\nAvailable examples:")
    print("  1. Basic usage")
    print("  2. Custom configuration")
    print("  3. Configuration inspection")
    print("  4. Step-by-step execution")
    
    if len(sys.argv) > 1:
        example_num = sys.argv[1]
        
        if example_num == '1':
            example_basic_usage()
        elif example_num == '2':
            example_custom_config()
        elif example_num == '3':
            example_config_inspection()
        elif example_num == '4':
            example_step_by_step()
        else:
            print(f"\nUnknown example: {example_num}")
    else:
        print("\nUsage: python examples.py <example_number>")
        print("Example: python examples.py 3")
