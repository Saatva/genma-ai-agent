"""
Athena Schema Extractor Module

This module connects to Amazon Athena and extracts database schema information
including table names, column names, data types, and other metadata.
"""

import boto3
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    """Represents metadata for a single column"""
    name: str
    data_type: str
    comment: Optional[str] = None
    is_nullable: bool = True
    ordinal_position: int = 0


@dataclass
class TableMetadata:
    """Represents metadata for a single table"""
    name: str
    columns: List[ColumnMetadata] = field(default_factory=list)
    table_type: Optional[str] = None
    comment: Optional[str] = None
    created_time: Optional[datetime] = None
    location: Optional[str] = None
    row_count: Optional[int] = None
    primary_keys: List[str] = field(default_factory=list)


class AthenaSchemaExtractor:
    """Extracts schema information from Amazon Athena databases"""
    
    def __init__(
        self,
        database: str,
        region: str = 'us-east-1',
        s3_output_location: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None
    ):
        """
        Initialize the Athena schema extractor
        
        Args:
            database: Name of the Athena database
            region: AWS region
            s3_output_location: S3 location for query results
            aws_access_key_id: AWS access key (optional, uses default credentials if not provided)
            aws_secret_access_key: AWS secret key (optional)
        """
        self.database = database
        self.region = region
        self.s3_output_location = s3_output_location
        
        # Initialize AWS clients
        session_kwargs = {'region_name': region}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs.update({
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key
            })
        
        self.session = boto3.Session(**session_kwargs)
        self.athena_client = self.session.client('athena')
        self.glue_client = self.session.client('glue')
        
        logger.info(f"Initialized AthenaSchemaExtractor for database: {database}")
    
    def get_tables(self, exclude_patterns: Optional[List[str]] = None, include_patterns: Optional[List[str]] = None) -> List[str]:
        """
        Get list of all tables in the database
        
        Args:
            exclude_patterns: List of patterns to exclude (e.g., ['temp_*', 'test_*'])
            include_patterns: List of patterns to include (e.g., ['magento_*']). If specified, ONLY matching tables are returned.
        
        Returns:
            List of table names
        """
        try:
            tables = []
            paginator = self.glue_client.get_paginator('get_tables')
            
            for page in paginator.paginate(DatabaseName=self.database):
                for table in page['TableList']:
                    table_name = table['Name']
                    
                    # Apply inclusion patterns first (if specified)
                    if include_patterns:
                        should_include = False
                        for pattern in include_patterns:
                            if self._matches_pattern(table_name, pattern):
                                should_include = True
                                break
                        if not should_include:
                            continue
                    
                    # Apply exclusion patterns
                    if exclude_patterns:
                        should_exclude = False
                        for pattern in exclude_patterns:
                            if self._matches_pattern(table_name, pattern):
                                should_exclude = True
                                break
                        if should_exclude:
                            continue
                    
                    tables.append(table_name)
            
            logger.info(f"Found {len(tables)} tables in database {self.database}")
            return tables
            
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            raise
    
    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """
        Get detailed metadata for a specific table
        
        Args:
            table_name: Name of the table
        
        Returns:
            TableMetadata object with complete table information
        """
        try:
            response = self.glue_client.get_table(
                DatabaseName=self.database,
                Name=table_name
            )
            
            table = response['Table']
            
            # Extract column metadata
            columns = []
            for idx, col in enumerate(table.get('StorageDescriptor', {}).get('Columns', [])):
                column = ColumnMetadata(
                    name=col['Name'],
                    data_type=col['Type'],
                    comment=col.get('Comment'),
                    ordinal_position=idx
                )
                columns.append(column)
            
            # Add partition columns if they exist
            for idx, col in enumerate(table.get('PartitionKeys', []), start=len(columns)):
                column = ColumnMetadata(
                    name=col['Name'],
                    data_type=col['Type'],
                    comment=col.get('Comment', 'Partition key'),
                    ordinal_position=idx
                )
                columns.append(column)
            
            # Create table metadata object
            metadata = TableMetadata(
                name=table_name,
                columns=columns,
                table_type=table.get('TableType'),
                comment=table.get('Description'),
                created_time=table.get('CreateTime'),
                location=table.get('StorageDescriptor', {}).get('Location')
            )
            
            logger.info(f"Extracted metadata for table {table_name} with {len(columns)} columns")
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting metadata for table {table_name}: {e}")
            raise
    
    def get_all_tables_metadata(
        self,
        exclude_patterns: Optional[List[str]] = None,
        include_patterns: Optional[List[str]] = None,
        max_tables: Optional[int] = None
    ) -> Dict[str, TableMetadata]:
        """
        Get metadata for all tables in the database
        
        Args:
            exclude_patterns: List of patterns to exclude
            include_patterns: List of patterns to include (if specified, ONLY these tables are processed)
            max_tables: Maximum number of tables to process (for testing)
        
        Returns:
            Dictionary mapping table names to TableMetadata objects
        """
        tables = self.get_tables(exclude_patterns, include_patterns)
        
        if max_tables:
            tables = tables[:max_tables]
        
        all_metadata = {}
        for table_name in tables:
            try:
                metadata = self.get_table_metadata(table_name)
                all_metadata[table_name] = metadata
            except Exception as e:
                logger.warning(f"Skipping table {table_name} due to error: {e}")
                continue
        
        logger.info(f"Successfully extracted metadata for {len(all_metadata)} tables")
        return all_metadata
    
    def _matches_pattern(self, text: str, pattern: str) -> bool:
        """
        Simple pattern matching (supports * wildcard)
        
        Args:
            text: Text to match against
            pattern: Pattern with * wildcard
        
        Returns:
            True if text matches pattern
        """
        if '*' not in pattern:
            return text == pattern
        
        if pattern.startswith('*') and pattern.endswith('*'):
            return pattern.strip('*') in text
        elif pattern.startswith('*'):
            return text.endswith(pattern[1:])
        elif pattern.endswith('*'):
            return text.startswith(pattern[:-1])
        
        return False

    def get_sample_data(
        self,
        table_name: str,
        limit: int = 5
    ) -> Optional[List[Dict]]:
        """
        Get sample rows from a table (useful for additional context)
        
        Args:
            table_name: Name of the table
            limit: Number of rows to retrieve
        
        Returns:
            List of row dictionaries or None if error
        """
        if not self.s3_output_location:
            logger.warning("S3 output location not configured, skipping sample data")
            return None
        
        try:
            query = f"SELECT * FROM {self.database}.{table_name} LIMIT {limit}"
            
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.s3_output_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Wait for query to complete
            import time
            max_attempts = 30
            for _ in range(max_attempts):
                query_status = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                status = query_status['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    logger.error(f"Query failed with status: {status}")
                    return None
                
                time.sleep(1)
            
            # Get results
            results = self.athena_client.get_query_results(
                QueryExecutionId=query_execution_id,
                MaxResults=limit + 1  # +1 for header row
            )
            
            # Parse results
            rows = []
            if len(results['ResultSet']['Rows']) > 1:
                headers = [col['VarCharValue'] for col in results['ResultSet']['Rows'][0]['Data']]
                for row in results['ResultSet']['Rows'][1:]:
                    row_dict = {}
                    for idx, col in enumerate(row['Data']):
                        row_dict[headers[idx]] = col.get('VarCharValue')
                    rows.append(row_dict)
            
            return rows
            
        except Exception as e:
            logger.warning(f"Error getting sample data for {table_name}: {e}")
            return None
