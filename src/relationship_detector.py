"""
Relationship Detection Engine

This module identifies potential foreign key relationships between tables
based on naming patterns, data types, and domain context, with confidence scoring.
"""

import logging
import re
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class RelationshipType(Enum):
    """Types of relationships that can be detected"""
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"
    ONE_TO_ONE = "one_to_one"


@dataclass
class Relationship:
    """Represents a detected relationship between tables"""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: RelationshipType
    confidence_score: float  # 0.0 to 1.0
    detection_method: str
    reasoning: str
    
    def __repr__(self):
        return (f"{self.source_table}.{self.source_column} -> "
                f"{self.target_table}.{self.target_column} "
                f"({self.confidence_score:.2f})")


class RelationshipDetector:
    """Detects potential relationships between database tables"""
    
    def __init__(
        self,
        id_patterns: Optional[List[str]] = None,
        common_suffixes: Optional[List[str]] = None,
        min_confidence: float = 0.6
    ):
        """
        Initialize the relationship detector
        
        Args:
            id_patterns: Regex patterns that identify ID columns
            common_suffixes: Common ID column suffixes
            min_confidence: Minimum confidence score to include a relationship
        """
        self.id_patterns = id_patterns or [
            r"_id$",
            r"^id_",
            r"_key$",
            r"^fk_",
            r"_fk$"
        ]
        
        self.common_suffixes = common_suffixes or [
            "user_id", "customer_id", "account_id", "product_id",
            "order_id", "transaction_id", "payment_id", "item_id",
            "category_id", "group_id", "team_id", "organization_id"
        ]
        
        self.min_confidence = min_confidence
        
        # Compile regex patterns
        self.id_pattern_regex = [re.compile(pattern, re.IGNORECASE) for pattern in self.id_patterns]
        
        logger.info(f"Initialized RelationshipDetector with min_confidence={min_confidence}")
    
    def detect_relationships(
        self,
        tables_metadata: Dict[str, any]
    ) -> List[Relationship]:
        """
        Detect all relationships across tables
        
        Args:
            tables_metadata: Dictionary of table metadata objects
        
        Returns:
            List of detected Relationship objects
        """
        relationships = []
        
        # Build indexes for efficient lookup
        table_columns = self._build_table_column_index(tables_metadata)
        primary_keys = self._identify_primary_keys(tables_metadata)
        
        # Iterate through all tables and columns
        for source_table, metadata in tables_metadata.items():
            for column in metadata.columns:
                column_name = column.name.lower()
                
                # Check if this looks like a foreign key column
                if self._is_potential_foreign_key(column_name):
                    # Find potential target tables
                    potential_relationships = self._find_target_tables(
                        source_table=source_table,
                        source_column=column.name,
                        source_type=column.data_type,
                        table_columns=table_columns,
                        primary_keys=primary_keys,
                        all_tables=list(tables_metadata.keys())
                    )
                    
                    relationships.extend(potential_relationships)
        
        # Filter by confidence and deduplicate
        relationships = [r for r in relationships if r.confidence_score >= self.min_confidence]
        relationships = self._deduplicate_relationships(relationships)
        
        # Sort by confidence score (descending)
        relationships.sort(key=lambda r: r.confidence_score, reverse=True)
        
        logger.info(f"Detected {len(relationships)} relationships with confidence >= {self.min_confidence}")
        return relationships
    
    def _is_potential_foreign_key(self, column_name: str) -> bool:
        """Check if a column name suggests it's a foreign key"""
        # Check against regex patterns
        for pattern in self.id_pattern_regex:
            if pattern.search(column_name):
                return True
        
        # Check against common suffixes
        for suffix in self.common_suffixes:
            if column_name.endswith(suffix.lower()):
                return True
        
        return False
    
    def _find_target_tables(
        self,
        source_table: str,
        source_column: str,
        source_type: str,
        table_columns: Dict[str, Dict[str, str]],
        primary_keys: Dict[str, Set[str]],
        all_tables: List[str]
    ) -> List[Relationship]:
        """Find potential target tables for a foreign key column"""
        relationships = []
        source_column_lower = source_column.lower()
        
        # Strategy 1: Exact match on column name with primary key
        for target_table in all_tables:
            if target_table == source_table:
                continue
            
            if target_table in table_columns:
                for target_column, target_type in table_columns[target_table].items():
                    target_column_lower = target_column.lower()
                    
                    # Exact column name match
                    if source_column_lower == target_column_lower:
                        # Check if types are compatible
                        if self._are_types_compatible(source_type, target_type):
                            confidence = 0.9
                            # Boost if target column is a primary key
                            if target_column_lower in primary_keys.get(target_table, set()):
                                confidence = 0.95
                            
                            relationships.append(Relationship(
                                source_table=source_table,
                                source_column=source_column,
                                target_table=target_table,
                                target_column=target_column,
                                relationship_type=RelationshipType.MANY_TO_ONE,
                                confidence_score=confidence,
                                detection_method="exact_column_match",
                                reasoning=f"Exact column name match with compatible types"
                            ))
        
        # Strategy 2: Table name embedded in column name
        # e.g., user_id -> users.id or user.user_id
        inferred_table = self._infer_table_from_column(source_column_lower)
        
        if inferred_table:
            # Look for tables with similar names
            for target_table in all_tables:
                if target_table == source_table:
                    continue
                
                target_table_lower = target_table.lower()
                
                # Check if table name matches (with pluralization handling)
                if self._tables_match(inferred_table, target_table_lower):
                    # Look for id or primary key columns in target
                    pk_columns = primary_keys.get(target_table, set())
                    
                    if pk_columns:
                        for pk_col in pk_columns:
                            if target_table in table_columns:
                                target_type = table_columns[target_table].get(pk_col)
                                
                                if target_type and self._are_types_compatible(source_type, target_type):
                                    confidence = 0.85
                                    
                                    relationships.append(Relationship(
                                        source_table=source_table,
                                        source_column=source_column,
                                        target_table=target_table,
                                        target_column=pk_col,
                                        relationship_type=RelationshipType.MANY_TO_ONE,
                                        confidence_score=confidence,
                                        detection_method="table_name_inference",
                                        reasoning=f"Column '{source_column}' suggests reference to '{target_table}'"
                                    ))
                    else:
                        # No explicit PK, look for 'id' column
                        if target_table in table_columns:
                            for target_column, target_type in table_columns[target_table].items():
                                if target_column.lower() in ['id', f"{target_table.lower()}_id"]:
                                    if self._are_types_compatible(source_type, target_type):
                                        relationships.append(Relationship(
                                            source_table=source_table,
                                            source_column=source_column,
                                            target_table=target_table,
                                            target_column=target_column,
                                            relationship_type=RelationshipType.MANY_TO_ONE,
                                            confidence_score=0.75,
                                            detection_method="table_name_inference_with_id",
                                            reasoning=f"Inferred from naming pattern"
                                        ))
        
        # Strategy 3: Pattern-based matching (e.g., created_by -> users.id)
        semantic_matches = self._find_semantic_matches(
            source_column_lower,
            source_type,
            table_columns,
            primary_keys,
            all_tables,
            source_table
        )
        relationships.extend(semantic_matches)
        
        return relationships
    
    def _infer_table_from_column(self, column_name: str) -> Optional[str]:
        """Infer target table name from column name"""
        # Remove common suffixes
        for suffix in ['_id', '_key', '_fk', 'id', 'key']:
            if column_name.endswith(suffix):
                inferred = column_name[:-len(suffix)].rstrip('_')
                if inferred:
                    return inferred
        
        # Check if it starts with fk_ or id_
        for prefix in ['fk_', 'id_']:
            if column_name.startswith(prefix):
                inferred = column_name[len(prefix):]
                if inferred:
                    return inferred
        
        return None
    
    def _tables_match(self, name1: str, name2: str) -> bool:
        """Check if two table names match (accounting for pluralization)"""
        if name1 == name2:
            return True
        
        # Check singular/plural variations
        if name1 + 's' == name2 or name1 == name2 + 's':
            return True
        
        if name1.endswith('s') and name1[:-1] == name2:
            return True
        
        if name2.endswith('s') and name2[:-1] == name1:
            return True
        
        # Handle irregular plurals (simple cases)
        irregular_plurals = {
            'person': 'people', 'child': 'children', 'man': 'men',
            'woman': 'women', 'tooth': 'teeth', 'foot': 'feet'
        }
        
        if name1 in irregular_plurals and irregular_plurals[name1] == name2:
            return True
        if name2 in irregular_plurals and irregular_plurals[name2] == name1:
            return True
        
        return False
    
    def _find_semantic_matches(
        self,
        source_column: str,
        source_type: str,
        table_columns: Dict[str, Dict[str, str]],
        primary_keys: Dict[str, Set[str]],
        all_tables: List[str],
        source_table: str
    ) -> List[Relationship]:
        """Find relationships based on semantic patterns"""
        relationships = []
        
        # Semantic patterns (column name -> likely table)
        semantic_patterns = {
            'created_by': ['users', 'user', 'employees', 'employee'],
            'updated_by': ['users', 'user', 'employees', 'employee'],
            'modified_by': ['users', 'user', 'employees', 'employee'],
            'owner_id': ['users', 'user', 'owners', 'owner'],
            'assigned_to': ['users', 'user', 'employees', 'employee'],
            'parent_id': [source_table],  # Self-referencing
        }
        
        for pattern, target_tables in semantic_patterns.items():
            if pattern in source_column:
                for potential_target in target_tables:
                    # Find matching table
                    for table in all_tables:
                        if self._tables_match(potential_target, table.lower()):
                            # Find primary key in target table
                            pk_columns = primary_keys.get(table, set())
                            
                            for pk_col in pk_columns or ['id']:
                                if table in table_columns:
                                    target_type = table_columns[table].get(pk_col)
                                    
                                    if target_type and self._are_types_compatible(source_type, target_type):
                                        # Self-referencing relationship
                                        rel_type = (RelationshipType.MANY_TO_ONE if table != source_table 
                                                   else RelationshipType.ONE_TO_MANY)
                                        
                                        relationships.append(Relationship(
                                            source_table=source_table,
                                            source_column=source_column,
                                            target_table=table,
                                            target_column=pk_col,
                                            relationship_type=rel_type,
                                            confidence_score=0.70,
                                            detection_method="semantic_pattern",
                                            reasoning=f"Semantic pattern '{pattern}' suggests reference to {table}"
                                        ))
        
        return relationships
    
    def _are_types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two data types are compatible for a relationship"""
        # Normalize types
        t1 = type1.lower()
        t2 = type2.lower()
        
        # Exact match
        if t1 == t2:
            return True
        
        # Integer types
        int_types = {'int', 'integer', 'bigint', 'smallint', 'tinyint'}
        if any(it in t1 for it in int_types) and any(it in t2 for it in int_types):
            return True
        
        # String types
        string_types = {'varchar', 'char', 'string', 'text'}
        if any(st in t1 for st in string_types) and any(st in t2 for st in string_types):
            return True
        
        return False
    
    def _build_table_column_index(
        self,
        tables_metadata: Dict[str, any]
    ) -> Dict[str, Dict[str, str]]:
        """Build index of table -> {column: type}"""
        index = {}
        
        for table_name, metadata in tables_metadata.items():
            index[table_name] = {}
            for column in metadata.columns:
                index[table_name][column.name] = column.data_type
        
        return index
    
    def _identify_primary_keys(
        self,
        tables_metadata: Dict[str, any]
    ) -> Dict[str, Set[str]]:
        """Identify likely primary key columns for each table"""
        primary_keys = {}
        
        for table_name, metadata in tables_metadata.items():
            pk_candidates = set()
            
            for column in metadata.columns:
                col_name_lower = column.name.lower()
                
                # Common primary key patterns
                if col_name_lower in ['id', f'{table_name.lower()}_id', 'pk']:
                    pk_candidates.add(column.name)
                elif col_name_lower.endswith('_pk'):
                    pk_candidates.add(column.name)
                # First column named 'id' is often the primary key
                elif col_name_lower == 'id' and column.ordinal_position == 0:
                    pk_candidates.add(column.name)
            
            if pk_candidates:
                primary_keys[table_name] = pk_candidates
        
        return primary_keys
    
    def _deduplicate_relationships(
        self,
        relationships: List[Relationship]
    ) -> List[Relationship]:
        """Remove duplicate relationships, keeping highest confidence"""
        seen = {}
        
        for rel in relationships:
            key = (rel.source_table, rel.source_column, rel.target_table, rel.target_column)
            
            if key not in seen or rel.confidence_score > seen[key].confidence_score:
                seen[key] = rel
        
        return list(seen.values())
