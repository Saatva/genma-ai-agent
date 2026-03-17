"""
AI-Powered Semantic Analyzer

This module uses LLMs to infer semantic descriptions for database tables and columns
based on their names, data types, and context.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import json
import os

logger = logging.getLogger(__name__)


@dataclass
class SemanticDescription:
    """Represents an AI-generated semantic description"""
    description: str
    business_context: Optional[str] = None
    suggested_tags: List[str] = None
    data_quality_notes: Optional[str] = None
    
    def __post_init__(self):
        if self.suggested_tags is None:
            self.suggested_tags = []


class AIProvider:
    """Base class for AI providers"""
    
    def generate_completion(self, prompt: str, **kwargs) -> str:
        """Generate a completion from the AI model"""
        raise NotImplementedError


class AnthropicProvider(AIProvider):
    """Anthropic Claude AI provider"""
    
    def __init__(self, api_key: str, model: str = "claude-3-5-sonnet-20241022"):
        """Initialize Anthropic provider"""
        try:
            import anthropic
            self.client = anthropic.Anthropic(api_key=api_key)
            self.model = model
            logger.info(f"Initialized Anthropic provider with model: {model}")
        except ImportError:
            raise ImportError("anthropic package not installed. Install with: pip install anthropic")
    
    def generate_completion(
        self,
        prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 2000,
        **kwargs
    ) -> str:
        """Generate completion using Claude"""
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"Error generating completion with Anthropic: {e}")
            raise


class OpenAIProvider(AIProvider):
    """OpenAI GPT AI provider"""
    
    def __init__(self, api_key: str, model: str = "gpt-4"):
        """Initialize OpenAI provider"""
        try:
            import openai
            self.client = openai.OpenAI(api_key=api_key)
            self.model = model
            logger.info(f"Initialized OpenAI provider with model: {model}")
        except ImportError:
            raise ImportError("openai package not installed. Install with: pip install openai")
    
    def generate_completion(
        self,
        prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 2000,
        **kwargs
    ) -> str:
        """Generate completion using GPT"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                temperature=temperature,
                max_tokens=max_tokens
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error generating completion with OpenAI: {e}")
            raise


class GeminiProvider(AIProvider):
    """Google Gemini AI provider"""
    
    def __init__(self, api_key: str, model: str = "gemini-1.5-pro"):
        """Initialize Gemini provider"""
        try:
            from google import genai
            from google.genai import types
            self.client = genai.Client(api_key=api_key)
            self.model_name = model
            self.types = types
            logger.info(f"Initialized Gemini provider with model: {model}")
        except ImportError:
            raise ImportError("google-genai package not installed. Install with: pip install google-genai")
    
    def generate_completion(
        self,
        prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 2000,
        **kwargs
    ) -> str:
        """Generate completion using Gemini"""
        try:
            # Configure safety settings to be less restrictive for technical content
            safety_settings = [
                self.types.SafetySetting(
                    category='HARM_CATEGORY_HATE_SPEECH',
                    threshold='BLOCK_ONLY_HIGH'
                ),
                self.types.SafetySetting(
                    category='HARM_CATEGORY_DANGEROUS_CONTENT',
                    threshold='BLOCK_ONLY_HIGH'
                ),
                self.types.SafetySetting(
                    category='HARM_CATEGORY_SEXUALLY_EXPLICIT',
                    threshold='BLOCK_ONLY_HIGH'
                ),
                self.types.SafetySetting(
                    category='HARM_CATEGORY_HARASSMENT',
                    threshold='BLOCK_ONLY_HIGH'
                ),
            ]
            
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=self.types.GenerateContentConfig(
                    temperature=temperature,
                    max_output_tokens=max_tokens,
                    safety_settings=safety_settings
                )
            )
            
            # Access the text from the response
            if response.text:
                return response.text
            else:
                logger.error(f"Gemini response blocked. Finish reason: {response.candidates[0].finish_reason if response.candidates else 'unknown'}")
                raise Exception(f"Gemini blocked response: {response.candidates[0].finish_reason if response.candidates else 'unknown'}")
                
        except Exception as e:
            logger.error(f"Error generating completion with Gemini: {e}")
            raise
            raise


class SemanticAnalyzer:
    """Analyzes database schemas using AI to generate semantic descriptions"""
    
    def __init__(
        self,
        ai_provider: AIProvider,
        temperature: float = 0.3,
        max_tokens: int = 2000
    ):
        """
        Initialize the semantic analyzer
        
        Args:
            ai_provider: AI provider instance (Anthropic or OpenAI)
            temperature: Temperature for AI generation (0.0 - 1.0)
            max_tokens: Maximum tokens per API call
        """
        self.ai_provider = ai_provider
        self.temperature = temperature
        self.max_tokens = max_tokens
        logger.info("Initialized SemanticAnalyzer")
    
    def analyze_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_comment: Optional[str] = None
    ) -> SemanticDescription:
        """
        Generate semantic description for a table
        
        Args:
            table_name: Name of the table
            columns: List of column dictionaries with name and data_type
            table_comment: Existing table comment (if any)
        
        Returns:
            SemanticDescription object
        """
        # Build prompt for table analysis
        prompt = self._build_table_prompt(table_name, columns, table_comment)
        
        try:
            # Get AI response
            response = self.ai_provider.generate_completion(
                prompt,
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            # Parse response
            description = self._parse_table_response(response)
            logger.info(f"Generated semantic description for table: {table_name}")
            return description
            
        except Exception as e:
            logger.error(f"Error analyzing table {table_name}: {e}")
            # Return a basic description on error
            return SemanticDescription(
                description=f"Table: {table_name}",
                business_context="Analysis failed"
            )
    
    def analyze_column(
        self,
        table_name: str,
        column_name: str,
        data_type: str,
        column_comment: Optional[str] = None,
        table_context: Optional[str] = None
    ) -> SemanticDescription:
        """
        Generate semantic description for a column
        
        Args:
            table_name: Name of the parent table
            column_name: Name of the column
            data_type: Data type of the column
            column_comment: Existing column comment (if any)
            table_context: Context about the parent table
        
        Returns:
            SemanticDescription object
        """
        prompt = self._build_column_prompt(
            table_name, column_name, data_type, column_comment, table_context
        )
        
        try:
            response = self.ai_provider.generate_completion(
                prompt,
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            description = self._parse_column_response(response)
            logger.debug(f"Generated description for column: {table_name}.{column_name}")
            return description
            
        except Exception as e:
            logger.error(f"Error analyzing column {table_name}.{column_name}: {e}")
            return SemanticDescription(
                description=f"Column: {column_name} ({data_type})",
                business_context="Analysis failed"
            )
    
    def analyze_columns_batch(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_context: Optional[str] = None,
        max_tokens_per_chunk: int = 2000
    ) -> Dict[str, SemanticDescription]:
        """
        Analyze multiple columns using intelligent token-based chunking (Map-Reduce pattern)
        
        Strategy:
        - Estimates token count for each chunk (more conservative now)
        - Preserves semantic column groups (related fields stay together)
        - Uses Map phase: process each chunk independently
        - Uses Reduce phase: merge results with consistency check
        
        Args:
            table_name: Name of the parent table
            columns: List of column dictionaries
            table_context: Context about the parent table
            max_tokens_per_chunk: Maximum estimated tokens per chunk (~2000 default, conservative)
        
        Returns:
            Dictionary mapping column names to SemanticDescription objects
        """
        # Estimate total tokens needed (more conservative calculation)
        estimated_tokens = self._estimate_tokens_for_columns(columns, table_context)
        
        # Use chunking if we exceed safe limit (reduced threshold)
        if estimated_tokens > max_tokens_per_chunk or len(columns) > 10:
            logger.info(f"Processing {len(columns)} columns (~{estimated_tokens} tokens) using Map-Reduce chunking")
            
            # MAP PHASE: Create intelligent chunks and process each
            chunks = self._create_token_aware_chunks(columns, max_tokens_per_chunk)
            
            all_descriptions = {}
            total_chunks = len(chunks)
            
            for idx, chunk in enumerate(chunks, 1):
                chunk_tokens = self._estimate_tokens_for_columns(chunk, table_context)
                logger.info(f"  MAP: Processing chunk {idx}/{total_chunks} ({len(chunk)} columns, ~{chunk_tokens} tokens)")
                
                batch_desc = self._analyze_columns_batch_internal(table_name, chunk, table_context)
                all_descriptions.update(batch_desc)
            
            # REDUCE PHASE: Merge results (already merged via update, but log completion)
            logger.info(f"  REDUCE: Merged {len(all_descriptions)} column descriptions")
            
            return all_descriptions
        else:
            # Small enough to process in one go
            return self._analyze_columns_batch_internal(table_name, columns, table_context)
    
    def _estimate_tokens_for_columns(
        self,
        columns: List[Dict[str, Any]],
        table_context: Optional[str] = None
    ) -> int:
        """
        Estimate token count for a column batch request (CONSERVATIVE estimate)
        
        More conservative estimation to avoid MAX_TOKENS:
        - Prompt template: ~300 tokens (increased buffer)
        - Table context: length / 3 chars per token (conservative)
        - Per column input: ~40 tokens (increased from 30)
        - Per column output: ~150 tokens (increased from 100)
        """
        base_tokens = 300  # Prompt template (increased buffer)
        
        if table_context:
            base_tokens += len(table_context) // 3  # More conservative (was 4)
        
        # Estimate tokens per column (input + expected output) - MORE CONSERVATIVE
        column_tokens = 0
        for col in columns:
            # Input: column name + type + structure
            input_tokens = len(col['name']) // 3 + len(col['data_type']) // 3 + 30
            # Expected output: description + metadata (be more generous)
            output_tokens = 150
            column_tokens += input_tokens + output_tokens
        
        total = base_tokens + column_tokens
        return total
    def _create_token_aware_chunks(
        self,
        columns: List[Dict[str, Any]],
        max_tokens: int = 2000
    ) -> List[List[Dict[str, Any]]]:
        """
        Create chunks based on estimated token count while preserving semantic boundaries
        CONSERVATIVE chunking to avoid MAX_TOKENS errors
        """
        # Categorize columns into semantic groups
        column_groups = self._group_columns_semantically(columns)
        
        chunks = []
        current_chunk = []
        current_tokens = 300  # Base prompt tokens (conservative)
        
        def finalize_chunk():
            nonlocal current_chunk, current_tokens
            if current_chunk:
                chunks.append(current_chunk)
                logger.debug(f"  Created chunk: {len(current_chunk)} columns (~{current_tokens} tokens)")
                current_chunk = []
                current_tokens = 300  # Reset to base
        
        # Process groups in priority order
        for group_name, group_cols in column_groups:
            # MORE CONSERVATIVE token estimate for this group
            group_tokens = sum(
                len(col['name']) // 3 + len(col['data_type']) // 3 + 180  # Increased per-column estimate
                for col in group_cols
            )
            
            # If adding this group exceeds limit, finalize current chunk
            # Use 80% of max_tokens as safety margin
            safe_limit = int(max_tokens * 0.8)
            
            if current_chunk and current_tokens + group_tokens > safe_limit:
                finalize_chunk()
            
            # If group itself is too large, split it into smaller pieces
            if group_tokens > safe_limit:
                # Split large group, max 5 columns per chunk to be very conservative
                for col in group_cols:
                    col_tokens = len(col['name']) // 3 + len(col['data_type']) // 3 + 180
                    
                    if current_tokens + col_tokens > safe_limit or len(current_chunk) >= 5:
                        finalize_chunk()
                    
                    current_chunk.append(col)
                    current_tokens += col_tokens
            else:
                # Add entire group to current chunk
                current_chunk.extend(group_cols)
                current_tokens += group_tokens
        
        # Finalize remaining chunk
        finalize_chunk()
        
        logger.debug(f"Created {len(chunks)} token-aware chunks from {len(columns)} columns (conservative mode)")
        return chunks
    
    def _group_columns_semantically(
        self,
        columns: List[Dict[str, Any]]
    ) -> List[Tuple[str, List[Dict[str, Any]]]]:
        """
        Group columns by semantic meaning (similar to preserving class/function boundaries)
        
        Returns list of (group_name, columns) tuples in priority order
        """
        # Categorize columns
        id_columns = []
        timestamp_columns = []
        grouped_by_prefix = {}  # prefix -> [columns]
        ungrouped_columns = []
        
        for col in columns:
            col_name = col['name'].lower()
            col_type = col['data_type'].lower()
            
            # Priority 1: ID/key columns (provide context for other columns)
            if any(pattern in col_name for pattern in ['_id', '_key', '_fk', 'id_', '^id$']):
                id_columns.append(col)
            
            # Priority 2: Timestamp columns (often grouped together in schemas)
            elif any(t in col_type for t in ['timestamp', 'date', 'time']) or \
                 any(pattern in col_name for pattern in ['_at', '_date', '_time', 'created', 'updated', 'modified', 'deleted']):
                timestamp_columns.append(col)
            
            # Priority 3: Columns with common prefixes (semantically related)
            else:
                parts = col_name.split('_')
                if len(parts) > 1:
                    prefix = parts[0]
                    if prefix not in grouped_by_prefix:
                        grouped_by_prefix[prefix] = []
                    grouped_by_prefix[prefix].append(col)
                else:
                    ungrouped_columns.append(col)
        
        # Build ordered list of groups
        groups = []
        
        if id_columns:
            groups.append(('id_keys', id_columns))
        
        # Add prefix groups sorted alphabetically for consistency
        for prefix in sorted(grouped_by_prefix.keys()):
            groups.append((f'prefix_{prefix}', grouped_by_prefix[prefix]))
        
        if timestamp_columns:
            groups.append(('timestamps', timestamp_columns))
        
        if ungrouped_columns:
            groups.append(('ungrouped', ungrouped_columns))
        
        return groups
    
    def _analyze_columns_batch_internal(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_context: Optional[str] = None
    ) -> Dict[str, SemanticDescription]:
        """Internal method to analyze a single batch of columns"""
        prompt = self._build_columns_batch_prompt(table_name, columns, table_context)
        
        try:
            response = self.ai_provider.generate_completion(
                prompt,
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            descriptions = self._parse_columns_batch_response(response, columns)
            logger.info(f"Generated descriptions for {len(descriptions)} columns in {table_name}")
            return descriptions
            
        except Exception as e:
            logger.error(f"Error analyzing columns for {table_name}: {e}")
            # Return basic descriptions on error
            return {
                col['name']: SemanticDescription(
                    description=f"Column: {col['name']} ({col['data_type']})",
                    business_context="Analysis failed"
                )
                for col in columns
            }
    
    def _build_table_prompt(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_comment: Optional[str] = None
    ) -> str:
        """Build prompt for table analysis"""
        columns_text = "\n".join([
            f"  - {col['name']} ({col['data_type']})"
            for col in columns
        ])
        
        prompt = f"""Analyze the following database table and provide a semantic description.

Table Name: {table_name}
{f'Existing Comment: {table_comment}' if table_comment else ''}

Columns:
{columns_text}

Please provide:
1. A clear, business-friendly description of what this table stores
2. The business context or domain this table belongs to
3. Suggested tags/categories (comma-separated)
4. Any data quality or usage notes

Format your response as JSON with this structure:
{{
  "description": "Clear description of the table",
  "business_context": "Business domain or context",
  "suggested_tags": ["tag1", "tag2", "tag3"],
  "data_quality_notes": "Any relevant notes"
}}"""
        
        return prompt
    
    def _build_column_prompt(
        self,
        table_name: str,
        column_name: str,
        data_type: str,
        column_comment: Optional[str],
        table_context: Optional[str]
    ) -> str:
        """Build prompt for single column analysis"""
        prompt = f"""Analyze the following database column and provide a semantic description.

Table: {table_name}
{f'Table Context: {table_context}' if table_context else ''}

Column Name: {column_name}
Data Type: {data_type}
{f'Existing Comment: {column_comment}' if column_comment else ''}

Please provide:
1. A clear description of what this column represents
2. Business context or usage
3. Suggested tags (comma-separated)
4. Any data quality notes (e.g., nullable, expected patterns)

Format your response as JSON with this structure:
{{
  "description": "Clear description",
  "business_context": "Context or usage",
  "suggested_tags": ["tag1", "tag2"],
  "data_quality_notes": "Any notes"
}}"""
        
        return prompt
    def _build_columns_batch_prompt(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        table_context: Optional[str]
    ) -> str:
        """Build prompt for batch column analysis - OPTIMIZED for token efficiency"""
        columns_text = "\n".join([
            f"  - {col['name']} ({col['data_type']})"
            for col in columns
        ])
        
        # Shortened prompt to reduce input tokens
        prompt = f"""Analyze database columns for table '{table_name}'. Provide concise, business-friendly descriptions.

Columns:
{columns_text}

For each column provide JSON with:
- description: Business-friendly description (NOT "Column: name (type)")
- business_context: Usage context
- suggested_tags: [array of tags]
- data_quality_notes: Constraints/patterns

Guidelines:
- entity_id, order_id etc → "Primary/unique identifier for..."
- status/state → Explain possible states
- timestamps → What event they track
- Be concise but informative

JSON format:
{{
  "column_name": {{
    "description": "...",
    "business_context": "...",
    "suggested_tags": [...],
    "data_quality_notes": "..."
  }}
}}
"""
        
        return prompt

    def _parse_table_response(self, response: str) -> SemanticDescription:
        """Parse AI response for table analysis"""
        try:
            # Extract JSON from response
            data = self._extract_json(response)
            
            return SemanticDescription(
                description=data.get('description', ''),
                business_context=data.get('business_context'),
                suggested_tags=data.get('suggested_tags', []),
                data_quality_notes=data.get('data_quality_notes')
            )
        except Exception as e:
            logger.warning(f"Error parsing table response: {e}")
            return SemanticDescription(description=response[:200])
    
    def _parse_column_response(self, response: str) -> SemanticDescription:
        """Parse AI response for column analysis"""
        try:
            data = self._extract_json(response)
            
            return SemanticDescription(
                description=data.get('description', ''),
                business_context=data.get('business_context'),
                suggested_tags=data.get('suggested_tags', []),
                data_quality_notes=data.get('data_quality_notes')
            )
        except Exception as e:
            logger.warning(f"Error parsing column response: {e}")
            return SemanticDescription(description=response[:200])
    
    def _parse_columns_batch_response(
        self,
        response: str,
        columns: List[Dict[str, Any]]
    ) -> Dict[str, SemanticDescription]:
        """Parse AI response for batch column analysis"""
        try:
            data = self._extract_json(response)
            
            descriptions = {}
            for col in columns:
                col_name = col['name']
                col_data = data.get(col_name, {})
                
                # Get description and ensure it's meaningful
                description = col_data.get('description', '')
                
                # If description is empty or generic, try to create a better one from context
                if not description or description.startswith('Column:'):
                    # Try to infer from column name and type
                    description = self._generate_fallback_description(col_name, col['data_type'])
                
                descriptions[col_name] = SemanticDescription(
                    description=description,
                    business_context=col_data.get('business_context'),
                    suggested_tags=col_data.get('suggested_tags', []),
                    data_quality_notes=col_data.get('data_quality_notes')
                )
            
            return descriptions
            
        except Exception as e:
            logger.warning(f"Error parsing batch response: {e}")
            # Return better fallback descriptions
            return {
                col['name']: SemanticDescription(
                    description=self._generate_fallback_description(col['name'], col['data_type']),
                    business_context="Auto-generated from column analysis"
                )
                for col in columns
            }
    
    def _generate_fallback_description(self, col_name: str, data_type: str) -> str:
        """Generate a better fallback description based on column name and type"""
        col_lower = col_name.lower()
        
        # ID patterns
        if col_lower.endswith('_id') or col_lower == 'id':
            base_name = col_lower.replace('_id', '').replace('id', '').strip('_')
            if base_name:
                return f"Unique identifier for {base_name.replace('_', ' ')}"
            else:
                return "Primary key identifier"
        
        # Foreign key patterns
        if col_lower.startswith('fk_') or col_lower.endswith('_fk'):
            ref_table = col_lower.replace('fk_', '').replace('_fk', '').strip('_')
            return f"Foreign key reference to {ref_table.replace('_', ' ')} table"
        
        # Timestamp patterns
        if any(pattern in col_lower for pattern in ['created_at', 'updated_at', 'deleted_at', 'modified_at']):
            action = col_lower.replace('_at', '').replace('_', ' ')
            return f"Timestamp when record was {action}"
        
        if 'timestamp' in data_type.lower() or 'datetime' in data_type.lower():
            return f"Timestamp field for {col_name.replace('_', ' ')}"
        
        # Status/state patterns
        if any(pattern in col_lower for pattern in ['status', 'state', 'flag', 'is_']):
            return f"Status indicator for {col_name.replace('_', ' ')}"
        
        # Name patterns
        if 'name' in col_lower:
            return f"Name field for {col_name.replace('_name', '').replace('name', '').replace('_', ' ').strip()}"
        
        # Amount/value patterns
        if any(pattern in col_lower for pattern in ['amount', 'price', 'cost', 'total', 'value']):
            return f"Monetary value representing {col_name.replace('_', ' ')}"
        
        # Count patterns
        if 'count' in col_lower or 'quantity' in col_lower:
            return f"Numeric count for {col_name.replace('_', ' ')}"
        
        # Default based on data type
        if 'int' in data_type.lower():
            return f"Numeric field: {col_name.replace('_', ' ')}"
        elif 'varchar' in data_type.lower() or 'string' in data_type.lower():
            return f"Text field: {col_name.replace('_', ' ')}"
        elif 'bool' in data_type.lower():
            return f"Boolean flag: {col_name.replace('_', ' ')}"
        
        # Generic fallback
        return f"Data field for {col_name.replace('_', ' ')} ({data_type})"
    
    def _extract_json(self, text: str) -> Dict[str, Any]:
        """Extract JSON from AI response"""
        # Try to find JSON in the response
        start = text.find('{')
        end = text.rfind('}') + 1
        
        if start >= 0 and end > start:
            json_str = text[start:end]
            return json.loads(json_str)
        
        # If no JSON found, try parsing the whole response
        return json.loads(text)


def create_ai_provider(provider_name: str, api_key: str, model: str) -> AIProvider:
    """
    Factory function to create AI provider
    
    Args:
        provider_name: 'anthropic', 'openai', or 'gemini'
        api_key: API key for the provider
        model: Model name to use
    
    Returns:
        AIProvider instance
    """
    if provider_name.lower() == 'anthropic':
        return AnthropicProvider(api_key, model)
    elif provider_name.lower() == 'openai':
        return OpenAIProvider(api_key, model)
    elif provider_name.lower() == 'gemini':
        return GeminiProvider(api_key, model)
    else:
        raise ValueError(f"Unknown AI provider: {provider_name}. Supported: anthropic, openai, gemini")