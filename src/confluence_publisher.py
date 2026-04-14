"""Confluence publisher for table-level catalog pages."""

import html
import logging
from datetime import datetime
from typing import Dict, Optional, Any

import requests
from requests import HTTPError

logger = logging.getLogger(__name__)


class ConfluencePublisher:
	"""Publishes one Confluence page per table."""

	def __init__(
		self,
		base_url: str,
		space_key: str,
		username: str,
		api_token: str,
		folder_name: str,
		parent_page_id: Optional[str] = None,
		page_title_prefix: str = "Catalog"
	):
		self.base_url = base_url.rstrip('/')
		self.space_key = space_key
		self.folder_name = folder_name
		self.parent_page_id = parent_page_id
		self.page_title_prefix = page_title_prefix
		self.auth = (username, api_token)

		logger.info(
			"Initialized ConfluencePublisher | base_url=%s | space=%s | folder=%s | parent_id=%s",
			self.base_url,
			self.space_key,
			self.folder_name,
			self.parent_page_id or "none",
		)

		# Validate connectivity and auth early
		self._validate_confluence_access()

	def _validate_confluence_access(self) -> None:
		"""Validate that we can connect to Confluence and access the space."""
		try:
			logger.info("Validating Confluence connectivity...")
			payload = self._api_get(
				"/rest/api/space",
				params={"spaceKey": self.space_key, "limit": 1},
			)
			if payload.get("results"):
				space_name = payload["results"][0].get("name", self.space_key)
				logger.info("Successfully connected to Confluence space '%s'", space_name)
			else:
				logger.warning("Space '%s' query returned no results", self.space_key)
		except Exception as e:
			logger.error(
				"Failed to validate Confluence access. Check: "
				"1) base_url is correct (got: %s) "
				"2) space_key exists (got: %s) "
				"3) credentials are valid | Error: %s",
				self.base_url,
				self.space_key,
				e,
			)
			raise

	def publish_tables(
		self,
		database_name: str,
		tables_metadata: Dict[str, Any],
		table_descriptions: Dict[str, Any],
		column_descriptions: Dict[str, Dict[str, Any]]
	) -> Dict[str, Any]:
		"""Create or update one Confluence page per table under the configured folder page."""
		folder_page = self._ensure_folder_page()
		published_pages = []

		for table_name, metadata in sorted(tables_metadata.items()):
			table_desc = table_descriptions.get(table_name)
			col_descs = column_descriptions.get(table_name, {})

			title = f"{self.page_title_prefix}: {database_name}.{table_name}"
			body = self._build_table_page_body(
				database_name=database_name,
				table_name=table_name,
				metadata=metadata,
				table_desc=table_desc,
				col_descs=col_descs,
			)

			page = self._upsert_page(title=title, body=body, parent_id=folder_page["id"])
			published_pages.append({
				"table_name": table_name,
				"page_id": page.get("id"),
				"page_url": self._to_web_url(page),
				"page_title": title,
			})

		logger.info("Published %d table pages to Confluence", len(published_pages))
		return {
			"folder_page_id": folder_page["id"],
			"published_pages": published_pages,
		}

	def _to_web_url(self, page_payload: Dict[str, Any]) -> str:
		webui_path = page_payload.get("_links", {}).get("webui", "")
		if webui_path.startswith("http"):
			return webui_path
		if webui_path:
			return f"{self.base_url}{webui_path}"
		return ""

	def _api_get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
		response = requests.get(
			f"{self.base_url}{endpoint}",
			params=params,
			auth=self.auth,
			headers={"Accept": "application/json"},
			timeout=30,
		)
		try:
			response.raise_for_status()
		except HTTPError as exc:
			error_details = self._extract_error_details(response)
			logger.error(
				"Confluence GET %s failed: %s | response=%s",
				endpoint,
				exc,
				error_details,
			)
			raise
		return response.json()

	def _api_post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
		response = requests.post(
			f"{self.base_url}{endpoint}",
			json=payload,
			auth=self.auth,
			headers={
				"Accept": "application/json",
				"Content-Type": "application/json",
			},
			timeout=30,
		)
		try:
			response.raise_for_status()
		except HTTPError as exc:
			error_details = self._extract_error_details(response)
			logger.error("Confluence POST failed: %s | response=%s", exc, error_details)
			raise
		return response.json()

	def _api_put(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
		response = requests.put(
			f"{self.base_url}{endpoint}",
			json=payload,
			auth=self.auth,
			headers={
				"Accept": "application/json",
				"Content-Type": "application/json",
			},
			timeout=30,
		)
		try:
			response.raise_for_status()
		except HTTPError as exc:
			error_details = self._extract_error_details(response)
			logger.error("Confluence PUT failed: %s | response=%s", exc, error_details)
			raise
		return response.json()

	def _extract_error_details(self, response: requests.Response) -> str:
		"""Best-effort extraction of API error details for easier debugging."""
		try:
			payload = response.json()
			if isinstance(payload, dict):
				message = payload.get("message") or payload.get("errorMessage")
				error_msg = payload.get("error")
				errors = payload.get("errors")
				if message and errors:
					return f"{message} | errors={errors}"
				if message:
					return str(message)
				if error_msg:
					return str(error_msg)
				return str(payload)[:500]
		except Exception:
			pass
		return response.text[:1000]

	def _validate_parent_page(self, parent_page_id: str) -> bool:
		"""Check if the parent page exists."""
		try:
			response = requests.get(
				f"{self.base_url}/rest/api/content/{parent_page_id}",
				auth=self.auth,
				headers={"Accept": "application/json"},
				timeout=30,
			)
			if response.status_code == 200:
				page = response.json()
				logger.info(
					"Validated parent page [%s] exists: %s",
					parent_page_id,
					page.get("title", "unknown"),
				)
				return True
			else:
				logger.error(
					"Parent page [%s] does not exist. Status: %d. Response: %s",
					parent_page_id,
					response.status_code,
					response.text[:500],
			)
				return False
		except Exception as e:
			logger.error("Failed to validate parent page [%s]: %s", parent_page_id, e)
			return False

	def _find_page_by_title(self, title: str, parent_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
		# Use CQL search because it is more reliable for exact title matches and statuses.
		escaped_title = title.replace('"', '\\"')
		payload = self._api_get(
			"/rest/api/content/search",
			params={
				"cql": f'type=page and space="{self.space_key}" and title="{escaped_title}"',
				"expand": "version,ancestors,status",
				"limit": 50,
			},
		)

		for result in payload.get("results", []):
			if result.get("title") != title:
				continue
			if result.get("status") and result.get("status") != "current":
				continue
			if parent_id:
				ancestor_ids = {ancestor.get("id") for ancestor in result.get("ancestors", [])}
				if parent_id not in ancestor_ids:
					continue
			return result

		# Fallback: the content endpoint with title+spaceKey can find pages that
		# CQL misses due indexing lag or permission-filtered search behavior.
		for result in self._find_pages_by_title_via_content_api(title):
			if result.get("title") != title:
				continue
			if result.get("status") and result.get("status") != "current":
				continue
			if parent_id:
				ancestor_ids = {ancestor.get("id") for ancestor in result.get("ancestors", [])}
				if parent_id not in ancestor_ids:
					continue
			return result
		return None

	def _find_pages_by_title_via_content_api(self, title: str) -> list[Dict[str, Any]]:
		"""Find pages via /content endpoint using title+spaceKey params."""
		payload = self._api_get(
			"/rest/api/content",
			params={
				"title": title,
				"spaceKey": self.space_key,
				"type": "page",
				"expand": "version,ancestors,status",
				"limit": 50,
			},
		)
		results = payload.get("results")
		if isinstance(results, list):
			return [item for item in results if isinstance(item, dict)]
		return []

	def _find_any_page_by_title_any_status(self, title: str) -> Optional[Dict[str, Any]]:
		"""Find a page by title across current/archived/trashed statuses."""
		escaped_title = title.replace('"', '\\"')
		payload = self._api_get(
			"/rest/api/content/search",
			params={
				"cql": f'type=page and space="{self.space_key}" and title="{escaped_title}"',
				"expand": "version,ancestors,status",
				"limit": 50,
			},
		)

		results = payload.get("results", [])
		if not results:
			return None

		# Prefer current/draft pages because archived pages cannot be moved.
		for status in ("current", "draft", "historical", "archived", "trashed"):
			for result in results:
				if result.get("title") == title and result.get("status") == status:
					return result

		for result in results:
			if result.get("title") == title:
				return result

		return None

	def _is_duplicate_title_error(self, exc: HTTPError) -> bool:
		response = getattr(exc, "response", None)
		if not response:
			return False
		body = self._extract_error_details(response).lower()
		if response.status_code != 400:
			return False
		duplicate_indicators = (
			"same title",
			"already exists",
			"title already exists",
		)
		return any(indicator in body for indicator in duplicate_indicators)

	def _build_unique_title(self, title: str) -> str:
		timestamp_suffix = datetime.now().strftime("%Y%m%d-%H%M%S")
		return f"{title} [{timestamp_suffix}]"

	def _ensure_folder_page(self) -> Dict[str, Any]:
		existing = self._find_page_by_title(self.folder_name, self.parent_page_id)
		if existing:
			return existing

		# Confluence page titles are unique per space. If the page already exists
		# under a different parent, reuse it instead of failing on create.
		existing_any_parent = self._find_page_by_title(self.folder_name)
		if existing_any_parent:
			logger.warning(
				"Folder page '%s' already exists in space '%s' under a different parent. Reusing page ID %s.",
				self.folder_name,
				self.space_key,
				existing_any_parent.get("id"),
			)
			return existing_any_parent

		# If parent page is specified, validate it exists first
		if self.parent_page_id:
			logger.info("Validating parent page [%s] exists...", self.parent_page_id)
			if not self._validate_parent_page(self.parent_page_id):
				raise ValueError(
					f"Parent page ID {self.parent_page_id} does not exist or is inaccessible. "
					f"Check CONFLUENCE_PARENT_PAGE_ID in .env. Or remove it to create folder at space root."
				)

		body = "<p>Catalog table pages generated automatically by schema_generator.</p>"
		return self._create_page(title=self.folder_name, body=body, parent_id=self.parent_page_id)

	def _create_page(self, title: str, body: str, parent_id: Optional[str] = None) -> Dict[str, Any]:
		payload: Dict[str, Any] = {
			"type": "page",
			"title": title,
			"space": {"key": self.space_key},
			"body": {
				"storage": {
					"value": body,
					"representation": "storage",
				}
			},
		}
		if parent_id:
			payload["ancestors"] = [{"id": parent_id}]
		return self._api_post("/rest/api/content", payload)

	def _update_page(
		self,
		page_id: str,
		title: str,
		body: str,
		version_number: int,
		parent_id: Optional[str] = None,
	) -> Dict[str, Any]:
		payload: Dict[str, Any] = {
			"id": page_id,
			"type": "page",
			"title": title,
			"space": {"key": self.space_key},
			"version": {"number": version_number},
			"body": {
				"storage": {
					"value": body,
					"representation": "storage",
				}
			},
		}
		if parent_id:
			payload["ancestors"] = [{"id": parent_id}]
		return self._api_put(f"/rest/api/content/{page_id}", payload)

	def _upsert_page(self, title: str, body: str, parent_id: str) -> Dict[str, Any]:
		existing = self._find_page_by_title(title, parent_id)
		if not existing:
			existing_any_parent = self._find_page_by_title(title)
			if existing_any_parent:
				version_number = int(existing_any_parent.get("version", {}).get("number", 1)) + 1
				return self._update_page(
					page_id=existing_any_parent["id"],
					title=title,
					body=body,
					version_number=version_number,
					parent_id=parent_id,
				)
			try:
				return self._create_page(title=title, body=body, parent_id=parent_id)
			except HTTPError as exc:
				if not self._is_duplicate_title_error(exc):
					raise

				# Create failed due duplicate title. Re-query globally and update if possible.
				duplicate_page = self._find_any_page_by_title_any_status(title)
				if duplicate_page:
					status = duplicate_page.get("status", "unknown")
					if status in {"archived", "trashed"}:
						unique_title = self._build_unique_title(title)
						logger.warning(
							"Title '%s' is blocked by a %s page (id=%s). Creating with unique title '%s'.",
							title,
							status,
							duplicate_page.get("id"),
							unique_title,
						)
						return self._create_page(title=unique_title, body=body, parent_id=parent_id)

					version_number = int(duplicate_page.get("version", {}).get("number", 1)) + 1
					logger.warning(
						"Title '%s' already exists in space on page id=%s (status=%s). Updating and moving it under parent %s.",
						title,
						duplicate_page.get("id"),
						status,
						parent_id,
					)
					return self._update_page(
						page_id=duplicate_page["id"],
						title=title,
						body=body,
						version_number=version_number,
						parent_id=parent_id,
					)

				# Duplicate title reported, but search couldn't find the conflicting page.
				# This can happen for restricted pages. Fall back to a unique title.
				unique_title = self._build_unique_title(title)
				logger.warning(
					"Title '%s' appears to exist but is not queryable. Falling back to unique title '%s'.",
					title,
					unique_title,
				)
				return self._create_page(title=unique_title, body=body, parent_id=parent_id)

		version_number = int(existing.get("version", {}).get("number", 1)) + 1
		return self._update_page(
			page_id=existing["id"],
			title=title,
			body=body,
			version_number=version_number,
			parent_id=parent_id,
		)

	def _build_table_page_body(
		self,
		database_name: str,
		table_name: str,
		metadata: Any,
		table_desc: Optional[Any],
		col_descs: Dict[str, Any],
	) -> str:
		timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		table_description = html.escape(getattr(table_desc, "description", "") or "N/A")
		business_context = html.escape(getattr(table_desc, "business_context", "") or "N/A")
		data_quality = html.escape(getattr(table_desc, "data_quality_notes", "") or "N/A")

		tags = getattr(table_desc, "suggested_tags", []) or []
		tags_text = html.escape(", ".join(str(tag) for tag in tags)) if tags else "N/A"

		primary_keys = getattr(metadata, "primary_keys", []) or []
		primary_keys_text = html.escape(", ".join(primary_keys)) if primary_keys else "N/A"

		rows = []
		for column in metadata.columns:
			col_desc = col_descs.get(column.name)
			col_description = html.escape(getattr(col_desc, "description", "") or "N/A")
			col_context = html.escape(getattr(col_desc, "business_context", "") or "")
			if col_context and col_context != "N/A":
				col_description = f"{col_description}<br/><em>{col_context}</em>"

			rows.append(
				"<tr>"
				f"<td>{html.escape(column.name)}</td>"
				f"<td>{html.escape(column.data_type)}</td>"
				f"<td>{col_description}</td>"
				"</tr>"
			)

		columns_table = (
			"<table>"
			"<tbody>"
			"<tr><th>Column</th><th>Type</th><th>Description</th></tr>"
			f"{''.join(rows)}"
			"</tbody>"
			"</table>"
		)

		return (
			f"<p><strong>Database:</strong> {html.escape(database_name)}</p>"
			f"<p><strong>Table:</strong> {html.escape(table_name)}</p>"
			f"<p><strong>Generated at:</strong> {html.escape(timestamp)}</p>"
			f"<p><strong>Table Description:</strong> {table_description}</p>"
			f"<p><strong>Business Context:</strong> {business_context}</p>"
			f"<p><strong>Tags:</strong> {tags_text}</p>"
			f"<p><strong>Data Quality Notes:</strong> {data_quality}</p>"
			f"<p><strong>Primary Keys:</strong> {primary_keys_text}</p>"
			"<h2>Columns</h2>"
			f"{columns_table}"
		)
