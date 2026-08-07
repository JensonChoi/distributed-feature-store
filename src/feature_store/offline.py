from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from feature_store.config import Settings, get_settings


class OfflineStore:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()

    def view_uri(self, view_ref: str) -> str:
        safe = view_ref.replace("@", "/")
        return f"s3://{self.settings.offline_bucket}/views/{safe}"

    def quarantine_uri(self, view_ref: str, job_id: str) -> str:
        safe = view_ref.replace("@", "/")
        return f"s3://{self.settings.offline_bucket}/quarantine/{safe}/{job_id}"

    def load(self, uri: str, *, version: int | None = None) -> pa.Table:
        table = DeltaTable(uri, version=version, storage_options=self._options(uri))
        return table.to_pyarrow_table()

    def load_range(
        self, uri: str, timestamp_field: str, start: datetime | None, end: datetime
    ) -> pa.Table:
        filters: list[tuple[str, str, Any]] = [(timestamp_field, "<", end)]
        if start is not None:
            filters.insert(0, (timestamp_field, ">=", start))
        table = DeltaTable(uri, storage_options=self._options(uri))
        return table.to_pyarrow_table(filters=filters)

    def append(self, uri: str, table: pa.Table, partition_by: str | None = None) -> None:
        kwargs: dict[str, Any] = {
            "mode": "append" if self.exists(uri) else "error",
            "storage_options": self._options(uri),
        }
        if partition_by and not self.exists(uri):
            kwargs["partition_by"] = [partition_by]
        write_deltalake(uri, table, **kwargs)

    def overwrite_partition(self, uri: str, table: pa.Table, predicate: str) -> None:
        write_deltalake(
            uri,
            table,
            mode="overwrite",
            predicate=predicate,
            partition_by=["event_date"],
            storage_options=self._options(uri),
        )

    def exists(self, uri: str) -> bool:
        try:
            DeltaTable(uri, storage_options=self._options(uri), without_files=True)
            return True
        except TableNotFoundError:
            return False

    def delete(self, uri: str) -> bool:
        """Best-effort cleanup for application-owned staging paths."""
        try:
            if uri.startswith("s3://"):
                endpoint = urlparse(self.settings.minio_endpoint)
                filesystem = pafs.S3FileSystem(
                    access_key=self.settings.minio_access_key,
                    secret_key=self.settings.minio_secret_key,
                    endpoint_override=endpoint.netloc,
                    scheme=endpoint.scheme,
                    region="us-east-1",
                )
                filesystem.delete_dir(uri.removeprefix("s3://"))
            else:
                pafs.LocalFileSystem().delete_dir(normalize_uri(uri))
            return True
        except Exception:
            return False

    def _options(self, uri: str) -> dict[str, str] | None:
        return self.settings.storage_options if uri.startswith("s3://") else None


def normalize_uri(uri: str) -> str:
    if uri.startswith("file://"):
        return str(Path(uri.removeprefix("file://")).resolve())
    return uri
