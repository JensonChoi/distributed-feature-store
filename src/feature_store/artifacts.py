from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from feature_store.config import Settings, get_settings


class ArtifactStorage:
    """Storage for durable job inputs and immutable attempt results."""

    def __init__(self, settings: Settings | None = None, *, root_uri: str | None = None):
        self.settings = settings or get_settings()
        self.root_uri = (root_uri or f"s3://{self.settings.offline_bucket}/artifacts/jobs").rstrip("/")

    def job_prefix(self, job_id: str) -> str:
        return f"{self.root_uri}/{job_id}"

    def input_uri(self, job_id: str) -> str:
        return f"{self.job_prefix(job_id)}/input.json"

    def result_uri(self, job_id: str, attempt: int, lease_token: str) -> str:
        return f"{self.job_prefix(job_id)}/results/attempt-{attempt}-{lease_token}.parquet"

    def write_json(self, uri: str, value: object) -> None:
        filesystem, path = self._resolve(uri)
        self._ensure_parent(filesystem, path)
        with filesystem.open_output_stream(path) as stream:
            stream.write(json.dumps(value, separators=(",", ":")).encode())

    def read_json(self, uri: str) -> object:
        filesystem, path = self._resolve(uri)
        with filesystem.open_input_file(path) as stream:
            return json.loads(stream.read().decode())

    def write_parquet(self, uri: str, table: pa.Table) -> int:
        filesystem, path = self._resolve(uri)
        self._ensure_parent(filesystem, path)
        try:
            with filesystem.open_output_stream(path) as stream:
                pq.write_table(table, stream)
            return int(filesystem.get_file_info(path).size)
        except Exception:
            self.delete(uri)
            raise

    def iter_bytes(self, uri: str, *, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
        filesystem, path = self._resolve(uri)
        with filesystem.open_input_stream(path) as stream:
            while chunk := stream.read(chunk_size):
                yield chunk

    def exists(self, uri: str) -> bool:
        filesystem, path = self._resolve(uri)
        return bool(filesystem.get_file_info(path).type == pafs.FileType.File)

    def delete(self, uri: str) -> None:
        filesystem, path = self._resolve(uri)
        if filesystem.get_file_info(path).type != pafs.FileType.NotFound:
            filesystem.delete_file(path)

    def delete_job(self, job_id: str) -> None:
        filesystem, path = self._resolve(self.job_prefix(job_id))
        info = filesystem.get_file_info(path)
        if info.type != pafs.FileType.NotFound:
            filesystem.delete_dir(path)

    def _resolve(self, uri: str) -> tuple[pafs.FileSystem, str]:
        if uri.startswith("s3://"):
            endpoint = urlparse(self.settings.minio_endpoint)
            filesystem = pafs.S3FileSystem(
                access_key=self.settings.minio_access_key,
                secret_key=self.settings.minio_secret_key,
                endpoint_override=endpoint.netloc,
                scheme=endpoint.scheme,
                region="us-east-1",
            )
            return filesystem, uri.removeprefix("s3://")
        path = uri.removeprefix("file://")
        return pafs.LocalFileSystem(), str(Path(path).resolve())

    @staticmethod
    def _ensure_parent(filesystem: pafs.FileSystem, path: str) -> None:
        parent = str(Path(path).parent)
        filesystem.create_dir(parent, recursive=True)
