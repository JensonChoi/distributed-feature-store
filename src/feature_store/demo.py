from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml
from confluent_kafka import Producer
from deltalake import write_deltalake

from feature_store.config import get_settings
from feature_store.models import RegistryManifest
from feature_store.sdk import FeatureStoreClient

SOURCE_URI = "s3://feature-store/sources/transactions"
VIEW_REF = "account_transaction_features@1.0.0"


def generate_transactions() -> list[dict[str, Any]]:
    randomizer = random.Random(42)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    accounts = {
        "acct_001": ("US", start - timedelta(days=600)),
        "acct_002": ("GB", start - timedelta(days=120)),
        "acct_003": ("DE", start - timedelta(days=30)),
        "acct_004": ("US", start - timedelta(days=10)),
    }
    rows: list[dict[str, Any]] = []
    for hour in range(72):
        for account_id, (home_country, created_at) in accounts.items():
            count = 1 + (hour + len(account_id)) % 3
            for item in range(count):
                timestamp = start + timedelta(hours=hour, minutes=item * 11)
                merchant_country = (
                    "CA" if account_id == "acct_004" and hour % 5 == 0 else home_country
                )
                rows.append(
                    {
                        "account_id": account_id,
                        "transaction_id": f"txn_{hour:03d}_{account_id}_{item}",
                        "event_timestamp": timestamp,
                        "amount": round(randomizer.uniform(5, 500), 2),
                        "home_country": home_country,
                        "merchant_country": merchant_country,
                        "account_created_at": created_at,
                    }
                )
    return rows


def seed_source() -> int:
    settings = get_settings()
    table = pa.Table.from_pylist(generate_transactions())
    write_deltalake(
        SOURCE_URI,
        table,
        mode="overwrite",
        partition_by=["account_id"],
        storage_options=settings.storage_options,
    )
    return int(table.num_rows)


def publish_example_event() -> str:
    settings = get_settings()
    event_id = f"live_{uuid.uuid4().hex[:12]}"
    payload = {
        "event_id": event_id,
        "feature_view": VIEW_REF,
        "entity_values": {"account_id": "acct_004"},
        "event_timestamp": datetime.now(UTC).isoformat(),
        "values": {
            "txn_count_1h": 8,
            "amount_sum_24h": 2134.5,
            "country_mismatch": True,
            "account_age_days": 82,
        },
    }
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    producer.produce("fraud.account-features.v1", key="acct_004", value=_json(payload))
    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"failed to deliver {remaining} demo event(s)")
    return event_id


def run_demo() -> dict[str, Any]:
    count = seed_source()
    packaged = files("feature_store").joinpath("examples/fraud_registry.yaml")
    if packaged.is_file():
        manifest = RegistryManifest.model_validate(yaml.safe_load(packaged.read_text()))
    else:
        manifest_path = Path(__file__).parents[2] / "examples" / "fraud" / "registry.yaml"
        manifest = RegistryManifest.model_validate(yaml.safe_load(manifest_path.read_text()))
    with FeatureStoreClient() as client:
        applied = client.apply(manifest)
        backfill = client.backfill(
            VIEW_REF,
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 4, tzinfo=UTC),
        )
    return {
        "seeded_transactions": count,
        "registry": applied.model_dump(mode="json"),
        "backfill_job": backfill.model_dump(mode="json"),
        "next": "Wait for the job to succeed, then run `feature-store demo-stream`.",
    }


def _json(value: dict[str, Any]) -> str:
    import json

    return json.dumps(value, separators=(",", ":"))
