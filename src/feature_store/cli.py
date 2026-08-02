from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from feature_store.sdk import FeatureStoreClient

app = typer.Typer(no_args_is_help=True, help="Operate the distributed feature store.")


def _print(value: object) -> None:
    typer.echo(json.dumps(value, indent=2, default=str))


@app.command("apply")
def apply(path: Annotated[Path, typer.Argument(exists=True, readable=True)]) -> None:
    with FeatureStoreClient() as client:
        _print(client.apply_file(path).model_dump(mode="json"))


@app.command("list")
def list_objects(kind: Annotated[str | None, typer.Option()] = None) -> None:
    with FeatureStoreClient() as client:
        _print(client.list_registry(kind))


@app.command("online-read")
def online_read(
    entity: Annotated[str, typer.Option(help="Entity JSON object")],
    feature: Annotated[list[str], typer.Option("--feature", "-f")],
) -> None:
    with FeatureStoreClient() as client:
        _print(
            client.get_online_features([json.loads(entity)], features=feature).model_dump(
                mode="json"
            )
        )


@app.command("historical-read")
def historical_read(
    observations: Annotated[Path, typer.Argument(exists=True, readable=True)],
    feature: Annotated[list[str], typer.Option("--feature", "-f")],
) -> None:
    payload = json.loads(observations.read_text())
    with FeatureStoreClient() as client:
        _print(client.get_historical_features(payload, features=feature).model_dump(mode="json"))


@app.command()
def backfill(feature_view: str, start: datetime, end: datetime) -> None:
    with FeatureStoreClient() as client:
        _print(client.backfill(feature_view, start, end))


@app.command()
def materialize(feature_view: str, start: datetime, end: datetime) -> None:
    with FeatureStoreClient() as client:
        _print(client.materialize(feature_view, start, end))


@app.command("materialize-incremental")
def materialize_incremental(
    feature_view: str,
    end: Annotated[str | None, typer.Option(help="UTC ISO-8601 cutoff")] = None,
    lookback_seconds: Annotated[int | None, typer.Option(min=0)] = None,
) -> None:
    with FeatureStoreClient() as client:
        _print(
            client.materialize_incremental(
                feature_view,
                end=datetime.fromisoformat(end) if end else None,
                lookback_seconds=lookback_seconds,
            )
        )


@app.command()
def jobs() -> None:
    with FeatureStoreClient() as client:
        _print(client.jobs())


@app.command()
def demo() -> None:
    """Seed fraud data, apply its registry, and start the historical backfill."""
    from feature_store.demo import run_demo

    _print(run_demo())


@app.command("demo-stream")
def demo_stream() -> None:
    """Publish one computed fraud feature row to the streaming path."""
    from feature_store.demo import publish_example_event

    _print({"event_id": publish_example_event()})


@app.command("job")
def job_status(job_id: str) -> None:
    with FeatureStoreClient() as client:
        _print(client.job(job_id))


@app.command("job-retry")
def job_retry(job_id: str) -> None:
    with FeatureStoreClient() as client:
        _print(client.retry_job(job_id))


@app.command("job-result")
def job_result(job_id: str, output: Path) -> None:
    with FeatureStoreClient() as client:
        path = client.download_job_result(job_id, output)
    _print({"job_id": job_id, "output": str(path)})


if __name__ == "__main__":
    app()
