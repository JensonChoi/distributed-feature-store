from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
import yaml
from pydantic import ValidationError

from feature_store.models import RegistryMetadataPatch, RegistryObjectKind, RegistryTarget
from feature_store.sdk import FeatureStoreClient

app = typer.Typer(no_args_is_help=True, help="Operate the distributed feature store.")
registry_app = typer.Typer(no_args_is_help=True, help="Validate and manage registry objects.")
app.add_typer(registry_app, name="registry")


def _print(value: object) -> None:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    elif isinstance(value, list) and value and hasattr(value[0], "model_dump"):
        value = [item.model_dump(mode="json") for item in value]
    typer.echo(json.dumps(value, indent=2, default=str))


def _registry_file(operation: str, path: Path) -> None:
    try:
        with FeatureStoreClient() as client:
            result = getattr(client, f"{operation}_file")(path)
        _print(result.model_dump(mode="json"))
        if operation in {"validate", "plan"} and result.summary.rejected:
            raise typer.Exit(code=1)
    except (OSError, ValidationError, yaml.YAMLError) as exc:
        _print({"error": str(exc)})
        raise typer.Exit(code=2) from exc


@app.command("apply")
def apply(path: Annotated[Path, typer.Argument(exists=True, readable=True)]) -> None:
    _registry_file("apply", path)


@app.command("list")
def list_objects(kind: Annotated[str | None, typer.Option()] = None) -> None:
    with FeatureStoreClient() as client:
        _print(client.list_registry(kind))


@registry_app.command("validate")
def registry_validate(path: Annotated[Path, typer.Argument(exists=True, readable=True)]) -> None:
    _registry_file("validate", path)


@registry_app.command("plan")
def registry_plan(path: Annotated[Path, typer.Argument(exists=True, readable=True)]) -> None:
    _registry_file("plan", path)


@registry_app.command("apply")
def registry_apply(path: Annotated[Path, typer.Argument(exists=True, readable=True)]) -> None:
    _registry_file("apply", path)


@registry_app.command("list")
def registry_list(kind: Annotated[str | None, typer.Option()] = None) -> None:
    list_objects(kind)


def _target(
    kind: RegistryObjectKind,
    name: str,
    version: str | None,
    feature: str | None,
) -> RegistryTarget:
    try:
        return RegistryTarget(kind=kind, name=name, version=version, feature=feature)
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc


@registry_app.command("describe")
def registry_describe(
    kind: RegistryObjectKind,
    name: str,
    version: Annotated[str | None, typer.Option()] = None,
    feature: Annotated[str | None, typer.Option()] = None,
) -> None:
    target = _target(kind, name, version, feature)
    with FeatureStoreClient() as client:
        _print(client.describe_registry_object(target).model_dump(mode="json"))


@registry_app.command("metadata")
def registry_metadata(
    kind: RegistryObjectKind,
    name: str,
    version: Annotated[str | None, typer.Option()] = None,
    feature: Annotated[str | None, typer.Option()] = None,
    owner: Annotated[list[str] | None, typer.Option("--owner")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag", help="KEY=VALUE")] = None,
    documentation: Annotated[list[str] | None, typer.Option("--documentation", "--doc")] = None,
    clear_owners: Annotated[bool, typer.Option()] = False,
    clear_tags: Annotated[bool, typer.Option()] = False,
    clear_documentation: Annotated[bool, typer.Option()] = False,
) -> None:
    if owner and clear_owners:
        raise typer.BadParameter("--owner and --clear-owners cannot be combined")
    if tag and clear_tags:
        raise typer.BadParameter("--tag and --clear-tags cannot be combined")
    if documentation and clear_documentation:
        raise typer.BadParameter("--documentation and --clear-documentation cannot be combined")
    values: dict[str, object] = {}
    if owner is not None or clear_owners:
        values["owners"] = [] if clear_owners else owner
    if tag is not None or clear_tags:
        parsed_tags: dict[str, str] = {}
        for item in tag or []:
            if "=" not in item or not item.split("=", 1)[0]:
                raise typer.BadParameter("--tag values must use KEY=VALUE")
            key, value = item.split("=", 1)
            if key in parsed_tags:
                raise typer.BadParameter(f"duplicate tag key: {key}")
            parsed_tags[key] = value
        values["tags"] = {} if clear_tags else parsed_tags
    if documentation is not None or clear_documentation:
        values["documentation_links"] = [] if clear_documentation else documentation
    if not values:
        raise typer.BadParameter("provide metadata values or an explicit clear flag")
    target = _target(kind, name, version, feature)
    try:
        patch = RegistryMetadataPatch.model_validate(values)
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    with FeatureStoreClient() as client:
        _print(client.patch_registry_metadata(target, patch).model_dump(mode="json"))


@registry_app.command("deprecate")
def registry_deprecate(
    kind: RegistryObjectKind,
    name: str,
    version: Annotated[str | None, typer.Option()] = None,
    feature: Annotated[str | None, typer.Option()] = None,
    message: Annotated[str | None, typer.Option()] = None,
    replacement: Annotated[str | None, typer.Option(help="Replacement object name")] = None,
    replacement_version: Annotated[str | None, typer.Option()] = None,
    replacement_feature: Annotated[str | None, typer.Option()] = None,
) -> None:
    target = _target(kind, name, version, feature)
    if replacement is None and (replacement_version or replacement_feature):
        raise typer.BadParameter("replacement options require --replacement")
    replacement_target = (
        _target(
            kind,
            replacement,
            replacement_version,
            replacement_feature if replacement_feature is not None else feature,
        )
        if replacement
        else None
    )
    with FeatureStoreClient() as client:
        result = client.deprecate_registry_object(
            target, message=message, replacement=replacement_target
        )
        _print(result.model_dump(mode="json"))


@registry_app.command("activate")
def registry_activate(
    kind: RegistryObjectKind,
    name: str,
    version: Annotated[str | None, typer.Option()] = None,
    feature: Annotated[str | None, typer.Option()] = None,
) -> None:
    target = _target(kind, name, version, feature)
    with FeatureStoreClient() as client:
        _print(client.activate_registry_object(target).model_dump(mode="json"))


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
