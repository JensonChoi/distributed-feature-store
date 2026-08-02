from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]


def test_prometheus_scrapes_every_process_and_has_example_alerts() -> None:
    prometheus = yaml.safe_load(
        (ROOT / "monitoring/prometheus/prometheus.yml").read_text()
    )
    targets = {
        target
        for scrape in prometheus["scrape_configs"]
        for config in scrape["static_configs"]
        for target in config["targets"]
    }
    assert targets == {"api:8000", "worker:9101", "stream-consumer:9102"}

    alerts = yaml.safe_load((ROOT / "monitoring/prometheus/alerts.yml").read_text())
    names = {rule["alert"] for group in alerts["groups"] for rule in group["rules"]}
    assert names == {
        "FeatureStoreJobsStuck",
        "FeatureStoreTargetUnavailable",
        "FeatureViewMaterializationStale",
        "OnlineMissingRateHigh",
        "StreamDeadLettersDetected",
        "StreamStagingBacklogOld",
    }


def test_grafana_provisioning_and_dashboard_cover_operator_signals() -> None:
    datasource = yaml.safe_load(
        (ROOT / "monitoring/grafana/provisioning/datasources/prometheus.yml").read_text()
    )
    assert datasource["datasources"][0]["url"] == "http://prometheus:9090"
    dashboard = json.loads(
        (ROOT / "monitoring/grafana/dashboards/feature-store-operations.json").read_text()
    )
    expressions = " ".join(
        target["expr"] for panel in dashboard["panels"] for target in panel["targets"]
    )
    for signal in (
        "online_request",
        "historical_query",
        "served_value_age",
        "materialization_source",
        "job_queue",
        "stream_event_ingestion_lag",
        "stream_dead_letters",
        "stream_ledger",
    ):
        assert signal in expressions


def test_compose_exposes_monitoring_and_process_metrics_ports() -> None:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    services = compose["services"]
    assert services["worker"]["ports"] == ["9101:9101"]
    assert services["stream-consumer"]["ports"] == ["9102:9102"]
    assert services["prometheus"]["ports"] == ["9090:9090"]
    assert services["grafana"]["ports"] == ["3000:3000"]
