from __future__ import annotations

import json

import httpx

from feature_store.sdk import FeatureStoreClient


def test_sdk_uses_versioned_online_contract() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "resolved_features": ["account_stats@1.0.0:amount"],
                "rows": [
                    {
                        "account_id": "a",
                        "account_stats__status": "present",
                        "account_stats__amount": 10.0,
                    }
                ],
            },
        )

    with FeatureStoreClient(
        "http://feature-store", transport=httpx.MockTransport(handler)
    ) as client:
        result = client.get_online_features(
            [{"account_id": "a"}], features=["account_stats@1.0.0:amount"]
        )
    assert captured["path"] == "/v1/online-features:read"
    assert captured["body"] == {
        "entities": [{"account_id": "a"}],
        "features": ["account_stats@1.0.0:amount"],
        "feature_service": None,
    }
    assert result.rows[0]["account_stats__amount"] == 10.0
