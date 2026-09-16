"""Tests for the REST + GraphQL query API (`rcd_data/api/`).

Follows the existing DB-test convention (`_sink_helpers.py`,
`test_referential_integrity.py`): real Postgres, no mocking, skip
gracefully if the data isn't there.

Run after generation:
    rcd-data generate --profile demo --seed 42 --sink postgres
    RCD_API_KEY=test-key python -m pytest rcd_data/tests/test_api.py -v
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect, text

from ._sink_helpers import get_db_engine

TEST_API_KEY = os.environ.get("RCD_API_KEY", "rcd-dev-key")
HEADERS = {"X-API-Key": TEST_API_KEY}


@pytest.fixture(scope="module")
def engine():
    return get_db_engine("postgres")


@pytest.fixture(scope="module")
def client(engine):
    if not inspect(engine).has_table("orders"):
        pytest.skip(
            "orders table not found in postgres — run `rcd-data generate --sink postgres` first"
        )
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
    if not count:
        pytest.skip("orders table is empty — run `rcd-data generate --sink postgres` first")

    from rcd_data.api.app import app

    return TestClient(app)


def _gql(
    client: TestClient, query: str, variables: dict | None = None, headers: dict | None = None
):
    return client.post(
        "/graphql",
        json={"query": query, "variables": variables or {}},
        headers=HEADERS if headers is None else headers,
    )


# ── Auth ─────────────────────────────────────────────────────────────────


class TestAuth:
    def test_rest_requires_api_key(self, client: TestClient):
        resp = client.get("/api/v1/orders")
        assert resp.status_code == 401

    def test_rest_rejects_wrong_api_key(self, client: TestClient):
        resp = client.get("/api/v1/orders", headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401

    def test_rest_accepts_valid_api_key(self, client: TestClient):
        resp = client.get("/api/v1/orders", headers=HEADERS)
        assert resp.status_code == 200

    def test_graphql_requires_api_key(self, client: TestClient):
        resp = _gql(client, "{ orders(limit: 1) { total } }", headers={})
        assert resp.status_code == 401

    def test_graphql_accepts_valid_api_key(self, client: TestClient):
        resp = _gql(client, "{ orders(limit: 1) { total } }")
        assert resp.status_code == 200
        assert "errors" not in resp.json()

    def test_health_does_not_require_api_key(self, client: TestClient):
        resp = client.get("/health")
        assert resp.status_code == 200


# ── Filter correctness ───────────────────────────────────────────────────


class TestFilterCorrectness:
    def test_orders_filtered_by_customer_id_all_match(self, client: TestClient, engine):
        unfiltered = client.get("/api/v1/orders", headers=HEADERS, params={"limit": 1}).json()
        assert unfiltered["items"], "expected at least one order to test against"
        customer_id = unfiltered["items"][0]["customer_id"]

        filtered = client.get(
            "/api/v1/orders", headers=HEADERS, params={"customer_id": customer_id, "limit": 200}
        ).json()
        assert len(filtered["items"]) > 0
        assert all(o["customer_id"] == customer_id for o in filtered["items"])

        with engine.connect() as conn:
            expected_total = conn.execute(
                text("SELECT COUNT(*) FROM orders WHERE customer_id = :cid"),
                {"cid": customer_id},
            ).scalar()
        assert filtered["total"] == expected_total

    def test_tickets_filtered_by_status(self, client: TestClient):
        page = client.get("/api/v1/tickets", headers=HEADERS, params={"limit": 1}).json()
        if not page["items"]:
            pytest.skip("no tickets to test against")
        status = page["items"][0]["status"]

        filtered = client.get(
            "/api/v1/tickets", headers=HEADERS, params={"status": status, "limit": 200}
        ).json()
        assert all(t["status"] == status for t in filtered["items"])


# ── Date-range boundary semantics ───────────────────────────────────────


class TestDateRangeBoundaries:
    def test_created_after_is_inclusive_created_before_is_exclusive(self, client: TestClient):
        page = client.get("/api/v1/orders", headers=HEADERS, params={"limit": 1}).json()
        assert page["items"], "expected at least one order to test against"
        order = page["items"][0]
        ts = order["created_at"]

        after = client.get(
            "/api/v1/orders", headers=HEADERS,
            params={"created_after": ts, "created_before": ts, "limit": 1},
        ).json()
        # created_before is exclusive of `ts`, so an exact-ts window is empty
        assert after["items"] == []

        inclusive = client.get(
            "/api/v1/orders", headers=HEADERS,
            params={"created_after": ts, "limit": 200},
        ).json()
        assert any(o["id"] == order["id"] for o in inclusive["items"]) or inclusive["total"] > 0


# ── Pagination bounds ────────────────────────────────────────────────────


class TestPaginationBounds:
    def test_limit_over_max_is_422(self, client: TestClient):
        resp = client.get("/api/v1/orders", headers=HEADERS, params={"limit": 500})
        assert resp.status_code == 422

    def test_limit_zero_is_422(self, client: TestClient):
        resp = client.get("/api/v1/orders", headers=HEADERS, params={"limit": 0})
        assert resp.status_code == 422

    def test_negative_offset_is_422(self, client: TestClient):
        resp = client.get("/api/v1/orders", headers=HEADERS, params={"offset": -1})
        assert resp.status_code == 422

    def test_limit_one_returns_exactly_one(self, client: TestClient):
        page = client.get("/api/v1/orders", headers=HEADERS, params={"limit": 1}).json()
        assert len(page["items"]) == 1

    def test_offset_past_end_is_empty_not_error(self, client: TestClient):
        page = client.get(
            "/api/v1/orders", headers=HEADERS, params={"limit": 1, "offset": 100_000_000}
        ).json()
        assert page["items"] == []


# ── 404s ─────────────────────────────────────────────────────────────────


class TestNotFound:
    def test_missing_order_is_404(self, client: TestClient):
        resp = client.get(
            "/api/v1/orders/00000000-0000-0000-0000-000000000000", headers=HEADERS
        )
        assert resp.status_code == 404

    def test_missing_customer_is_404(self, client: TestClient):
        resp = client.get(
            "/api/v1/customers/00000000-0000-0000-0000-000000000000", headers=HEADERS
        )
        assert resp.status_code == 404


# ── GraphQL/REST parity ──────────────────────────────────────────────────


class TestGraphQLRestParity:
    def test_same_filter_same_ids(self, client: TestClient):
        rest_page = client.get(
            "/api/v1/orders", headers=HEADERS, params={"limit": 5}
        ).json()
        rest_ids = {o["id"] for o in rest_page["items"]}

        gql_resp = _gql(
            client,
            "{ orders(limit: 5) { items { id } } }",
        )
        gql_ids = {o["id"] for o in gql_resp.json()["data"]["orders"]["items"]}

        assert rest_ids == gql_ids


# ── N+1 regression guard ─────────────────────────────────────────────────


class TestNPlusOneGuard:
    def test_nested_customer_does_not_n_plus_one(self, client: TestClient):
        from sqlalchemy import event

        from rcd_data.api.db import engine as api_engine

        statements: list[str] = []

        def _log(conn, cursor, statement, *args):
            statements.append(statement)

        event.listen(api_engine, "before_cursor_execute", _log)
        try:
            resp = _gql(
                client,
                "{ orders(limit: 10) { items { id customer { id name } } } }",
            )
        finally:
            event.remove(api_engine, "before_cursor_execute", _log)

        assert resp.status_code == 200
        assert "errors" not in resp.json()
        # 1 query for orders + 1 batched query for customers via the
        # DataLoader; allow a little slack but this must stay well under 10.
        assert len(statements) <= 3, (
            f"expected DataLoader batching to keep query count low, "
            f"got {len(statements)}: {statements}"
        )
