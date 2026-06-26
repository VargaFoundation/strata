"""Tests for the Strata Python client — no network, via an in-memory httpx MockTransport."""

from __future__ import annotations

import asyncio
import json

import httpx

from strata.client import StrataClient


def make_client(handler) -> StrataClient:
    """A StrataClient whose transport is an in-memory mock (no sockets)."""
    client = StrataClient("http://test")
    client._client = httpx.AsyncClient(
        base_url="http://test", transport=httpx.MockTransport(handler)
    )
    return client


def run(coro):
    return asyncio.run(coro)


def test_memory_add_posts_scope_and_returns_outcome():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["method"] = req.method
        seen["path"] = req.url.path
        seen["body"] = json.loads(req.content)
        return httpx.Response(
            200, json={"memory": {"id": "m1", "content": "likes tea"}, "outcome": "Inserted"}
        )

    client = make_client(handler)
    try:
        out = run(client.memory_add("likes tea", user_id="alice", subject="drink"))
    finally:
        run(client.close())

    assert seen["method"] == "POST"
    assert seen["path"] == "/api/v1/memories"
    assert seen["body"]["content"] == "likes tea"
    assert seen["body"]["user_id"] == "alice"
    assert seen["body"]["subject"] == "drink"
    assert out["outcome"] == "Inserted"


def test_memory_search_returns_results_list():
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/api/v1/memories/search"
        return httpx.Response(
            200, json={"results": [{"memory": {"content": "x"}, "score": 0.9}], "count": 1}
        )

    client = make_client(handler)
    try:
        hits = run(client.memory_search("tea", k=3, user_id="alice"))
    finally:
        run(client.close())
    assert len(hits) == 1
    assert hits[0]["score"] == 0.9


def test_memory_list_passes_query_params():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["params"] = dict(req.url.params)
        return httpx.Response(200, json={"memories": [{"id": "m1"}], "count": 1})

    client = make_client(handler)
    try:
        mems = run(client.memory_list(limit=10, user_id="alice"))
    finally:
        run(client.close())
    assert seen["params"]["limit"] == "10"
    assert seen["params"]["user_id"] == "alice"
    assert len(mems) == 1


def test_memory_get_404_returns_none():
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "NOT_FOUND"}})

    client = make_client(handler)
    try:
        assert run(client.memory_get("missing")) is None
    finally:
        run(client.close())


def test_memory_delete_true_and_false():
    def found(req):
        return httpx.Response(200, json={"id": "m1", "deleted": True})

    def missing(req):
        return httpx.Response(404, json={})

    c1 = make_client(found)
    try:
        assert run(c1.memory_delete("m1")) is True
    finally:
        run(c1.close())

    c2 = make_client(missing)
    try:
        assert run(c2.memory_delete("gone")) is False
    finally:
        run(c2.close())


def test_memory_history_returns_list():
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/api/v1/memories/m1/history"
        return httpx.Response(200, json={"history": [{"content": "old"}, {"content": "new"}], "count": 2})

    client = make_client(handler)
    try:
        hist = run(client.memory_history("m1"))
    finally:
        run(client.close())
    assert [h["content"] for h in hist] == ["old", "new"]


def test_session_lifecycle():
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append((req.method, req.url.path))
        if req.url.path.endswith("/recall"):
            return httpx.Response(200, json={"events": [{"a": 1}], "count": 1})
        return httpx.Response(200, json={"status": "ok"})

    client = make_client(handler)
    try:
        run(client.session_start("s1", "bot"))
        run(client.session_end("s1", summary="done"))
        events = run(client.session_recall("s1"))
    finally:
        run(client.close())

    assert ("POST", "/api/v1/sessions") in calls
    assert ("POST", "/api/v1/sessions/s1/end") in calls
    assert events == [{"a": 1}]


def test_ingest_and_query_core_paths():
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/api/v1/ingest":
            return httpx.Response(200, json={"ingested": 2})
        if req.url.path == "/api/v1/query":
            return httpx.Response(200, json={"rows": [{"x": 1}], "count": 1})
        return httpx.Response(404, json={})

    client = make_client(handler)
    try:
        assert run(client.ingest("app", [{}, {}])) == 2
        assert run(client.query("SELECT 1")) == [{"x": 1}]
    finally:
        run(client.close())
