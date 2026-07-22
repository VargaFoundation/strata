"""Tests for the Ecphoria Python client — no network, via an in-memory httpx MockTransport."""

from __future__ import annotations

import asyncio
import json

import httpx

from ecphoria.client import EcphoriaClient


def make_client(handler) -> EcphoriaClient:
    """A EcphoriaClient whose transport is an in-memory mock (no sockets)."""
    client = EcphoriaClient("http://test")
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


def test_events_escapes_source_and_validates_order_limit():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["sql"] = json.loads(req.content)["sql"]
        return httpx.Response(200, json={"rows": []})

    client = make_client(handler)
    try:
        run(
            client.events(
                source="x'; DROP TABLE episodic; --",
                order="DESC; DROP TABLE episodic",
                limit=5,
            )
        )
    finally:
        run(client.close())

    sql = seen["sql"]
    # The malicious quote is escaped (doubled), not breaking out of the string literal.
    assert "source = 'x''; DROP TABLE episodic; --'" in sql
    # order is whitelisted to ASC/DESC (the injected tail is dropped); limit is an int.
    assert "ORDER BY ts DESC LIMIT 5" in sql
    assert sql.count("DROP TABLE episodic") == 1  # only inside the escaped literal, not executable


def test_graph_and_cognition_methods_hit_right_endpoints():
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append((req.method, req.url.path))
        p = req.url.path
        if p == "/api/v1/memories/graph/centrality":
            return httpx.Response(200, json={"nodes": [{"node": "a", "pagerank": 0.5}], "count": 1})
        if p == "/api/v1/memories/graph/path":
            return httpx.Response(200, json={"path": ["a", "b"], "reachable": True})
        if p == "/api/v1/memories/graph/communities":
            return httpx.Response(200, json={"communities": [["a", "b"]], "count": 1})
        if p == "/api/v1/memories/m1/provenance":
            return httpx.Response(200, json={"memory": {"id": "m1"}, "source_events": []})
        if p == "/api/v1/memories/m1/feedback":
            return httpx.Response(200, json={"id": "m1", "applied": True})
        if p == "/api/v1/memory-templates":
            return httpx.Response(200, json={"templates": [{"name": "preference"}]})
        return httpx.Response(200, json={})

    client = make_client(handler)
    try:
        nodes = run(client.graph_centrality(limit=5))
        assert nodes[0]["node"] == "a"
        assert run(client.graph_path("a", "b")) == ["a", "b"]
        assert run(client.graph_communities())[0] == ["a", "b"]
        assert run(client.memory_provenance("m1"))["memory"]["id"] == "m1"
        assert run(client.memory_feedback("m1", "helpful"))["applied"] is True
        assert run(client.memory_templates())[0]["name"] == "preference"
    finally:
        run(client.close())

    paths = [p for _, p in calls]
    assert "/api/v1/memories/graph/centrality" in paths
    assert ("POST", "/api/v1/memories/m1/feedback") in calls


def test_attachment_upload_sends_raw_bytes_with_content_type():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["ct"] = req.headers.get("content-type")
        seen["body"] = req.content
        return httpx.Response(201, json={"id": "att1", "content_type": "image/png"})

    client = make_client(handler)
    try:
        meta = run(client.attachment_upload(b"\x89PNG", content_type="image/png", caption="shot"))
    finally:
        run(client.close())
    assert meta["id"] == "att1"
    assert seen["ct"] == "image/png"
    assert seen["body"] == b"\x89PNG"


def test_memory_update_patches_via_patch_verb():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["method"] = req.method
        seen["path"] = req.url.path
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"id": "m1", "content": "corrected", "importance": 0.9})

    client = make_client(handler)
    try:
        out = run(client.memory_update("m1", content="corrected", importance=0.9))
    finally:
        run(client.close())

    assert seen["method"] == "PATCH"
    assert seen["path"] == "/api/v1/memories/m1"
    assert seen["body"] == {"content": "corrected", "importance": 0.9}
    assert out["content"] == "corrected"


def test_memory_update_returns_none_on_404():
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "not found"})

    client = make_client(handler)
    try:
        out = run(client.memory_update("missing", content="x"))
    finally:
        run(client.close())
    assert out is None


def test_memory_list_sends_filters_and_offset():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["params"] = dict(req.url.params)
        return httpx.Response(200, json={"memories": [], "count": 0})

    client = make_client(handler)
    try:
        run(
            client.memory_list(
                user_id="alice",
                limit=10,
                offset=5,
                mem_type="semantic",
                min_importance=0.5,
                metadata_key="tag",
                metadata_value="vip",
            )
        )
    finally:
        run(client.close())

    p = seen["params"]
    assert p["limit"] == "10"
    assert p["offset"] == "5"
    assert p["user_id"] == "alice"
    assert p["mem_type"] == "semantic"
    assert p["min_importance"] == "0.5"
    assert p["metadata_key"] == "tag"
    assert p["metadata_value"] == "vip"
