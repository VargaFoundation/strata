"""Ecphoria Python SDK — client for the Ecphoria context lake."""

from ecphoria.client import (
    ClusterStatusResponse,
    HealthResponse,
    SearchFilters,
    SearchResult,
    StateEntry,
    EcphoriaClient,
    EcphoriaError,
)

__all__ = [
    "ClusterStatusResponse",
    "HealthResponse",
    "SearchFilters",
    "SearchResult",
    "StateEntry",
    "EcphoriaClient",
    "EcphoriaError",
]
__version__ = "0.2.0"
