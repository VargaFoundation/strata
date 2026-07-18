"""
Ingest documents into Ecphoria for the RAG pipeline.

Reads .txt and .md files from a directory, splits them into paragraph-based
chunks, and ingests each chunk via the Ecphoria REST API.

Usage:
    python ingest.py                     # Ingest ./sample_docs
    python ingest.py /path/to/docs       # Ingest custom directory
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx

ECPHORIA_URL = os.environ.get("ECPHORIA_URL", "http://localhost:8432")
SOURCE = "langchain-rag"
MIN_CHUNK_LENGTH = 50  # Skip very short chunks.


def split_into_chunks(text: str) -> list[str]:
    """Split text into paragraph-based chunks."""
    paragraphs = text.split("\n\n")
    chunks = []
    current = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Start a new chunk if current is already substantial.
        if len(current) > 500 and para:
            chunks.append(current)
            current = para
        elif current:
            current += "\n\n" + para
        else:
            current = para

    if current:
        chunks.append(current)

    return [c for c in chunks if len(c) >= MIN_CHUNK_LENGTH]


def ingest_file(client: httpx.Client, filepath: Path) -> int:
    """Read a file, split into chunks, and ingest into Ecphoria."""
    text = filepath.read_text(encoding="utf-8")
    chunks = split_into_chunks(text)

    if not chunks:
        print(f"  Skipped {filepath.name} (no substantial chunks)")
        return 0

    events = []
    for i, chunk in enumerate(chunks):
        events.append({
            "event_type": "document.chunk",
            "payload": {
                "filename": filepath.name,
                "chunk_index": i,
                "content": chunk,
                "total_chunks": len(chunks),
            },
        })

    resp = client.post(
        f"{ECPHORIA_URL}/api/v1/ingest",
        json={"source": SOURCE, "events": events},
        timeout=30.0,
    )
    resp.raise_for_status()
    ingested = resp.json().get("ingested", 0)
    print(f"  {filepath.name}: {ingested} chunks ingested")
    return ingested


def main() -> None:
    docs_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "sample_docs"

    if not docs_dir.is_dir():
        print(f"Error: {docs_dir} is not a directory")
        sys.exit(1)

    files = sorted(
        p for p in docs_dir.iterdir()
        if p.suffix in (".txt", ".md") and p.is_file()
    )

    if not files:
        print(f"No .txt or .md files found in {docs_dir}")
        sys.exit(1)

    print(f"Ingesting {len(files)} files from {docs_dir} into Ecphoria ({ECPHORIA_URL})\n")

    with httpx.Client() as client:
        # Health check.
        try:
            client.get(f"{ECPHORIA_URL}/health").raise_for_status()
        except httpx.ConnectError:
            print(f"Error: cannot reach Ecphoria at {ECPHORIA_URL}")
            sys.exit(1)

        total = 0
        for f in files:
            total += ingest_file(client, f)

        print(f"\nDone — {total} total chunks ingested from {len(files)} files.")


if __name__ == "__main__":
    main()
