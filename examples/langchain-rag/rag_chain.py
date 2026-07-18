"""
RAG pipeline: Ecphoria retriever → prompt → LLM → answer with sources.

Usage:
    python rag_chain.py                          # Uses Ollama (default)
    LLM_PROVIDER=openai python rag_chain.py      # Uses OpenAI
"""

from __future__ import annotations

import os
import sys

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough

from ecphoria_retriever import EcphoriaRetriever

ECPHORIA_URL = os.environ.get("ECPHORIA_URL", "http://localhost:8432")
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "ollama")

RAG_PROMPT = ChatPromptTemplate.from_template("""\
Answer the question based on the following context retrieved from the knowledge base.
If the context doesn't contain enough information, say so — don't make things up.

Context:
{context}

Question: {question}

Answer:""")


def get_llm():
    """Create LLM based on LLM_PROVIDER env var."""
    if LLM_PROVIDER == "openai":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=0,
        )
    else:
        from langchain_ollama import ChatOllama
        return ChatOllama(
            model=os.environ.get("OLLAMA_MODEL", "llama3.2"),
            base_url=os.environ.get("OLLAMA_URL", "http://localhost:11434"),
            temperature=0,
        )


def format_docs(docs) -> str:
    """Format retrieved documents into a context string."""
    parts = []
    for i, doc in enumerate(docs, 1):
        source = doc.metadata.get("filename", "unknown")
        score = doc.metadata.get("score", 0)
        parts.append(f"[{i}] (source: {source}, relevance: {score:.2f})\n{doc.page_content}")
    return "\n\n".join(parts)


def print_sources(docs) -> None:
    """Print the retrieved source documents."""
    if not docs:
        print("  (no sources retrieved)")
        return
    for i, doc in enumerate(docs, 1):
        source = doc.metadata.get("filename", "?")
        score = doc.metadata.get("score", 0)
        preview = doc.page_content[:100].replace("\n", " ")
        print(f"  [{i}] {source} (score: {score:.2f}) — {preview}...")


def main() -> None:
    retriever = EcphoriaRetriever(
        ecphoria_url=ECPHORIA_URL,
        k=5,
        source_filter="langchain-rag",
    )
    llm = get_llm()

    chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | RAG_PROMPT
        | llm
        | StrOutputParser()
    )

    print(f"RAG pipeline ready (LLM: {LLM_PROVIDER}, Ecphoria: {ECPHORIA_URL})")
    print("Type a question (or 'quit' to exit).\n")

    while True:
        try:
            question = input("Question: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question or question.lower() in ("quit", "exit"):
            print("Goodbye!")
            break

        # Retrieve docs separately so we can display sources.
        docs = retriever.invoke(question)

        print("\nSources:")
        print_sources(docs)

        # Run the full chain.
        answer = chain.invoke(question)
        print(f"\nAnswer: {answer}\n")


if __name__ == "__main__":
    main()
