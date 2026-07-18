# AutoGen with Ecphoria Memory Backend

This example shows how to use [Ecphoria](https://github.com/VargaFoundation/ecphoria) as a persistent memory backend for [AutoGen](https://github.com/microsoft/autogen) agents.

Ecphoria provides episodic, semantic, and state memory — giving your agents long-term recall across conversations.

## Prerequisites

1. A running Ecphoria server:
   ```bash
   docker run -p 8432:8432 ghcr.io/vargafoundation/ecphoria:latest
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set your OpenAI API key (used by AutoGen for LLM calls):
   ```bash
   export OPENAI_API_KEY=sk-...
   ```

## What This Example Does

- **Memory-augmented assistant**: An AutoGen assistant that stores and retrieves context from Ecphoria
- **Retrieval functions**: Custom functions registered with AutoGen that query Ecphoria
- **Cross-conversation persistence**: Agent state and past interactions persist across runs

## Run

```bash
python main.py
```

## Architecture

```
AutoGen Agents
    │
    ├── recall_context()    → semantic search for relevant past context
    ├── save_memory()       → ingest observations into episodic store
    ├── get_agent_state()   → retrieve persistent agent state
    └── set_agent_state()   → persist agent state for future runs
```

## Key Concepts

- **Function calling**: AutoGen agents call Ecphoria-backed functions to read/write memory
- **Context augmentation**: Before responding, the assistant retrieves relevant past events
- **State persistence**: Agent preferences, counters, and notes survive across sessions
