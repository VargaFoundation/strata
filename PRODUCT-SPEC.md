# PRODUCT SPEC — Context Lake Open Source
# Document de référence produit — v1.0
# À ingérer par Claude Code pour le développement du projet

---

> **⚠️ Note de repositionnement (2026-06) — lire en premier.**
>
> L'analyse marché a montré que la catégorie « context lake » (Tacnode, jan. 2026) est
> naissante et mono-acteur, tandis que la demande **validée et financée** est sur le marché
> *agent-memory* (Mem0, Zep, Letta), qui se joue sur la **qualité de rappel** (benchmark LoCoMo).
>
> **Le produit est donc repositionné** en *« le moteur de mémoire open-source pour agents,
> self-hostable et benchmarkable »* — l'alternative vraiment ouverte là où Mem0 est cloud-first
> et Zep paywall son graphe. Le « context lake / decision coherence » devient un **second acte
> entreprise** (état partagé multi-agents, mémoire auditable en SQL), pas l'accroche.
>
> La couche d'**intelligence mémoire** qui manquait est désormais implémentée dans `strata-core`
> (`memory/cognition.rs`) : mémoires bi-temporelles, résolution de contradictions, dédup/
> consolidation, recherche hybride BM25+vecteur (RRF), oubli par decay, extraction LLM opt-in,
> API compatible Mem0 (REST + MCP), et un harnais d'éval LoCoMo (`examples/locomo_eval.rs`).
> Cible (ICP) : équipes régulées / EU / on-prem qui ne peuvent pas envoyer leurs données au cloud.
>
> Les sections ci-dessous reflètent la vision « context lake » d'origine et restent valables pour
> le substrat (3 moteurs, PG-wire, clustering) ; lire l'accroche produit à travers ce prisme.

---

## 1. RÉSUMÉ EXÉCUTIF

### Nom de travail (à finaliser — voir section 10)
**Strata** (recommandation principale — voir section 10 pour alternatives)

### Tagline
"The open-source context lake. Deploy in 30 seconds. Scale to millions."

### One-liner pitch
Strata est un context lake open source qui unifie mémoire épisodique, sémantique et d'état pour les agents IA dans un seul binaire Rust — déployable en Docker comme MinIO, scalable sur Kubernetes, compatible PostgreSQL et MCP-natif.

### Elevator pitch (60 secondes)
Les agents IA en production aujourd'hui opèrent sur des données fragmentées. L'agent de fraude voit un snapshot vieux de 5 minutes. L'agent de support ne sait pas que l'agent d'analyse a déjà détecté un problème. Chaque agent a sa propre mémoire dans son propre silo.

Strata résout ça. C'est un context lake — une couche de contexte partagé temps réel pour tous vos agents. Trois types de mémoire (ce qui s'est passé, ce que ça signifie, où on en est) unifiés dans un seul moteur. Les agents lisent et écrivent dans la même réalité. Le tout en open source, déployable en `docker run`, compatible avec les outils existants via le protocole PostgreSQL, et nativement intégré au Model Context Protocol.

Tacnode fait ça pour les grands comptes US sur AWS. Nous le faisons pour tous les autres — en open source, on-premise, RGPD-natif.

---

## 2. ANALYSE CONCURRENTIELLE DÉTAILLÉE

### 2.1 Concurrents directs (Context Lake)

#### Tacnode
- **Fondé** : 2024, Bellevue WA, par Xiaowei Jiang
- **Statut** : Sorti de stealth janvier 2026, venture-funded (montant non divulgué)
- **Revenue** : ~$1.8M estimé (2025), ~16 personnes
- **Produit** : Context Lake managed sur AWS. PostgreSQL-compatible. Moteur unifié (SQL + vectoriel + streaming). Semantic Operators.
- **Client notable** : DoorDash (en production, latence réduite de minutes à centaines de millisecondes)
- **Distribution** : AWS Marketplace, ventes directes
- **Forces** : Paper académique (arXiv), validation Forrester, intégration AWS Bedrock + AgentCore, DoorDash comme référence
- **Faiblesses** : Cloud-only (AWS), pas open source, pas de self-hosted, pricing opaque, écosystème MCP indirect (via AWS), zéro présence européenne
- **Notre différenciation** : Open source, self-hosted, RGPD-natif, MCP intégré, déploiement Docker/K8s

#### Port.io
- **Produit** : Context lake orienté platform engineering / DevOps
- **Angle** : Graphe de connaissances pour le SDLC (ownership, dépendances, criticité)
- **Forces** : Niche bien définie (platform engineering), concept de "golden paths" pour agents
- **Faiblesses** : SaaS uniquement, pas de PostgreSQL, pas de vectoriel, vertical très étroit
- **Notre différenciation** : Généraliste (pas limité au DevOps), SQL-compatible, vectoriel intégré

### 2.2 Concurrents adjacents (Agent Memory)

#### Mem0
- **Fondé** : 2024, YC, $24.5M (Series A, octobre 2025)
- **GitHub** : 48K+ stars
- **Produit** : Memory-as-a-service pour agents IA. API simple (3 lignes de code). Extraction automatique de mémoires. Scopes user/session/agent.
- **Forces** : Écosystème le plus large, intégration AWS Agent SDK, DX excellente, traction massive
- **Faiblesses** : Cloud-first (données sur serveurs Mem0), graph features derrière paywall $249/mo, score LongMemEval moyen (49%), pas de requêtes SQL, pas de feature store, pas de contexte opérationnel temps réel
- **Notre différenciation** : Strata n'est PAS un memory framework — c'est une infrastructure data. Mem0 stocke des "souvenirs" d'interactions. Strata stocke l'état opérationnel complet d'un système. Mem0 est le cerveau conversationnel. Strata est la source de vérité partagée.

#### Zep (Graphiti)
- **Produit** : Temporal knowledge graph pour agents. Graphiti engine open source.
- **Forces** : Meilleur score temporel (LongMemEval 63.8%), graph-based, production-ready
- **Faiblesses** : Nécessite Neo4j/FalkorDB, self-hosting complexe, cloud pricing élevé, pas de feature store temps réel
- **Notre différenciation** : Strata unifie tout dans un seul moteur — pas besoin de gérer Neo4j + vector DB + feature store séparément

#### Letta (ex-MemGPT)
- **Fondé** : 2024, $10M seed (Felicis Ventures)
- **GitHub** : 21K+ stars
- **Produit** : Agent runtime avec mémoire tiered (core/recall/archival). Le LLM gère lui-même sa mémoire.
- **Forces** : Architecture OS-like innovante, contrôle explicite de la mémoire, local-LLM friendly
- **Faiblesses** : Lock-in dans le framework Letta, mémoire couplée au runtime agent, pas de SQL, pas de contexte partagé multi-agent
- **Notre différenciation** : Strata est agnostique du framework agent. N'importe quel agent peut y lire/écrire. Pas de runtime propriétaire.

#### Cognee
- **Produit** : Knowledge graph + vector search, 30+ connecteurs data, multimodal
- **Forces** : Tourne entièrement en local (SQLite + LanceDB + Kuzu), pipeline d'extraction structurée
- **Faiblesses** : Orienté RAG/extraction, pas de contexte opérationnel temps réel
- **Notre différenciation** : Strata est une base de données, pas un pipeline d'extraction

#### Supermemory
- **Produit** : Memory API avec MCP integrations, orienté coding agents
- **Forces** : SOC2/HIPAA/GDPR certifié, self-hosted disponible, MCP-first
- **Faiblesses** : Niche (coding agents), pas de SQL, pas de feature store
- **Notre différenciation** : Généraliste, SQL-native, feature store intégré

### 2.3 Concurrents infrastructure (Data layer)

#### Vector DBs (Chroma, Milvus, Qdrant, Weaviate, pgvector)
- Résolvent le problème du stockage/recherche vectorielle UNIQUEMENT
- Pas de mémoire épisodique, pas d'état, pas de transactions cross-type, pas de MCP
- Strata les remplace en incluant le vectoriel + tout le reste

#### Feature stores (Feast, Tecton, Featureform)
- Orientés ML classique, pas agents IA
- Batch-first pour la plupart
- Strata inclut un feature store temps réel comme sous-ensemble de la State Memory

#### Stream processing (Kafka, Flink, NATS)
- Transport de messages, pas stockage de contexte
- Strata consomme depuis ces systèmes, ne les remplace pas

#### Bases temps réel (Redis, DragonflyDB)
- Cache/state, pas de persistance analytique ni vectorielle
- Strata offre la persistance + analytique + vectoriel dans un seul système

### 2.4 Matrice comparative synthétique

| Critère | Strata | Tacnode | Mem0 | Zep | Letta | pgvector |
|---------|----------|---------|------|-----|-------|----------|
| Open source | Apache 2.0 | Non | Partiel | Partiel | Oui | Oui |
| Self-hosted | Oui (Docker/K8s) | Non (AWS) | Oui (limité) | Oui (complexe) | Oui | Oui |
| PostgreSQL compatible | Oui (wire) | Oui | Non | Non | Non | Natif |
| Vectoriel intégré | Oui | Oui | Oui | Oui | Non | Oui |
| MCP natif | Oui | Via AWS | Non | Non | Non | Non |
| Mémoire épisodique | Oui | Oui | Oui | Oui | Oui | Non |
| Mémoire sémantique | Oui | Oui | Oui | Oui | Oui | Partiel |
| Mémoire d'état | Oui | Oui | Non | Non | Partiel | Non |
| Feature store | Oui | Oui | Non | Non | Non | Non |
| Transactions ACID | Oui | Oui | Non | Non | Non | Oui |
| RGPD natif | Oui | Non | Non | Non | Non | Dépend |
| Proxy LLM | Oui | Non | Non | Non | Oui | Non |
| Multi-agent context | Oui | Oui | Partiel | Partiel | Non | Non |
| Decision Coherence | Oui | Oui | Non | Non | Non | Non |
| Kubernetes Operator | Oui | Non | Non | Non | Non | Non |
| Prix entry | Gratuit | Devis | Gratuit (limité) | Gratuit (limité) | Gratuit | Gratuit |

---

## 3. FEATURES DÉTAILLÉES

### 3.1 Core Engine

#### 3.1.1 Trois mémoires unifiées

**Episodic Memory** (ce qui s'est passé)
- Stockage append-only d'événements bruts (WAL-based)
- Champs : source, event_type, payload (JSONB), timestamp, metadata
- Rétention configurable par source (TTL policies)
- Requêtes temporelles optimisées (indexation par time range)
- Ingestion bulk (batch) et unitaire (streaming)
- Export RGPD : extraction complète par entity_id

**Semantic Memory** (ce que ça veut dire)
- Stockage de vecteurs d'embeddings avec métadonnées
- Index HNSW via USearch (sub-milliseconde en recherche)
- Support multi-modèle d'embedding (configurable par collection)
- Chunking automatique des textes longs
- Recherche hybride : vectorielle + filtres métadonnées
- Mise à jour non-destructive : les interprétations changent, les faits restent

**State Memory** (où on en est)
- Key-value store transactionnel pour l'état live
- Scopes : agent, session, entity, global
- MVCC pour isolation des lectures concurrentes
- Watchers : notifications en temps réel sur changement d'état
- TTL par clé (états temporaires auto-expirés)
- Atomic compare-and-swap pour coordination multi-agents

#### 3.1.2 Decision Coherence
- Toute requête cross-mémoire s'exécute dans une seule transaction MVCC
- Snapshot cohérent garanti : un agent voit la même réalité que les autres à l'instant T
- Pas de drift possible entre mémoires
- Isolation configurable : read committed, snapshot, serializable

#### 3.1.3 PostgreSQL Wire Protocol
- Compatible psql, DBeaver, pgAdmin, Metabase, Grafana, DataGrip
- Dialecte SQL étendu avec fonctions contextuelles :
  - `embed(text)` : calcule un embedding à la volée
  - `cosine_similarity(vec1, vec2)` : similarité cosinus
  - `kontext_search(query, top_k, filters)` : recherche hybride
  - `kontext_state(agent_id, key)` : accès rapide à l'état
- Transactions SQL standard (BEGIN, COMMIT, ROLLBACK)
- Prepared statements, connection pooling

#### 3.1.4 REST & gRPC API
- OpenAPI 3.1 spec complète
- Endpoints : /ingest, /query, /search, /state, /embed, /health
- Authentification : API keys, JWT, OAuth2
- Rate limiting configurable
- WebSocket pour streaming d'événements et watchers d'état
- gRPC pour intégrations haute performance

### 3.2 MCP (Model Context Protocol) Integration

#### 3.2.1 Serveur MCP intégré
- Transport : Streamable HTTP (SSE + POST)
- Auto-discovery : l'agent MCP voit automatiquement les tables, collections et agents disponibles
- Primitives exposées :

**Resources** (lecture passive de données) :
- `strata://episodic/{source}` — événements par source
- `strata://semantic/{collection}` — collections vectorielles
- `strata://state/{agent_id}` — état d'un agent
- `strata://schema` — schéma complet du context lake
- `strata://stats` — métriques (volume, latence, santé)

**Tools** (actions exécutables) :
- `query(sql)` — requête SQL libre
- `ingest(events[])` — ingestion d'événements
- `search(text, options)` — recherche sémantique
- `get_state(agent_id, key)` — lecture d'état
- `set_state(agent_id, key, value)` — écriture d'état
- `embed(text)` — calcul d'embedding
- `list_sources()` — sources d'événements disponibles
- `list_agents()` — agents enregistrés
- `recall(entity_id, time_range)` — rappel contextuel complet

**Prompts** (templates réutilisables) :
- `decision_context(agent_id, decision_type)` — contexte de décision formaté
- `entity_summary(entity_id)` — résumé d'une entité
- `incident_briefing(incident_id)` — briefing d'incident
- `recent_activity(source, timeframe)` — activité récente

#### 3.2.2 Configuration MCP
```json
// claude_desktop_config.json / cursor / vscode
{
  "mcpServers": {
    "strata": {
      "url": "http://localhost:8432/mcp",
      "transport": "streamable-http",
      "auth": { "type": "bearer", "token": "${STRATA_API_KEY}" }
    }
  }
}
```

### 3.3 Proxy LLM (RAG transparent)

#### 3.3.1 Endpoint OpenAI-compatible
- `POST /v1/chat/completions` — drop-in replacement
- Intercepte la requête, enrichit avec le contexte pertinent, forward au LLM configuré
- Multi-provider : Anthropic, OpenAI, Google, Mistral, Ollama, vLLM, llama.cpp, LocalAI
- Configuration du backend LLM par variable d'env ou API

#### 3.3.2 Stratégies de contexte
```json
{
  "kontext": {
    "strategy": "auto|manual|none",
    "sources": ["episodic", "semantic", "state"],
    "time_window": "1h",
    "max_context_tokens": 4000,
    "entity_extraction": true,
    "reranking": true
  }
}
```

- **auto** : analyse la question, identifie les entités, cherche le contexte pertinent automatiquement
- **manual** : le développeur spécifie exactement quelles sources/filtres utiliser
- **none** : pass-through simple (proxy sans enrichissement)

#### 3.3.3 Semantic caching
- Cache les réponses LLM par similarité sémantique
- Si une question similaire a déjà été posée avec le même contexte → retourne le cache
- Réduction potentielle de 40-70% des appels LLM
- TTL configurable, invalidation sur changement de contexte

### 3.4 Ingestion & Connecteurs

#### 3.4.1 Ingestion native
- HTTP POST `/api/v1/ingest` (JSON, batch)
- WebSocket streaming (événements continus)
- gRPC streaming (haute performance)
- Fichiers : CSV, JSON, NDJSON (import bulk)

#### 3.4.2 Connecteurs CDC (Change Data Capture)
- **PostgreSQL** : logical replication (pgoutput/wal2json)
- **MySQL** : binlog streaming
- **MongoDB** : change streams

#### 3.4.3 Connecteurs messaging
- **Kafka** : consumer group natif
- **NATS** : subscriber natif
- **Redis Streams** : consumer
- **RabbitMQ** : consumer (AMQP)

#### 3.4.4 Connecteurs webhook (événements applicatifs)
- Grafana (alertes)
- GitLab / GitHub (deploys, PRs, issues)
- PagerDuty / OpsGenie (incidents)
- PostHog (analytics events)
- Sentry (errors)
- Slack (messages/réactions)
- n8n / Zapier (webhooks génériques)
- Custom webhooks (schéma configurable)

#### 3.4.5 Pipeline d'embedding
- Embedding automatique à l'ingestion (configurable par source)
- Providers : Ollama (local), sentence-transformers (local), OpenAI, Cohere, Voyage AI, Mistral
- Chunking configurable : fixed-size, sentence-based, semantic
- Re-embedding en background quand le modèle change

### 3.5 Materialized Views incrémentales

```sql
-- Feature store temps réel : les features sont recalculées 
-- en continu à chaque nouvel événement
CREATE MATERIALIZED VIEW user_risk_features AS
SELECT
  payload->>'user_id' as user_id,
  count(*) FILTER (WHERE ts > NOW() - '1 hour') as txn_1h,
  avg((payload->>'amount')::float) FILTER (WHERE ts > NOW() - '24 hours') as avg_amount_24h,
  count(DISTINCT payload->>'country') FILTER (WHERE ts > NOW() - '7 days') as countries_7d,
  max(ts) as last_seen
FROM episodic
WHERE event_type = 'transaction'
GROUP BY payload->>'user_id';

-- Requête en < 5ms même sur millions d'événements
SELECT * FROM user_risk_features WHERE user_id = 'usr_42';
```

### 3.6 Administration & Observabilité

#### 3.6.1 CLI
```bash
strata status                    # santé du cluster
strata sources list              # sources d'ingestion
strata agents list               # agents enregistrés
strata query "SELECT count(*) FROM episodic"
strata ingest --source myapp --file events.json
strata export --entity usr_42 --format json   # export RGPD
strata backup --target s3://bucket/backup
strata restore --from s3://bucket/backup/2026-04-08
```

#### 3.6.2 Web UI (optionnel)
- Dashboard : volume d'ingestion, latence, agents actifs
- Explorateur : requêtes SQL interactives, recherche vectorielle
- Admin : sources, connecteurs, embedding models, users
- Timeline : visualisation chronologique des événements par entité

#### 3.6.3 Métriques Prometheus / Grafana
- Métriques standard : ingestion rate, query latency (p50/p95/p99), storage size, active connections
- Dashboards Grafana pré-configurés (JSON importable)
- Alerting rules recommandées

### 3.7 Sécurité & Compliance

#### 3.7.1 Authentification
- API keys (simple, pour dev/internal)
- JWT tokens (pour applications)
- OAuth2 / OIDC (pour SSO enterprise)
- mTLS (pour service-to-service)

#### 3.7.2 Autorisation (RBAC)
- Rôles : admin, writer, reader, agent
- Permissions granulaires : par source, par collection, par agent
- Row-level security (filtrage par tenant/org)

#### 3.7.3 RGPD
- `DELETE FROM * WHERE entity_id = ?` — suppression cascade dans les 3 mémoires + vecteurs
- Export complet par entité (right to access)
- TTL policies automatiques (data minimization)
- Audit log de chaque accès (qui a lu quoi, quand)
- Chiffrement at rest (AES-256) et in transit (TLS 1.3)
- Pas de télémétrie par défaut (opt-in uniquement)

---

## 4. ARCHITECTURE TECHNIQUE

### 4.1 Choix technologiques

| Composant | Technologie | Justification |
|-----------|-------------|---------------|
| Langage | Rust | Performance, safety, binaire unique. Pas de JVM, pas de runtime. |
| SQL engine | DuckDB (embedded) | Analytique columnar, zero config, MIT license, excellent pour JSONB |
| Vector index | USearch (Rust bindings) | HNSW, 10x plus compact que FAISS, open source, single-header |
| WAL/Persistence | Custom Rust (+ SQLite pour metadata) | Append-only log pour episodic, B-tree pour state |
| Object storage | S3/MinIO (tiered) | Hot/warm/cold tiering. Séparation compute/storage en mode cluster |
| Consensus | Raft (via openraft crate) | Pour le mode cluster multi-nœuds |
| PG Protocol | pgwire crate | Wire protocol PostgreSQL en Rust |
| MCP Server | Custom Rust (axum + SSE) | Streamable HTTP transport |
| HTTP framework | Axum | Async Rust, performant, bien maintenu |
| Serialization | FlatBuffers ou MessagePack | Pour le format interne et gRPC |
| Config | TOML + env vars | Convention over configuration |

### 4.2 Modes de déploiement

#### Mode Standalone (Docker)
```
┌──────────────────────────┐
│     Single Container     │
│  ┌────────┐ ┌─────────┐ │
│  │ Gateway│ │ Engine  │ │
│  │ (PG+   │ │ (DuckDB │ │
│  │  MCP+  │ │  +USearch│ │
│  │  REST) │ │  +WAL)  │ │
│  └────────┘ └─────────┘ │
│        /data (volume)    │
└──────────────────────────┘
```
- Un seul binaire, un seul conteneur
- Stockage local (volume Docker)
- Parfait pour : dev, test, POC, petites prod (< 10M events, < 1M vecteurs)

#### Mode Compose (Docker Compose)
```
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│ Strata │ │  MinIO   │ │  Ollama  │ │ Strata   │
│ (engine) │ │ (storage)│ │ (embed)  │ │ UI       │
└──────────┘ └──────────┘ └──────────┘ └──────────┘
```
- Stack complète avec object storage et embedding local
- Parfait pour : équipes de 5-20, production PME

#### Mode Cluster (Kubernetes)
```
┌─────────────────────────────────────────┐
│          Kubernetes Cluster             │
│  ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │ Strata-1 │ │ Strata-2 │ │Strata-3│ │
│  │ (leader) │ │(follower)│ │(follow)│ │
│  └────┬─────┘ └────┬─────┘ └───┬────┘ │
│       └─────────────┼───────────┘      │
│              ┌──────▼──────┐           │
│              │  S3/MinIO   │           │
│              │  (shared)   │           │
│              └─────────────┘           │
│  ┌───────────┐ ┌───────────────┐       │
│  │ Ollama    │ │ Prometheus+   │       │
│  │ (embed)   │ │ Grafana       │       │
│  └───────────┘ └───────────────┘       │
└─────────────────────────────────────────┘
```
- Helm chart + Kubernetes Operator (CRD)
- Auto-scaling horizontal (plus de nœuds = plus de throughput)
- Tiered storage : hot (mémoire/SSD) → warm (SSD) → cold (S3)
- Parfait pour : enterprise, haute dispo, multi-tenant

### 4.3 Structure du projet (pour Claude Code)

```
strata/
├── Cargo.toml                    # Workspace Rust
├── Cargo.lock
├── README.md
├── LICENSE                       # Apache 2.0
├── Dockerfile
├── docker-compose.yml
├── Makefile
│
├── crates/
│   ├── strata-core/              # Moteur principal
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── memory/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── episodic.rs   # WAL + time-indexed store
│   │   │   │   ├── semantic.rs   # Vector store + HNSW index
│   │   │   │   └── state.rs      # KV store + MVCC
│   │   │   ├── query/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── planner.rs    # Query planning
│   │   │   │   ├── executor.rs   # Query execution
│   │   │   │   └── functions.rs  # embed(), cosine_similarity(), etc.
│   │   │   ├── storage/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── local.rs      # Local disk storage
│   │   │   │   ├── s3.rs         # S3/MinIO backend
│   │   │   │   └── tiering.rs    # Hot/warm/cold management
│   │   │   ├── ingest/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── http.rs       # HTTP ingest endpoint
│   │   │   │   ├── kafka.rs      # Kafka consumer
│   │   │   │   ├── cdc.rs        # PostgreSQL CDC
│   │   │   │   └── webhook.rs    # Webhook receiver
│   │   │   ├── embedding/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── ollama.rs
│   │   │   │   ├── openai.rs
│   │   │   │   └── local.rs      # sentence-transformers via ONNX
│   │   │   └── materialized.rs   # Incremental materialized views
│   │   └── Cargo.toml
│   │
│   ├── strata-gateway/           # Couche protocole
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── pg_wire.rs        # PostgreSQL wire protocol
│   │   │   ├── rest.rs           # REST API (axum)
│   │   │   ├── grpc.rs           # gRPC server
│   │   │   ├── mcp.rs            # MCP server (SSE + HTTP)
│   │   │   ├── llm_proxy.rs      # LLM proxy (OpenAI-compat)
│   │   │   └── auth.rs           # Auth middleware
│   │   └── Cargo.toml
│   │
│   ├── strata-cluster/           # Mode distribué
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── raft.rs           # Consensus Raft
│   │   │   ├── replication.rs    # Réplication des données
│   │   │   └── coordinator.rs    # Coordination des nœuds
│   │   └── Cargo.toml
│   │
│   └── strata-cli/               # CLI admin
│       ├── src/
│       │   └── main.rs
│       └── Cargo.toml
│
├── strata-server/                # Binaire principal
│   ├── src/
│   │   └── main.rs               # Point d'entrée, config, startup
│   └── Cargo.toml
│
├── sdk/
│   ├── python/                   # SDK Python (PyO3 ou HTTP client)
│   │   ├── strata/
│   │   │   ├── __init__.py
│   │   │   ├── client.py
│   │   │   ├── memory.py
│   │   │   └── types.py
│   │   ├── pyproject.toml
│   │   └── tests/
│   │
│   ├── typescript/               # SDK TypeScript
│   │   ├── src/
│   │   │   ├── index.ts
│   │   │   ├── client.ts
│   │   │   └── types.ts
│   │   ├── package.json
│   │   └── tests/
│   │
│   └── go/                       # SDK Go
│       ├── strata.go
│       └── go.mod
│
├── deploy/
│   ├── docker/
│   │   ├── Dockerfile
│   │   └── docker-compose.yml
│   ├── helm/
│   │   └── strata/
│   │       ├── Chart.yaml
│   │       ├── values.yaml
│   │       └── templates/
│   └── operator/
│       ├── api/v1alpha1/
│       └── controllers/
│
├── docs/
│   ├── getting-started.md
│   ├── architecture.md
│   ├── mcp-integration.md
│   ├── llm-proxy.md
│   ├── deployment.md
│   ├── api-reference.md
│   ├── sql-reference.md
│   └── gdpr.md
│
├── grafana/
│   └── dashboards/
│       └── strata-overview.json
│
├── tests/
│   ├── integration/
│   └── benchmarks/
│
└── website/                      # Landing page + docs (Astro ou Next.js)
    ├── src/
    └── package.json
```

---

## 5. EXEMPLES D'INTÉGRATION DÉTAILLÉS

### 5.1 Agent support client multi-LLM

**Scénario** : Un chatbot support (Claude) et un agent d'analyse (Mistral local) partagent le contexte client.

```python
# SDK Python — Agent support
from strata import Strata

db = Strata("localhost:8432")

# 1. Ingestion de l'événement de contact
db.ingest({
    "source": "support-bot",
    "event_type": "customer.contact",
    "payload": {
        "customer_id": "cust_42",
        "channel": "chat",
        "message": "J'ai été facturé deux fois pour ma commande #1234",
        "sentiment": "frustrated"
    }
})

# 2. Enrichissement sémantique
db.semantic.upsert(
    collection="customers",
    entity_id="cust_42",
    text="Client fidèle depuis 2020, 3 incidents précédents résolus, " +
         "panier moyen 89€, préfère résolution rapide",
    metadata={"tier": "gold", "lifetime_value": 2847.50}
)

# 3. L'agent d'analyse (Mistral) lit le même contexte
context = db.query("""
    SELECT
        e.payload->>'message' as last_message,
        s.metadata->>'tier' as customer_tier,
        s.metadata->>'lifetime_value' as ltv,
        (SELECT count(*) FROM episodic 
         WHERE payload->>'customer_id' = 'cust_42'
         AND event_type = 'customer.complaint'
         AND ts > NOW() - INTERVAL '90 days') as recent_complaints
    FROM episodic e
    JOIN semantic s ON s.entity_id = e.payload->>'customer_id'
    WHERE e.source = 'support-bot'
    ORDER BY e.ts DESC LIMIT 1
""")

# 4. Résultat : les deux agents voient exactement le même contexte
# → customer_tier: "gold", ltv: 2847.50, recent_complaints: 1
# → L'agent priorise automatiquement ce client
```

### 5.2 Détection de fraude temps réel

```python
# Pipeline de détection de fraude
from strata import Strata

db = Strata("strata.internal:8432")

# Materialized view (créée une fois)
db.query("""
    CREATE MATERIALIZED VIEW fraud_features AS
    SELECT
        payload->>'user_id' as user_id,
        count(*) FILTER (WHERE ts > NOW() - '10 min') as txn_10m,
        count(*) FILTER (WHERE ts > NOW() - '1 hour') as txn_1h,
        sum((payload->>'amount')::float) FILTER (WHERE ts > NOW() - '1 hour') as spend_1h,
        count(DISTINCT payload->>'ip') FILTER (WHERE ts > NOW() - '1 hour') as ips_1h,
        count(DISTINCT payload->>'country') FILTER (WHERE ts > NOW() - '24 hours') as countries_24h,
        bool_or(payload->>'device_fingerprint' IN (
            SELECT known_fraudulent FROM fraud_watchlist
        )) as known_device
    FROM episodic
    WHERE event_type = 'payment.attempt'
    GROUP BY payload->>'user_id'
""")

# À chaque transaction (< 10ms)
def evaluate_transaction(txn):
    # 1. Ingestion
    db.ingest({
        "source": "payment-gateway",
        "event_type": "payment.attempt",
        "payload": txn
    })
    
    # 2. Features temps réel + profil sémantique (une seule transaction)
    result = db.query("""
        SELECT 
            f.txn_10m, f.spend_1h, f.ips_1h, f.countries_24h,
            s.metadata->>'risk_profile' as risk_profile,
            cosine_similarity(
                s.embedding,
                embed('transaction suspecte carte volée pays étranger')
            ) as fraud_similarity
        FROM fraud_features f
        JOIN semantic s ON s.entity_id = f.user_id
        WHERE f.user_id = %s
    """, [txn["user_id"]])
    
    # 3. Décision en < 10ms
    if result.fraud_similarity > 0.85 or result.txn_10m > 5:
        db.state.set("fraud-agent", f"alert:{txn['user_id']}", {
            "status": "flagged",
            "reason": "high_similarity_and_velocity",
            "txn_id": txn["txn_id"]
        })
        return "BLOCK"
    
    return "ALLOW"
```

### 5.3 Agent SRE avec Claude Code (via MCP)

```
# L'utilisateur dans Claude Code / Claude Desktop :
> "Qu'est-ce qui s'est passé sur le service checkout dans la dernière heure ?"

# Claude interroge Strata via MCP automatiquement :
# 1. tools/call → query("SELECT * FROM episodic WHERE ...")
# 2. tools/call → get_state("sre-agent", "checkout-service")

# Réponse de Claude (avec contexte de Strata) :
"Voici ce qui s'est passé sur checkout dans la dernière heure :

1. 14h32 — Deploy v2.3.1 (source: gitlab)
2. 14h37 — Alerte P99 latency > 2s (source: grafana)
3. 14h39 — Error rate +340% sur /api/checkout (source: posthog)
4. 14h41 — Incident P1 créé automatiquement (source: pagerduty)
5. 14h55 — Rollback vers v2.3.0 (source: gitlab)
6. 14h58 — Latence revenue à la normale (source: grafana)

Le deploy v2.3.1 semble avoir introduit une régression sur le
endpoint checkout. Le rollback a résolu le problème. L'état actuel
du service est 'healthy'. Je recommande d'investiguer les changements
dans v2.3.1 avant de retenter le deploy."
```

### 5.4 RAG entreprise en temps réel

```typescript
// SDK TypeScript — Chatbot interne avec RAG
import { Strata } from '@strata/sdk';

const db = new Strata('http://strata:8432');

// Ingestion continue depuis Confluence (webhook)
app.post('/webhooks/confluence', async (req, res) => {
  const { page } = req.body;
  await db.ingest({
    source: 'confluence',
    eventType: 'page.updated',
    payload: {
      page_id: page.id,
      title: page.title,
      content: page.body.storage.value,
      space: page.space.key,
      author: page.version.by.displayName,
    }
  });
  // Strata chunk + embed automatiquement
  res.sendStatus(200);
});

// Chatbot : utilise le proxy LLM de Strata
const response = await fetch('http://strata:8432/v1/chat/completions', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    model: 'claude-sonnet-4-20250514',
    messages: [
      { role: 'user', content: userQuestion }
    ],
    kontext: {
      strategy: 'auto',
      sources: ['semantic'],
      filters: { source: 'confluence' },
      max_context_tokens: 3000,
    }
  })
});
// → Strata trouve les chunks Confluence pertinents,
//   les injecte dans le prompt, forward à Claude
```

### 5.5 Orchestration multi-agents avec n8n

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Agent   │     │  Agent   │     │  Agent   │
│ Ingestion│────▶│ Analyse  │────▶│ Action   │
│ (n8n)    │     │ (Claude) │     │ (n8n)    │
└────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │
     ▼                ▼                ▼
┌──────────────────────────────────────────┐
│              Strata                     │
│  Episodic ◄──── Semantic ◄──── State     │
│  (events)       (meaning)      (status)  │
└──────────────────────────────────────────┘
```

Chaque agent lit et écrit dans Strata. L'agent d'analyse voit les événements ingérés. L'agent d'action voit les conclusions de l'analyse. Tout est cohérent.

### 5.6 Home Assistant + Strata (domotique intelligente)

```yaml
# Strata ingère les événements Home Assistant
# via webhook ou via MCP bridge

# L'agent IA peut alors :
# - Corréler les patterns d'activité (episodic)
# - Comprendre les habitudes sémantiquement (semantic)
# - Maintenir l'état de la maison (state)

# Exemple : "Pourquoi il fait froid dans le salon ?"
# Strata contexte :
# - episodic: chauffage éteint il y a 2h (source: home-assistant)
# - episodic: fenêtre ouverte il y a 45min (source: capteur)
# - semantic: "le salon est orienté nord, mal isolé"
# - state: thermostat = OFF, température extérieure = 4°C
```

---

## 6. DÉPLOIEMENT DÉTAILLÉ

### 6.1 Docker (30 secondes)

```bash
# Minimal — stockage local, embedding OpenAI
docker run -d --name strata \
  -p 5432:5432 -p 8432:8432 \
  -v strata-data:/data \
  -e STRATA_LLM_PROVIDER=openai \
  -e OPENAI_API_KEY=sk-... \
  strata/strata:latest

# 100% local — embedding Ollama, zéro cloud
docker run -d --name strata \
  -p 5432:5432 -p 8432:8432 \
  -v strata-data:/data \
  -e STRATA_EMBEDDING_PROVIDER=ollama \
  -e STRATA_EMBEDDING_MODEL=nomic-embed-text \
  -e OLLAMA_URL=http://host.docker.internal:11434 \
  strata/strata:latest

# Test immédiat
psql -h localhost -U strata -d strata \
  -c "SELECT version();"
# → Strata v0.1.0 (context lake engine)
```

### 6.2 Docker Compose (stack complète)

```yaml
version: '3.8'
services:
  strata:
    image: strata/strata:latest
    ports: ["5432:5432", "8432:8432"]
    environment:
      STRATA_STORAGE: minio
      STRATA_S3_ENDPOINT: http://minio:9000
      STRATA_S3_BUCKET: strata
      STRATA_S3_ACCESS_KEY: minioadmin
      STRATA_S3_SECRET_KEY: minioadmin
      STRATA_EMBEDDING_PROVIDER: ollama
      STRATA_EMBEDDING_MODEL: nomic-embed-text
      OLLAMA_URL: http://ollama:11434
      STRATA_MCP_ENABLED: "true"
      STRATA_LLM_PROXY_ENABLED: "true"
      STRATA_LLM_PROVIDER: anthropic
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
    depends_on: [minio, ollama]
    volumes: [strata-data:/data]

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes: [minio-data:/data]

  ollama:
    image: ollama/ollama
    volumes: [ollama-data:/root/.ollama]
    deploy:
      resources:
        reservations:
          devices:
            - capabilities: [gpu]  # optionnel

  strata-ui:
    image: strata/strata-ui:latest
    ports: ["3000:3000"]
    environment:
      STRATA_URL: http://strata:8432

volumes:
  strata-data:
  minio-data:
  ollama-data:
```

### 6.3 Kubernetes (Helm)

```bash
helm repo add strata https://charts.strata.dev
helm install strata strata/strata \
  --namespace strata --create-namespace \
  --values production-values.yaml
```

```yaml
# production-values.yaml
replicas: 3
resources:
  requests: { cpu: "2", memory: "8Gi" }
  limits: { cpu: "4", memory: "16Gi" }

storage:
  type: s3
  s3:
    endpoint: http://minio.minio:9000
    bucket: strata-prod
  tiering:
    hot: 7d      # mémoire + SSD
    warm: 30d    # SSD only
    cold: s3     # object storage

embedding:
  provider: ollama
  url: http://ollama.ollama:11434
  model: nomic-embed-text
  batchSize: 64

mcp:
  enabled: true
  auth: oauth2
  ingress:
    enabled: true
    host: strata-mcp.internal.example.com

llmProxy:
  enabled: true
  provider: anthropic
  ingress:
    enabled: true
    host: strata-llm.internal.example.com

monitoring:
  prometheus: true
  grafanaDashboard: true

ingress:
  enabled: true
  className: nginx
  host: strata.internal.example.com
  tls:
    enabled: true
    secretName: strata-tls

backup:
  enabled: true
  schedule: "0 2 * * *"
  target: s3://strata-backups/
  retention: 30d
```

### 6.4 Kubernetes Operator (CRD)

```yaml
apiVersion: strata.dev/v1alpha1
kind: StrataCluster
metadata:
  name: production
  namespace: strata
spec:
  replicas: 3
  version: "0.5.0"
  storage:
    type: s3
    bucket: strata-prod
    tiering: { hot: 7d, warm: 30d }
  embedding:
    provider: ollama
    model: nomic-embed-text
  mcp: { enabled: true, auth: oauth2 }
  monitoring: { prometheus: true }
  backup: { schedule: "0 2 * * *", retention: 30d }
---
# L'Operator gère automatiquement :
# - Scaling horizontal
# - Rolling updates
# - Failover automatique
# - Backup/restore
# - Certificate rotation
```

---

## 7. SÉCURITÉ & RGPD

### 7.1 RGPD — Fonctionnalités intégrées

```sql
-- Right to access (Article 15)
SELECT * FROM strata_export('entity_id', 'usr_42');
-- → Exporte TOUTES les données liées à usr_42 
--   dans les 3 mémoires en JSON

-- Right to erasure (Article 17)
CALL strata_erase('entity_id', 'usr_42');
-- → Supprime dans episodic, semantic ET state
-- → Supprime les vecteurs associés
-- → Log l'opération dans l'audit trail

-- Data minimization (Article 5)
ALTER SOURCE 'payment-gateway' SET RETENTION '90 days';
ALTER SOURCE 'analytics' SET RETENTION '30 days';
-- → Les données sont automatiquement purgées

-- Audit trail
SELECT * FROM strata_audit_log
WHERE entity_id = 'usr_42'
ORDER BY ts DESC;
-- → Qui a accédé à quoi, quand, via quel agent
```

### 7.2 Architecture de sécurité

```
┌────────────────────────────────────┐
│          TLS 1.3                   │
│  ┌──────────────────────────────┐  │
│  │     Auth Layer               │  │
│  │  API Key │ JWT │ OAuth2/OIDC │  │
│  ├──────────────────────────────┤  │
│  │     RBAC Engine              │  │
│  │  role → permissions → scope  │  │
│  ├──────────────────────────────┤  │
│  │     Audit Logger             │  │
│  │  every read/write logged     │  │
│  ├──────────────────────────────┤  │
│  │     Encryption at Rest       │  │
│  │  AES-256 (BYOK en Enterprise)│  │
│  └──────────────────────────────┘  │
└────────────────────────────────────┘
```

---

## 8. BUSINESS MODEL

### 8.1 Tiers

| | Community | Pro | Enterprise |
|---|---|---|---|
| **Licence** | Apache 2.0 | Commercial | Commercial |
| **Prix** | Gratuit | €49/nœud/mois | Sur devis |
| **Core engine** | ✅ Complet | ✅ Complet | ✅ Complet |
| **3 mémoires** | ✅ | ✅ | ✅ |
| **MCP server** | ✅ | ✅ | ✅ |
| **LLM proxy** | ✅ | ✅ | ✅ |
| **PG wire** | ✅ | ✅ | ✅ |
| **REST/gRPC** | ✅ | ✅ | ✅ |
| **Docker/Compose** | ✅ | ✅ | ✅ |
| **Helm chart** | ✅ | ✅ | ✅ |
| **CLI** | ✅ | ✅ | ✅ |
| **SDKs** | ✅ | ✅ | ✅ |
| **RBAC avancé** | Basique | ✅ Granulaire | ✅ + Row-level |
| **SSO (SAML/OIDC)** | ❌ | ✅ | ✅ |
| **Audit log enrichi** | Basique | ✅ Complet | ✅ + Export |
| **Backup automatisé** | Manuel | ✅ Schedulé | ✅ + Cross-DC |
| **Tiered storage** | Local | ✅ S3 | ✅ + BYOK encryption |
| **K8s Operator** | ❌ | ❌ | ✅ |
| **Multi-tenancy** | ❌ | ❌ | ✅ |
| **HA Cross-DC** | ❌ | ❌ | ✅ |
| **Compliance pack** | ❌ | ❌ | ✅ (RGPD/SOC2/ISO) |
| **Support** | Community | Email prioritaire | Dédié + SLA 99.9% |

### 8.2 Roadmap 18 mois

| Phase | Période | Livrables | Métriques |
|-------|---------|-----------|-----------|
| **Fondations** | M0–M3 | Core Rust (DuckDB+USearch+WAL), PG wire, REST API, Docker image, ingestion HTTP, recherche vectorielle, docs | Dogfooding sur propre infra |
| **MCP & LLM** | M3–M6 | MCP server, LLM proxy, pipeline embedding, SDK Python+TS, Helm v1, materialized views, landing page | 10 early adopters |
| **Production** | M6–M9 | Mode cluster (Raft), tiered storage, CDC PostgreSQL, connecteurs webhook, CLI, K8s Operator beta, Grafana dashboards | 50 utilisateurs community |
| **Monétisation** | M9–M12 | Launch Pro tier, RBAC+audit, SSO, backup, docs enterprise, premier meetup | 200 stars GitHub, 5 clients Pro |
| **Scale** | M12–M18 | Multi-tenancy, HA cross-DC, marketplace connecteurs, SOC2, partenariats EU | 1000 stars, 20 clients Pro/Enterprise |

---

## 9. MARKETING & VENTE

### 9.1 Positionnement

**Catégorie** : Context Lake / AI Data Infrastructure
**Analogie** : "MinIO pour le contexte IA" (ou "Supabase pour les agents IA")

**Message clé** :
> Vos agents IA sont aveugles. Ils opèrent sur des données périmées, fragmentées, incohérentes.
> Strata leur donne la vue. Une source de vérité partagée, temps réel, on-premise.
> Déployez en 30 secondes. Gardez vos données chez vous.

### 9.2 Personas cibles

**P1 — Le DevOps/SRE qui déploie des agents IA** (toi)
- Frustré par les pipelines batch, les silos de données
- Veut une solution self-hosted, K8s-native
- Adopte par Docker/Helm, pousse en interne ensuite
- Canal : HackerNews, Reddit r/selfhosted, r/kubernetes, blogs techniques

**P2 — Le développeur IA/ML en startup**
- Build des agents multi-LLM, cherche une mémoire partagée
- Utilise déjà Mem0 ou pgvector mais ça ne scale pas
- Veut du SQL + vectoriel dans le même outil
- Canal : Twitter/X, Discord communautés IA, ProductHunt

**P3 — Le CTO/VP Eng de PME européenne**
- Contraint par le RGPD, veut du on-premise
- Cherche une alternative à Tacnode qui ne soit pas AWS-only
- Décide sur la base de la conformité et du TCO
- Canal : LinkedIn, conférences (KubeCon EU, Devoxx, Paris AI)

**P4 — L'intégrateur/consultant IA en France**
- Déploie des solutions IA pour ses clients
- Cherche des briques open source enterprise-grade
- Revend du support et de l'intégration
- Canal : Meetups, partenariats directs

### 9.3 Stratégie de contenu (6 premiers mois)

#### Blog technique (sur le site + dev.to + Medium)
1. "Why your AI agents are blind — and how to fix it" (thought leadership)
2. "Context Lake vs Data Lake vs Vector DB — what's the difference?" (éducation)
3. "Deploy a shared context layer for your AI agents in 30 seconds" (tutorial)
4. "How we built a PostgreSQL-compatible context lake in Rust" (build in public)
5. "MCP + Strata: Give Claude access to your company's brain" (integration guide)
6. "GDPR-ready AI infrastructure: why on-premise matters" (positioning EU)
7. "From Mem0 to Strata: when your agents need more than memory" (comparison)
8. "Real-time fraud detection with a context lake" (use case deep dive)
9. "Self-hosted RAG in 5 minutes: Strata + Ollama + your docs" (tutorial)
10. "The architecture behind Strata: Rust, DuckDB, and zero-copy vectors" (deep tech)

#### Réseaux sociaux
- **Twitter/X** : Build in public, partage de métriques, réponses aux threads sur agent memory
- **LinkedIn** : Posts sur RGPD + IA, context engineering, cas d'usage enterprise
- **Reddit** : r/selfhosted, r/kubernetes, r/LocalLLaMA, r/MachineLearning
- **HackerNews** : Show HN au lancement, Ask HN pour feedback

#### Conférences & meetups
- **Paris AI Meetup** : Talk "Context Lake: la couche manquante de votre stack IA"
- **KubeCon EU** : Lightning talk "Kubernetes-native AI context layer"
- **Devoxx France** : "Du Data Lake au Context Lake"
- **Meetup local** : Organisation d'un meetup mensuel "AI Infrastructure"

### 9.4 Launch strategy (ProductHunt + HackerNews)

**ProductHunt** :
- Titre : "Strata — Open source context lake for AI agents"
- Tagline : "Deploy in 30 seconds. Your agents share the same brain."
- Jour : Mardi ou mercredi (meilleur traffic)
- Assets : vidéo demo 60s, GIF d'un `docker run` → requête SQL → résultat
- Hunters : solliciter un hunter connu dans l'IA

**HackerNews (Show HN)** :
- Titre : "Show HN: Strata – an open-source context lake for AI agents (Rust, PG-compatible)"
- Body : problème → solution → demo → GitHub link
- Timing : publication entre 8h et 10h EST (14h-16h Paris)

### 9.5 Funnel de vente

```
AWARENESS                    ADOPTION                 CONVERSION
───────────                  ──────────               ───────────
Blog posts          ──→     docker run        ──→    Pro trial
HN/Reddit/Twitter   ──→     GitHub star       ──→    Sales call
Conf talks          ──→     Helm install      ──→    Enterprise POC
SEO ("context lake") ──→    SDK integration   ──→    Annual contract
```

### 9.6 Pricing psychology

- **Community gratuit sans limite** : pas de "10K events gratuits puis payez". Tout le core est gratuit. C'est le modèle MinIO/PostHog. Ça crée la confiance.
- **Pro à €49/nœud/mois** : assez bas pour qu'un dev passe en charge de carte bancaire sans approbation. 3 nœuds = €147/mois. Compétitif vs Mem0 Pro ($249/mois pour un seul feature).
- **Enterprise sur devis** : pour les deals > €10K/an avec compliance + support + SLA.

### 9.7 SEO Keywords

**Primaires** : context lake, context lake open source, AI agent memory, shared context AI agents
**Secondaires** : self-hosted AI memory, GDPR AI infrastructure, PostgreSQL vector database, MCP server, agent context layer, real-time AI context, on-premise AI, Tacnode alternative, Mem0 alternative self-hosted
**Long-tail** : "how to give AI agents shared memory", "deploy context lake docker", "kubernetes AI agent infrastructure", "GDPR compliant AI agent memory", "open source alternative to tacnode"

---

## 10. NAMING & DOMAINES

### 10.1 Critères de naming

- Court (< 10 caractères idéalement)
- Prononçable en français et en anglais
- Évoque : fondation, ancrage, mémoire, contexte, vérité
- Domaine .dev ou .io disponible (ou .com)
- Pas de conflit avec un produit existant majeur
- Fonctionne comme commande CLI (`strata query ...`)

### 10.2 Propositions classées

| Rang | Nom | CLI | Domaines suggérés | Concept | Notes |
|------|-----|-----|-------------------|---------|-------|
| 1 | **Strata** | `strata` | strata.dev, stratadb.dev | Couches géologiques — les strates de mémoire. | Élu. Élégant, évoque la profondeur et les couches de données. |
| 2 | **Bedrock** | `bedrock` | getbedrock.dev, bedrock.run | La couche fondamentale. | ⚠️ Conflit potentiel avec AWS Bedrock. À éviter pour cette raison. |
| 3 | **Strata** | `strata` | strata.dev, stratadb.dev | Couches géologiques — les strates de mémoire. | Élégant, évoque la profondeur. Vérifier les conflits. |
| 4 | **Mnemonic** | `mnemo` | mnemonic.dev, mnemo.dev | Art de la mémoire (grec). | Culturel, peut être difficile à épeler. |
| 5 | **Engram** | `engram` | engram.dev, engramdb.dev | Trace mnésique en neuroscience. | Technique, plaisant pour les nerds. |
| 6 | **Axon** | `axon` | axondb.dev, axon.run | Le câble du neurone. | Court, punchy. Vérifier les conflits. |
| 7 | **Lattice** | `lattice` | latticedb.dev | Structure cristalline — interconnexion. | Évoque le réseau de données. |
| 8 | **Anamnesis** | `anam` | anamnesis.dev | "Réminiscence" en philosophie (Platon). | Profond, mais difficile à prononcer en anglais. |
| 9 | **Soma** | `soma` | somadb.dev | Corps du neurone (neuroscience). | Court, mémorable. Connotations drogue à vérifier. |
| 10 | **Phrenos** | `phrenos` | phrenos.dev | "Esprit" en grec ancien. | Original, racine de "phrenology". Connotation historique négative. |
| 11 | **Cortex** | `cortex` | cortexlake.dev | Le cortex cérébral. | Déjà utilisé par pas mal de projets. |
| 12 | **Mnemos** | `mnemos` | mnemos.dev, mnemos.io | Variation de Mnemosyne (déesse de la mémoire). | Mythologique, beau. |
| 13 | **Epoch** | `epoch` | epochdb.dev | Unité de temps, moment clé. | Simple, clair, lié au temporel. |
| 14 | **Substrate** | `substrate` | substratedb.dev | La couche sous-jacente. | ⚠️ Conflit avec Substrate (Polkadot). |
| 15 | **Palimpsest** | `palim` | palimpsest.dev | Manuscrit réécrit — mémoire qui évolue. | Intellectuellement riche. Trop long. |

### 10.3 Recommandation

**Strata** est le choix le plus solide :
- "Ground truth" est un concept universellement compris en IA/ML
- "Grounding" est le terme Bessemer/Forrester pour connecter l'IA à la réalité
- `ground` en CLI est naturel : `strata query`, `strata ingest`, `strata status`
- strata.dev est probablement disponible (nouveau TLD, niche)
- Pas de conflit majeur identifié
- Fonctionne aussi comme verbe : "Ground your agents in reality"

**Alternative forte** : **Engram** si on veut un positionnement plus neuroscience/recherche.
**Alternative safe** : **Strata** si on veut quelque chose de plus corporate.

### 10.4 Domaines à réserver (en priorité)

```
strata.dev       ← principal
strata.io        ← alternatif
strata.com       ← si disponible
getstrata.com    ← fallback
strata-db.dev      ← protection
```

Et si choix alternatif :
```
engram.dev / engramdb.dev
strata.dev / stratadb.dev
mnemos.dev / mnemos.io
```

---

## 11. ÉLÉMENTS VISUELS & BRANDING

### 11.1 Identité visuelle

- **Couleur primaire** : #00E5A0 (vert émeraude vif — fraîcheur, données vivantes)
- **Couleur secondaire** : #0A0A0F (noir profond — technique, sérieux)
- **Accent** : #FF6B6B (rouge corail — alertes, urgence)
- **Police display** : JetBrains Mono ou IBM Plex Mono (technique, open source)
- **Police body** : Inter ou Source Sans Pro (lisibilité)

### 11.2 Logo concept

- Symbole : hexagone (◈) avec 3 couches internes (représentant les 3 mémoires)
- Ou : diamant géométrique évoquant une coupe stratigraphique
- Style : géométrique, minimal, monochrome avec accent couleur

### 11.3 Messaging framework

| Audience | Message | Proof point |
|----------|---------|-------------|
| Développeur | "Deploy a shared brain for your AI agents in 30 seconds" | `docker run` → psql → résultat en 30s |
| CTO | "GDPR-native AI context layer. Your data stays on your servers." | Zéro dépendance cloud, audit log intégré |
| Investisseur | "We're building MinIO for the AI context layer — open source, bottom-up adoption" | Marché context lake validé par Forrester, zéro concurrent open source |
| Recrutement | "Building the foundational infrastructure layer for the agentic AI era, in Rust" | Paper arXiv, DX first-class, open source |

### 11.4 Pages clés du site

1. **Homepage** : Hero ("Ground your agents in reality") + docker run demo + features grid + testimonials
2. **Docs** : Getting started en 5 min, architecture, API reference, SQL reference, MCP guide
3. **Blog** : Build in public, use cases, comparisons
4. **Pricing** : 3 tiers, comparaison claire
5. **GitHub README** : Le plus important — c'est la première impression

---

## 12. README.md (draft pour GitHub)

```markdown
<p align="center">
  <img src="logo.svg" width="120" />
</p>

<h1 align="center">Strata</h1>
<p align="center">
  <strong>The open-source context lake for AI agents.</strong><br>
  Deploy in 30 seconds. Scale to millions. Keep your data on your servers.
</p>

<p align="center">
  <a href="https://strata.dev/docs">Docs</a> •
  <a href="https://strata.dev/docs/quickstart">Quickstart</a> •
  <a href="https://discord.gg/strata">Discord</a> •
  <a href="https://strata.dev/blog">Blog</a>
</p>

---

Strata is a **context lake** — a unified data layer that gives your AI agents
a shared, real-time understanding of reality. It combines three types of memory
in a single engine:

- **Episodic Memory** — What happened (events, logs, actions)
- **Semantic Memory** — What it means (embeddings, entities, relationships)
- **State Memory** — Where things stand (live agent state, features, decisions)

All three are queried in a single ACID transaction, so every agent sees the same
coherent snapshot of reality. No stale data. No conflicting views.

## Why Strata?

| Problem | Without Strata | With Strata |
|---------|------------------|---------------|
| Agent A and B see different data | ✗ Conflicting decisions | ✓ Same snapshot, same reality |
| Context is 5 minutes old | ✗ Fraud slips through | ✓ Sub-10ms freshness |
| Need SQL + vectors + state | ✗ 3 separate databases | ✓ One engine, one query |
| GDPR compliance | ✗ Data scattered everywhere | ✓ One `CALL strata_erase()` |
| Connecting to Claude/GPT | ✗ Custom integration code | ✓ Built-in MCP server |

## Quick Start

```bash
docker run -d -p 5432:5432 -p 8432:8432 strata/strata:latest
```

```sql
-- Connect with any PostgreSQL client
psql -h localhost -U strata -d strata

-- Ingest an event
INSERT INTO episodic (source, event_type, payload)
VALUES ('my-app', 'user.signup', '{"user_id": "u1", "plan": "pro"}');

-- Search semantically
SELECT * FROM kontext_search('frustrated customer billing issue', 5);

-- Check agent state
SELECT * FROM state WHERE agent_id = 'support-bot';
```

## MCP Integration

Strata includes a built-in MCP server. Add it to Claude Desktop, Cursor,
or any MCP-compatible agent:

```json
{
  "mcpServers": {
    "strata": {
      "url": "http://localhost:8432/mcp"
    }
  }
}
```

## Features

- 🔌 **PostgreSQL wire protocol** — Works with psql, DBeaver, Metabase, Grafana
- 🧠 **Three unified memories** — Episodic + Semantic + State in one transaction
- 🤖 **MCP server built-in** — Native integration with Claude, Cursor, VS Code
- 🔍 **Hybrid search** — SQL + vector similarity + metadata filters
- 🏠 **Self-hosted** — Your data never leaves your servers
- 🇪🇺 **GDPR-native** — Built-in erasure, export, audit, retention policies
- 📦 **One binary** — Docker, Compose, Kubernetes, or embedded
- ⚡ **Rust-powered** — Sub-10ms queries, minimal resource usage
- 🔄 **LLM proxy** — OpenAI-compatible endpoint with automatic RAG

## Deployment

| Mode | Command | Best for |
|------|---------|----------|
| Docker | `docker run strata/strata` | Dev, small prod |
| Compose | `docker compose up` | Teams, medium prod |
| Kubernetes | `helm install strata` | Enterprise, HA |

## License

Apache 2.0 — Use it however you want.
```

---

## 13. PITCH DECK OUTLINE (pour investisseurs ou early adopters)

1. **Le problème** : Les agents IA sont aveugles. Ils opèrent sur des données fragmentées et périmées.
2. **La taille du marché** : $100B+ investis dans l'IA en 2025. L'infra data pour agents est le prochain goulot d'étranglement (Bessemer, Forrester).
3. **La solution** : Strata — context lake open source. 3 mémoires unifiées, PostgreSQL-compatible, MCP-natif.
4. **Le produit** : Demo live — `docker run` → ingestion → requête → résultat en 30 secondes.
5. **Le marché** : Context lakes validés par Forrester (feb 2026). Tacnode est cloud-only/US. Zéro solution open source.
6. **Le business model** : Open core (Community gratuit → Pro €49/nœud → Enterprise devis).
7. **La traction** : [à remplir — GitHub stars, Docker pulls, early adopters].
8. **L'avantage** : Open source + self-hosted + RGPD = moat européen. MCP-natif = intégration universelle.
9. **L'équipe** : [profil technique, expérience K8s/infra production].
10. **L'ask** : [ce qu'on cherche — early adopters, feedback, contribution, ou seed].

---

## 14. MÉTRIQUES DE SUCCÈS

### 14.1 Mois 3
- [ ] Core engine fonctionnel (3 mémoires + SQL + vectoriel)
- [ ] Docker image publiée
- [ ] README + docs getting started
- [ ] 5 early adopters en test
- [ ] Dogfooding sur propre infra

### 14.2 Mois 6
- [ ] MCP server intégré
- [ ] LLM proxy fonctionnel
- [ ] Helm chart v1
- [ ] SDK Python + TypeScript
- [ ] 50 GitHub stars
- [ ] 1 article de blog viral
- [ ] 10 utilisateurs actifs

### 14.3 Mois 12
- [ ] Mode cluster (3+ nœuds)
- [ ] 200+ GitHub stars
- [ ] 5K+ Docker pulls/mois
- [ ] 5 clients Pro payants
- [ ] €3K–5K MRR
- [ ] 1 talk en conférence

### 14.4 Mois 18
- [ ] 1000+ GitHub stars
- [ ] 20+ clients Pro/Enterprise
- [ ] €15K+ MRR
- [ ] SOC2 en cours
- [ ] 3+ partenaires intégrateurs

---

*Document généré le 2026-04-08. Version 1.0.*
*À utiliser comme spécification de référence pour le développement avec Claude Code.*
