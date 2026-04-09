import { useState } from "react";

const S = {
  root: { fontFamily: "'JetBrains Mono','SF Mono','Fira Code',monospace", background: "#08080d", color: "#d0d0d8", minHeight: "100vh" },
  hdr: { padding: "32px 20px 16px", borderBottom: "1px solid rgba(255,255,255,0.05)" },
  logo: { fontSize: 26, fontWeight: 800, color: "#00e5a0", margin: 0, letterSpacing: "-0.02em" },
  sub: { fontSize: 12, color: "#666", marginTop: 4 },
  nav: { display: "flex", gap: 0, overflowX: "auto", borderBottom: "1px solid rgba(255,255,255,0.05)", padding: "0 12px", WebkitOverflowScrolling: "touch" },
  nb: (a) => ({ padding: "10px 14px", background: "none", border: "none", borderBottom: a ? "2px solid #00e5a0" : "2px solid transparent", color: a ? "#00e5a0" : "#555", cursor: "pointer", fontSize: 11, fontFamily: "inherit", whiteSpace: "nowrap" }),
  main: { padding: "20px", maxWidth: 880, margin: "0 auto" },
  h2: { fontSize: 20, fontWeight: 700, color: "#fff", margin: "0 0 16px", letterSpacing: "-0.01em" },
  card: { background: "rgba(255,255,255,0.025)", borderRadius: 8, padding: "16px", marginBottom: 12, border: "1px solid rgba(255,255,255,0.05)" },
  h3: { fontSize: 14, fontWeight: 700, color: "#bbb", margin: "0 0 10px" },
  p: { fontSize: 12.5, lineHeight: 1.7, color: "#999", margin: "0 0 10px" },
  sm: { fontSize: 11.5, color: "#777", lineHeight: 1.5, margin: "4px 0 0" },
  tag: { fontSize: 14, color: "#00e5a0", fontStyle: "italic", marginBottom: 20, padding: "10px 14px", background: "rgba(0,229,160,0.05)", borderRadius: 6, borderLeft: "3px solid #00e5a0" },
  code: { background: "rgba(0,0,0,0.45)", borderRadius: 6, padding: "12px 14px", fontSize: 11, lineHeight: 1.45, color: "#90dbb8", overflowX: "auto", fontFamily: "inherit", whiteSpace: "pre", margin: "8px 0", border: "1px solid rgba(255,255,255,0.03)" },
  tbl: { width: "100%", borderCollapse: "collapse", fontSize: 11 },
  th: { textAlign: "left", padding: "6px 10px", borderBottom: "1px solid rgba(255,255,255,0.08)", color: "#777", fontWeight: 600 },
  td: { padding: "6px 10px", borderBottom: "1px solid rgba(255,255,255,0.03)", color: "#999" },
  row: { display: "flex", gap: 12, padding: "8px 10px", background: "rgba(0,0,0,0.2)", borderRadius: 4, alignItems: "flex-start", marginBottom: 2 },
  lbl: { fontSize: 11, fontWeight: 700, color: "#00e5a0", minWidth: 130, flexShrink: 0 },
  val: { fontSize: 11, color: "#888", lineHeight: 1.5, flex: 1 },
  grid: { display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(200px,1fr))", gap: 10 },
  gcard: { background: "rgba(0,0,0,0.3)", borderRadius: 8, padding: 14, border: "1px solid rgba(255,255,255,0.04)" },
  glbl: { fontSize: 12, fontWeight: 700, color: "#00e5a0", marginBottom: 6 },
  big: { fontSize: 22, fontWeight: 700, color: "#00e5a0", margin: "6px 0" },
};

const sections = [
  {
    id: "pitch", title: "Pitch", icon: "◈",
    render: () => (
      <div>
        <h2 style={S.h2}>GroundDB — The Open-Source Context Lake</h2>
        <div style={S.tag}>"MinIO pour le contexte IA — déployez en 30 secondes, scalez à l'infini"</div>
        <div style={S.card}>
          <h3 style={S.h3}>Elevator Pitch (60s)</h3>
          <p style={S.p}>Les agents IA en production opèrent sur des données fragmentées. L'agent de fraude voit un snapshot vieux de 5 minutes. L'agent de support ne sait pas que l'agent d'analyse a déjà détecté un problème.</p>
          <p style={S.p}><strong>GroundDB</strong> est un context lake open source — une couche de contexte partagé temps réel pour tous vos agents. Trois types de mémoire unifiés dans un seul moteur Rust. Compatible PostgreSQL, MCP-natif, déployable en <code>docker run</code>.</p>
          <p style={S.p}>Tacnode fait ça pour les grands comptes US sur AWS. Nous le faisons pour tous les autres — en open source, on-premise, RGPD-natif.</p>
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Messaging par audience</h3>
          {[
            { who: "Développeur", msg: "Deploy a shared brain for your AI agents in 30 seconds", proof: "docker run → psql → résultat en 30s" },
            { who: "CTO / VP Eng", msg: "GDPR-native AI context layer. Your data stays on your servers.", proof: "Zéro dépendance cloud, audit log intégré, Apache 2.0" },
            { who: "Investisseur", msg: "We're building MinIO for the AI context layer", proof: "Marché validé par Forrester, zéro concurrent open source" },
          ].map(m => (
            <div key={m.who} style={S.row}>
              <div style={S.lbl}>{m.who}</div>
              <div style={S.val}><strong>{m.msg}</strong><br/>{m.proof}</div>
            </div>
          ))}
        </div>
      </div>
    ),
  },
  {
    id: "compete", title: "Concurrence", icon: "⚔",
    render: () => (
      <div>
        <h2 style={S.h2}>Analyse concurrentielle</h2>
        <div style={S.card}>
          <h3 style={S.h3}>Matrice comparative</h3>
          <div style={{ overflowX: "auto" }}>
            <table style={S.tbl}>
              <thead>
                <tr>{["", "GroundDB", "Tacnode", "Mem0", "Zep", "Letta", "pgvector"].map((h, i) => <th key={i} style={{ ...S.th, color: i === 1 ? "#00e5a0" : S.th.color }}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {[
                  ["Open source", "Apache 2.0", "Non", "Partiel", "Partiel", "Oui", "Oui"],
                  ["Self-hosted", "Docker/K8s", "Non (AWS)", "Limité", "Complexe", "Oui", "Oui"],
                  ["PostgreSQL", "Wire proto", "Oui", "Non", "Non", "Non", "Natif"],
                  ["Vectoriel", "✅ USearch", "✅", "✅", "✅", "Non", "✅"],
                  ["MCP natif", "✅ intégré", "Via AWS", "Non", "Non", "Non", "Non"],
                  ["3 mémoires", "✅ unified", "✅", "Non", "Non", "Partiel", "Non"],
                  ["Feature store", "✅ matview", "✅", "Non", "Non", "Non", "Non"],
                  ["ACID cross-mem", "✅", "✅", "Non", "Non", "Non", "Oui (SQL)"],
                  ["RGPD natif", "✅ intégré", "Non", "Non", "Non", "Non", "Dépend"],
                  ["LLM proxy", "✅ RAG auto", "Non", "Non", "Non", "Oui", "Non"],
                  ["Prix entry", "Gratuit∞", "Devis", "Gratuit*", "Gratuit*", "Gratuit", "Gratuit"],
                ].map((r, i) => (
                  <tr key={i} style={i % 2 === 0 ? { background: "rgba(255,255,255,0.01)" } : {}}>
                    {r.map((c, j) => <td key={j} style={{ ...S.td, fontWeight: j === 0 ? 600 : 400, color: j === 1 ? "#00e5a0" : S.td.color }}>{c}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        {[
          { name: "Tacnode", fund: "VC (non divulgué)", rev: "~$1.8M", team: "~16", strength: "Paper arXiv, Forrester validation, DoorDash en prod, AWS Marketplace", weak: "Cloud-only AWS, pas open source, zéro Europe, MCP indirect", our: "Open source, self-hosted, RGPD, MCP intégré" },
          { name: "Mem0", fund: "$24.5M Series A", rev: "N/A", team: "~20", strength: "48K GitHub stars, AWS Agent SDK exclusif, DX 3 lignes de code, YC", weak: "Cloud-first, graph features $249/mo, score LongMemEval 49%, pas de SQL/feature store", our: "GroundDB est une infra data, pas un memory API. SQL-native, feature store intégré, on-premise" },
          { name: "Zep (Graphiti)", fund: "N/A", rev: "N/A", team: "~10", strength: "Meilleur score temporel (63.8%), graph-based, production-ready", weak: "Nécessite Neo4j, self-hosting complexe, pas de feature store", our: "Tout dans un seul moteur, pas de dépendance Neo4j" },
          { name: "Letta (MemGPT)", fund: "$10M seed", rev: "N/A", team: "~15", strength: "Architecture OS innovante, contrôle mémoire explicite, local-LLM", weak: "Lock-in framework, mémoire couplée au runtime, pas de SQL/multi-agent", our: "Framework-agnostic, tout agent peut lire/écrire, SQL-native" },
        ].map(c => (
          <div key={c.name} style={S.card}>
            <h3 style={S.h3}>{c.name} — {c.fund} | {c.team} pers.</h3>
            <div style={S.row}><div style={S.lbl}>Forces</div><div style={S.val}>{c.strength}</div></div>
            <div style={S.row}><div style={S.lbl}>Faiblesses</div><div style={S.val}>{c.weak}</div></div>
            <div style={S.row}><div style={{ ...S.lbl, color: "#00e5a0" }}>Notre edge</div><div style={{ ...S.val, color: "#00e5a0" }}>{c.our}</div></div>
          </div>
        ))}
      </div>
    ),
  },
  {
    id: "features", title: "Features", icon: "⬡",
    render: () => (
      <div>
        <h2 style={S.h2}>Features détaillées</h2>
        {[
          { cat: "Core Engine", color: "#4ecdc4", items: [
            "3 mémoires unifiées (episodic WAL, semantic HNSW, state KV-MVCC)",
            "Decision Coherence : requête cross-mémoire en une seule transaction ACID",
            "PostgreSQL wire protocol (psql, DBeaver, Metabase, Grafana out-of-the-box)",
            "Fonctions SQL étendues : embed(), cosine_similarity(), kontext_search()",
            "Materialized views incrémentales (feature store temps réel)",
            "REST API (OpenAPI 3.1) + gRPC + WebSocket streaming",
          ]},
          { cat: "MCP & LLM", color: "#ff6b6b", items: [
            "Serveur MCP intégré (Streamable HTTP — SSE + POST)",
            "Resources : episodic/{source}, semantic/{collection}, state/{agent}, schema, stats",
            "Tools : query, ingest, search, get_state, set_state, embed, recall",
            "Prompts : decision_context, entity_summary, incident_briefing",
            "Proxy LLM OpenAI-compatible avec RAG automatique (auto/manual/none)",
            "Semantic caching (réduction 40-70% appels LLM)",
            "Multi-provider : Claude, GPT, Gemini, Mistral, Ollama, vLLM, llama.cpp",
          ]},
          { cat: "Ingestion & Connecteurs", color: "#ffd93d", items: [
            "HTTP POST / WebSocket / gRPC streaming / bulk import (CSV, JSON, NDJSON)",
            "CDC : PostgreSQL logical replication, MySQL binlog, MongoDB change streams",
            "Messaging : Kafka, NATS, Redis Streams, RabbitMQ",
            "Webhooks : Grafana, GitLab, GitHub, PagerDuty, PostHog, Sentry, Slack, n8n",
            "Embedding automatique à l'ingestion (configurable par source)",
            "Chunking configurable : fixed-size, sentence-based, semantic",
          ]},
          { cat: "Déploiement", color: "#a29bfe", items: [
            "Un seul binaire Rust (pas de JVM, pas de runtime)",
            "Mode Standalone : docker run (30 secondes)",
            "Mode Compose : stack complète avec MinIO + Ollama",
            "Mode Cluster : Helm chart + Kubernetes Operator (CRD)",
            "Tiered storage : hot (RAM/SSD) → warm (SSD) → cold (S3/MinIO)",
            "Auto-scaling horizontal, rolling updates, failover automatique",
          ]},
          { cat: "Sécurité & RGPD", color: "#00e5a0", items: [
            "Auth : API keys, JWT, OAuth2/OIDC, mTLS",
            "RBAC granulaire (par source, collection, agent) + row-level security",
            "RGPD intégré : ground_erase(), ground_export(), TTL policies, audit log",
            "Chiffrement at rest (AES-256) + in transit (TLS 1.3)",
            "BYOK (Enterprise) — Bring Your Own Key",
            "Zéro télémétrie par défaut (opt-in uniquement)",
          ]},
          { cat: "Observabilité", color: "#e056a0", items: [
            "CLI admin : ground status / query / ingest / export / backup",
            "Web UI optionnelle : dashboard, explorateur SQL, timeline par entité",
            "Métriques Prometheus (ingestion rate, latency p50/95/99, storage, connections)",
            "Dashboards Grafana pré-configurés (JSON importable)",
            "Alerting rules recommandées",
          ]},
        ].map(c => (
          <div key={c.cat} style={{ ...S.card, borderLeft: `3px solid ${c.color}` }}>
            <h3 style={{ ...S.h3, color: c.color }}>{c.cat}</h3>
            {c.items.map((it, i) => <div key={i} style={{ ...S.sm, padding: "3px 0", color: "#999" }}>→ {it}</div>)}
          </div>
        ))}
      </div>
    ),
  },
  {
    id: "usecases", title: "Cas d'usage", icon: "◆",
    render: () => (
      <div>
        <h2 style={S.h2}>Intégrations & cas d'usage</h2>
        {[
          { title: "Support client multi-agents", color: "#4ecdc4", desc: "Agent support (Claude) + agent analyse (Mistral local) partagent le contexte client en temps réel. L'analyse enrichit le profil sémantique, le support lit les features à jour.", code: `db.ingest({source: "support-bot", event_type: "customer.complaint",\n  payload: {customer_id: "cust_42", issue: "double billing"}})\n\n# L'agent analyse lit le même contexte :\nSELECT e.payload, s.metadata->>'tier', s.metadata->>'ltv'\nFROM episodic e JOIN semantic s ON s.entity_id = 'cust_42'\nORDER BY e.ts DESC LIMIT 1;` },
          { title: "Fraude temps réel (< 10ms)", color: "#ff6b6b", desc: "Materialized view incrémentale calcule les features en continu. Chaque transaction est évaluée contre le profil sémantique + features live en une seule transaction.", code: `CREATE MATERIALIZED VIEW fraud_features AS\nSELECT payload->>'user_id' as uid,\n  count(*) FILTER (WHERE ts > NOW()-'10m') as txn_10m,\n  sum((payload->>'amount')::float) FILTER (WHERE ts > NOW()-'1h') as spend_1h\nFROM episodic WHERE event_type='payment' GROUP BY 1;\n\n-- Évaluation en < 10ms\nSELECT f.*, cosine_similarity(s.embedding, embed('stolen card'))\nFROM fraud_features f JOIN semantic s ON s.entity_id=f.uid\nWHERE f.uid='usr_42';` },
          { title: "Agent SRE via MCP (Claude Code)", color: "#a29bfe", desc: 'L\'utilisateur demande "Que s\'est-il passé sur checkout ?" dans Claude Code. Via MCP, Claude interroge GroundDB et corrèle automatiquement les événements de GitLab, Grafana, PostHog et PagerDuty.', code: `# Claude via MCP → tools/call query()\nSELECT source, event_type, payload->>'detail', ts\nFROM episodic\nWHERE payload->>'service' = 'checkout'\n  AND ts > NOW() - INTERVAL '1 hour'\nORDER BY ts;\n\n# Résultat corrélé :\n# 14:32 deploy v2.3.1 (gitlab)\n# 14:37 P99 > 2s (grafana)\n# 14:39 error rate +340% (posthog)\n# 14:55 rollback v2.3.0 (gitlab)` },
          { title: "RAG entreprise temps réel", color: "#ffd93d", desc: "Confluence/Notion push des webhooks à chaque page modifiée. GroundDB chunk+embed en continu. Le chatbot utilise le proxy LLM avec RAG automatique — toujours à jour.", code: `# Webhook Confluence → GroundDB (continu)\nPOST /api/v1/ingest {source: "confluence",\n  events: [{type: "page.updated", content: "..."}]}\n\n# Chatbot via proxy LLM\nPOST /v1/chat/completions {\n  model: "claude-sonnet-4-20250514",\n  messages: [{role:"user", content:"Procédure remboursement ?"}],\n  kontext: {strategy:"auto", sources:["semantic"]}\n}` },
          { title: "Domotique (Home Assistant)", color: "#e056a0", desc: "GroundDB ingère les événements Home Assistant. L'agent corrèle les patterns et maintient l'état de la maison. \"Pourquoi il fait froid ?\" → chauffage OFF + fenêtre ouverte + 4°C dehors.", code: `# Episodic : chauffage éteint (2h), fenêtre ouverte (45min)\n# Semantic : "salon orienté nord, mal isolé"\n# State : thermostat=OFF, temp_ext=4°C\n\n# L'agent IA répond avec le contexte complet :\n# "Le chauffage est éteint depuis 2h et la fenêtre\n#  du salon est ouverte depuis 45 minutes, alors\n#  qu'il fait 4°C dehors."` },
          { title: "Multi-agents n8n / LangChain", color: "#00e5a0", desc: "Chaque agent d'un workflow n8n/LangChain lit et écrit dans GroundDB. L'agent d'ingestion capture, l'agent d'analyse enrichit, l'agent d'action exécute — tous sur le même contexte cohérent.", code: `# Agent 1 (Ingestion) → episodic\n# Agent 2 (Analyse) → semantic + state\n# Agent 3 (Action) → lit state, écrit episodic\n\n# Coordination via State Memory :\nUPDATE state SET value='{"status":"ready_for_action"}'\nWHERE agent_id='analysis' AND key='pipeline_status';\n\n# Agent 3 lit :\nSELECT value FROM state\nWHERE agent_id='analysis' AND key='pipeline_status';` },
        ].map(uc => (
          <div key={uc.title} style={{ ...S.card, borderLeft: `3px solid ${uc.color}` }}>
            <h3 style={{ ...S.h3, color: uc.color }}>{uc.title}</h3>
            <p style={S.p}>{uc.desc}</p>
            <pre style={S.code}>{uc.code}</pre>
          </div>
        ))}
      </div>
    ),
  },
  {
    id: "naming", title: "Naming", icon: "✦",
    render: () => (
      <div>
        <h2 style={S.h2}>Naming & Branding</h2>
        <div style={S.card}>
          <h3 style={{ ...S.h3, color: "#00e5a0" }}>★ Recommandation : GroundDB</h3>
          <p style={S.p}>"Ground truth" est universellement compris en IA/ML. "Grounding" = connecter l'IA à la réalité (terme Bessemer/Forrester). CLI naturel : <code>ground query</code>, <code>ground ingest</code>. Fonctionne comme verbe : "Ground your agents in reality".</p>
          <p style={S.sm}>Domaines : grounddb.dev, grounddb.io, getgrounddb.com</p>
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Top 10 alternatives</h3>
          {[
            { n: "Strata", cli: "strata", dom: "stratadb.dev", concept: "Couches géologiques — strates de mémoire", note: "Élégant, corporate-friendly" },
            { n: "Engram", cli: "engram", dom: "engramdb.dev", concept: "Trace mnésique en neuroscience", note: "Technique, plaisant pour les nerds" },
            { n: "Axon", cli: "axon", dom: "axondb.dev", concept: "Câble du neurone — transmission", note: "Court, punchy. Vérifier conflits" },
            { n: "Mnemos", cli: "mnemos", dom: "mnemos.dev", concept: "Mnemosyne — déesse de la mémoire", note: "Mythologique, beau" },
            { n: "Epoch", cli: "epoch", dom: "epochdb.dev", concept: "Unité de temps, moment clé", note: "Simple, temporel" },
            { n: "Lattice", cli: "lattice", dom: "latticedb.dev", concept: "Structure cristalline — interconnexion", note: "Évoque le réseau" },
            { n: "Mnemonic", cli: "mnemo", dom: "mnemo.dev", concept: "Art de la mémoire (grec)", note: "Culturel, épelage difficile" },
            { n: "Anamnesis", cli: "anam", dom: "anamnesis.dev", concept: "Réminiscence (Platon)", note: "Profond, prononc. difficile" },
            { n: "Soma", cli: "soma", dom: "somadb.dev", concept: "Corps du neurone", note: "Court, connotations à vérifier" },
            { n: "Palimpsest", cli: "palim", dom: "palimpsest.dev", concept: "Manuscrit réécrit — mémoire évolutive", note: "Intellectuel, trop long" },
          ].map((a, i) => (
            <div key={a.n} style={{ ...S.row, background: i === 0 ? "rgba(0,229,160,0.04)" : S.row.background }}>
              <div style={{ ...S.lbl, minWidth: 90 }}>{i + 2}. {a.n}</div>
              <div style={{ ...S.val, minWidth: 60 }}><code>{a.cli}</code></div>
              <div style={{ ...S.val, minWidth: 110, color: "#666" }}>{a.dom}</div>
              <div style={S.val}>{a.concept}. <span style={{ color: "#555" }}>{a.note}</span></div>
            </div>
          ))}
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Identité visuelle</h3>
          <div style={S.grid}>
            <div style={S.gcard}>
              <div style={S.glbl}>Couleurs</div>
              <div style={{ display: "flex", gap: 6 }}>
                {[["#00E5A0","Primary"],["#0A0A0F","Dark"],["#FF6B6B","Accent"],["#4ECDC4","Info"],["#FFD93D","Warning"]].map(([c,n])=>(
                  <div key={c} style={{textAlign:"center"}}>
                    <div style={{width:32,height:32,borderRadius:6,background:c,border:"1px solid rgba(255,255,255,0.1)"}}/>
                    <div style={{fontSize:9,color:"#555",marginTop:3}}>{n}</div>
                  </div>
                ))}
              </div>
            </div>
            <div style={S.gcard}>
              <div style={S.glbl}>Polices</div>
              <div style={S.sm}>Display : JetBrains Mono / IBM Plex Mono</div>
              <div style={S.sm}>Body : Inter / Source Sans Pro</div>
            </div>
            <div style={S.gcard}>
              <div style={S.glbl}>Logo concept</div>
              <div style={S.sm}>Hexagone ◈ avec 3 couches internes (3 mémoires). Géométrique, minimal, monochrome + accent vert.</div>
            </div>
          </div>
        </div>
      </div>
    ),
  },
  {
    id: "marketing", title: "Marketing & Sales", icon: "◇",
    render: () => (
      <div>
        <h2 style={S.h2}>Stratégie Marketing & Vente</h2>
        <div style={S.card}>
          <h3 style={S.h3}>Personas cibles</h3>
          {[
            { who: "P1 — DevOps/SRE", pain: "Pipelines batch, silos de données, agents sans contexte partagé", adopt: "Docker → Helm → push en interne", canal: "HN, Reddit r/selfhosted, r/kubernetes" },
            { who: "P2 — Dev IA/ML startup", pain: "Mem0/pgvector ne scale pas, besoin SQL+vectoriel dans un outil", adopt: "SDK Python, docker-compose", canal: "Twitter/X, Discord IA, ProductHunt" },
            { who: "P3 — CTO PME Europe", pain: "RGPD, veut du on-premise, pas d'alternative à Tacnode", adopt: "POC → Enterprise deal", canal: "LinkedIn, conférences (KubeCon, Devoxx)" },
            { who: "P4 — Intégrateur/consultant", pain: "Cherche des briques open source enterprise-grade pour ses clients", adopt: "Partenariat, revente de support", canal: "Meetups, partenariats directs" },
          ].map(p => (
            <div key={p.who} style={S.row}>
              <div style={{ ...S.lbl, minWidth: 160 }}>{p.who}</div>
              <div style={S.val}><strong>Pain :</strong> {p.pain}<br/><strong>Adoption :</strong> {p.adopt}<br/><strong>Canal :</strong> {p.canal}</div>
            </div>
          ))}
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Contenu (6 premiers mois)</h3>
          {[
            "Why your AI agents are blind — and how to fix it",
            "Context Lake vs Data Lake vs Vector DB — what's the difference?",
            "Deploy a shared context layer for your AI agents in 30 seconds",
            "How we built a PostgreSQL-compatible context lake in Rust",
            "MCP + GroundDB: Give Claude access to your company's brain",
            "GDPR-ready AI infrastructure: why on-premise matters",
            "From Mem0 to GroundDB: when agents need more than memory",
            "Real-time fraud detection with a context lake",
            "Self-hosted RAG in 5 minutes: GroundDB + Ollama",
            "The Rust architecture behind GroundDB",
          ].map((t, i) => <div key={i} style={{ ...S.sm, padding: "3px 0" }}>{i + 1}. {t}</div>)}
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Funnel de vente</h3>
          <pre style={S.code}>{`AWARENESS              ADOPTION                CONVERSION
─────────              ──────────              ───────────
Blog / HN / Reddit  →  docker run         →   Pro trial (€49/nœud)
Conf talks          →  GitHub star         →   Sales call
SEO "context lake"  →  Helm install        →   Enterprise POC
Twitter build-in-   →  SDK integration     →   Annual contract
  public            →  MCP config dans     →   
                        Claude/Cursor`}</pre>
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>SEO Keywords</h3>
          <p style={S.sm}><strong>Primaires :</strong> context lake, context lake open source, AI agent memory, shared context AI agents</p>
          <p style={S.sm}><strong>Secondaires :</strong> self-hosted AI memory, GDPR AI infrastructure, PostgreSQL vector database, MCP server, Tacnode alternative, Mem0 alternative self-hosted</p>
          <p style={S.sm}><strong>Long-tail :</strong> "how to give AI agents shared memory", "deploy context lake docker", "kubernetes AI agent infrastructure", "GDPR compliant AI agent memory"</p>
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Launch strategy</h3>
          <div style={S.row}><div style={S.lbl}>ProductHunt</div><div style={S.val}>"GroundDB — Open source context lake for AI agents". Mardi/mercredi. Vidéo 60s : docker run → psql → MCP → Claude répond avec contexte.</div></div>
          <div style={S.row}><div style={S.lbl}>HackerNews</div><div style={S.val}>"Show HN: GroundDB – open-source context lake (Rust, PG-compatible, MCP-native)". Publication 14h-16h Paris. Body : problème → solution → demo → GitHub.</div></div>
          <div style={S.row}><div style={S.lbl}>Conférences</div><div style={S.val}>KubeCon EU (lightning talk), Devoxx France, Paris AI Meetup, meetup mensuel "AI Infrastructure" auto-organisé.</div></div>
        </div>
      </div>
    ),
  },
  {
    id: "business", title: "Business", icon: "▣",
    render: () => (
      <div>
        <h2 style={S.h2}>Business Model & Roadmap</h2>
        <div style={S.card}>
          <h3 style={S.h3}>Open Core (MinIO / PostHog model)</h3>
          {[
            { tier: "Community (Apache 2.0)", price: "Gratuit ∞", feat: "Core complet, 3 mémoires, MCP, PG wire, REST, Docker, Helm, CLI, SDKs, embedding local. Aucune limite." },
            { tier: "Pro", price: "€49/nœud/mois", feat: "RBAC granulaire, audit log complet, SSO (SAML/OIDC), backup automatisé, tiered storage S3, support email prioritaire." },
            { tier: "Enterprise", price: "Sur devis", feat: "K8s Operator CRD, multi-tenancy, HA cross-DC, BYOK encryption, compliance pack (RGPD/SOC2/ISO), dashboards Grafana, SLA 99.9%, support dédié." },
          ].map(t => (
            <div key={t.tier} style={S.row}>
              <div style={{ ...S.lbl, minWidth: 180 }}>{t.tier}</div>
              <div style={{ fontSize: 11, fontWeight: 700, color: "#00e5a0", minWidth: 100 }}>{t.price}</div>
              <div style={S.val}>{t.feat}</div>
            </div>
          ))}
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Roadmap 18 mois</h3>
          {[
            { phase: "M0–M3 Fondations", color: "#4ecdc4", items: "Core Rust (DuckDB+USearch+WAL), PG wire, REST, Docker, ingestion HTTP, vectoriel, docs, dogfooding" },
            { phase: "M3–M6 MCP & LLM", color: "#ff6b6b", items: "MCP server, LLM proxy, embedding pipeline, SDK Python+TS, Helm v1, matviews, landing page, 10 early adopters" },
            { phase: "M6–M9 Production", color: "#ffd93d", items: "Cluster Raft, tiered storage, CDC PostgreSQL, connecteurs webhook, CLI, K8s Operator beta, Grafana, 50 users" },
            { phase: "M9–M12 Monétisation", color: "#a29bfe", items: "Pro tier, RBAC+audit, SSO, backup, docs enterprise, premier meetup, 200 stars, 5 clients Pro" },
            { phase: "M12–M18 Scale", color: "#00e5a0", items: "Multi-tenancy, HA cross-DC, marketplace connecteurs, SOC2, partenariats EU, 1000 stars, 20 clients" },
          ].map(p => (
            <div key={p.phase} style={S.row}>
              <div style={{ ...S.lbl, color: p.color, minWidth: 150 }}>{p.phase}</div>
              <div style={S.val}>{p.items}</div>
            </div>
          ))}
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Métriques cibles</h3>
          <div style={S.grid}>
            {[
              { m: "GitHub Stars", t3: "—", t6: "50", t12: "200+", t18: "1000+" },
              { m: "Docker pulls/mo", t3: "—", t6: "500", t12: "5K+", t18: "20K+" },
              { m: "Clients Pro", t3: "0", t6: "0", t12: "5", t18: "20+" },
              { m: "MRR", t3: "€0", t6: "€0", t12: "€3-5K", t18: "€15K+" },
            ].map(m => (
              <div key={m.m} style={S.gcard}>
                <div style={S.glbl}>{m.m}</div>
                <div style={{ fontSize: 10, color: "#555" }}>M3: {m.t3} → M6: {m.t6} → M12: {m.t12} → M18: {m.t18}</div>
              </div>
            ))}
          </div>
        </div>
        <div style={S.card}>
          <h3 style={S.h3}>Avantages concurrentiels durables</h3>
          {[
            "Open source + self-hosted = pas de vendor lock-in. Tacnode ne peut pas copier sans détruire son modèle cloud.",
            "MCP-natif dès le jour 1 = intégration universelle avec tout l'écosystème agentic. Aucun concurrent ne fait ça.",
            "RGPD by design = moat européen naturel que les US n'adressent pas.",
            "Effet 'MinIO for AI' = l'open source capture le marché infra. Même playbook, nouveau marché.",
            "Distribution bottom-up = dev adopte en Docker, pousse en interne. Cycle de vente minimal.",
          ].map((a, i) => <div key={i} style={{ ...S.sm, padding: "4px 0" }}>◈ {a}</div>)}
        </div>
      </div>
    ),
  },
];

export default function ProductDoc() {
  const [tab, setTab] = useState("pitch");
  const sec = sections.find(s => s.id === tab);
  return (
    <div style={S.root}>
      <div style={S.hdr}>
        <h1 style={S.logo}>◈ GroundDB — Product Spec</h1>
        <p style={S.sub}>Document de référence produit complet — v1.0 — 2026-04-08</p>
      </div>
      <nav style={S.nav}>
        {sections.map(s => (
          <button key={s.id} style={S.nb(tab === s.id)} onClick={() => setTab(s.id)}>
            {s.icon} {s.title}
          </button>
        ))}
      </nav>
      <main style={S.main}>{sec && sec.render()}</main>
    </div>
  );
}
