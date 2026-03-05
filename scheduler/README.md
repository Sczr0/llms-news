# scheduler

Rust scheduler for:

- Pulling shards from Worker (`/sources` + `/fetch`).
- Running multi-round harvest.
- Dedupe + store in SQLite.
- Rule-based classification/scoring.
- Tier routing by configurable file (`P0/P1/P2`).
- Optional LLM enrichment (OpenAI-compatible API).
- Notification (console or webhook, Feishu interactive card supported).

## Run

```bash
cp ../.env.example .env
cargo run
```

## Key env vars

- `WORKER_BASE_URL`
- `WORKER_SHARED_TOKEN`
- `SQLITE_PATH`
- `SOURCE_TIER_CONFIG`
- `HARVEST_ROUNDS`
- `CANDIDATE_MIN_SCORE`
- `NOTIFY_WEBHOOK`
- `LLM_API_BASE` / `LLM_API_KEY` / `LLM_MODEL`

Production example:

- `WORKER_BASE_URL=https://llms-news.xtower.site`

LLM protocol behavior:

- Scheduler tries `POST {LLM_API_BASE}/responses` first.
- If `responses` fails or payload cannot be parsed, it falls back to `POST {LLM_API_BASE}/chat/completions`.

## Tier config

Default file: `scheduler/config/source_tiers.json`

- `default_tier`: fallback tier when no rule matches.
- `notify_min_score`: per-tier minimum score threshold.
- `rules`: ordered matching rules; first match wins.

## Feishu payload

When webhook host contains `open.feishu.cn`, scheduler sends `interactive` card payload with:

- Tier/topic/score in header.
- Source/published_at/reason metadata.
- Angle/opinion fields.
- One-click button to open original URL.

## Tables

- `raw_items`: raw fetched items.
- `events`: topic/score records.
- `alerts`: sent dedupe.
