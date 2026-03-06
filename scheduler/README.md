# scheduler

Rust scheduler for:

- Pulling shards from Worker (`/sources` + `/fetch`).
- Running multi-round harvest.
- Dedupe + store in SQLite.
- Rule-based classification/scoring.
- Tier routing by configurable file (`P0/P1/P2`).
- Dual-LLM pipeline (small filter + big Chinese rewrite, via `async-openai-compat`).
- Notification (console or webhook, Feishu interactive card supported).
- Notification retry based on SQLite state.
- Heuristic semantic dedupe based on story fingerprint.

## Run

```bash
cp ../.env.example .env
cargo run
```

## Build via GitHub Actions (no Rust on server)

This repo provides `.github/workflows/build-scheduler.yml`.

- Trigger:
  - push to `main` (when `scheduler/**` changes), or
  - run manually from GitHub Actions (`workflow_dispatch`).
- Output artifact name: `scheduler-linux-x86_64`
- Artifact files:
  - `news-radar-scheduler`
  - `config/source_tiers.json`
  - `.env.example`

Deploy on server:

```bash
# 1) download and unzip artifact from GitHub Actions
# 2) put files into your deploy directory, e.g. /opt/news-radar/scheduler
chmod +x news-radar-scheduler
cp .env.example .env
# edit .env: WORKER_BASE_URL / WORKER_SHARED_TOKEN / NOTIFY_WEBHOOK ...
./news-radar-scheduler
```

## Key env vars

- `WORKER_BASE_URL`
- `WORKER_SHARED_TOKEN`
- `SQLITE_PATH`
- `SOURCE_TIER_CONFIG`
- `HARVEST_ROUNDS`
- `CANDIDATE_MIN_SCORE`
- `ALERT_RETRY_MAX_ATTEMPTS`
- `ALERT_RETRY_BASE_SECS`
- `SEMANTIC_DEDUPE_LOOKBACK_HOURS`
- `NOTIFY_WEBHOOK`
- `SMALL_LLM_API_BASE` / `SMALL_LLM_API_KEY` / `SMALL_LLM_MODEL` / `SMALL_LLM_CONCURRENCY`
- `SMALL_LLM_API_PROTOCOL`
- `BIG_LLM_API_BASE` / `BIG_LLM_API_KEY` / `BIG_LLM_MODEL` / `BIG_LLM_MAX_ITEMS` / `BIG_LLM_CONCURRENCY`
- `BIG_LLM_API_PROTOCOL`
- `LLM_API_PROTOCOL`

Production example:

- `WORKER_BASE_URL=https://llms-news.xtower.site`

Dual-LLM pipeline:

- Small LLM: runs on all new items, filters AI-related content first.
- Big LLM: rewrites/classifies filtered items to readable Chinese summaries.
- Both stages support independent concurrency limits.
- Scheduler requires both `SMALL_LLM_*` and `BIG_LLM_*` to run.

LLM protocol behavior (both small and big models):

- `SMALL_LLM_API_PROTOCOL` / `BIG_LLM_API_PROTOCOL` support `auto`, `responses`, `chat`.
- Default `auto` mode tries `POST {API_BASE}/responses` first.
- `auto` falls back to `POST {API_BASE}/chat/completions` only when the provider clearly reports that `/responses` is unsupported or unavailable.
- Request-body errors, invalid JSON output, and normal model failures stay on the original protocol and are logged instead of silently falling back.
- Legacy `LLM_API_PROTOCOL` is still honored when `BIG_LLM_*` is omitted.

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
- `alerts`: retry queue + sent dedupe.

## Alert delivery behavior

- New shortlisted candidates are first enqueued into `alerts`.
- Webhook success marks one alert as `sent`.
- Webhook failure marks one alert as `failed` and schedules next retry by backoff.
- Semantic dedupe uses a story fingerprint built from normalized title and short content tokens.
- Recent sent alerts within `SEMANTIC_DEDUPE_LOOKBACK_HOURS` suppress repeated delivery for the same story.
