# news-radar (real-fetch MVP)

Single repository, two services:

- `worker/`: Cloudflare Worker for cross-border fetching and sharded API.
- `scheduler/`: Rust scheduler for rounds, dedupe, scoring, optional LLM enrich, and notifications.

## Layout

```text
news-radar/
  worker/
  scheduler/
  .env.example
  README.md
```

## What is already "real" now

- Worker fetches real upstream data:
  - 62 built-in RSS feeds (official vendors / media / research / community)
  - 7 domestic vendor official web listing sources (Baidu / Aliyun / Zhipu / DeepSeek / Volcengine / MiniMax / Moonshot)
  - Anthropic News web parser
  - GitHub Trending
  - Hugging Face trending models API
- Scheduler calls `/sources` then `/fetch` with shard + cursor.
- Scheduler stores data in SQLite (`raw_items`, `events`, `alerts`).
- Rule-based classification/scoring is enabled.
- Source tier routing (`P0/P1/P2`) is configurable via `scheduler/config/source_tiers.json`.
- Optional OpenAI-compatible LLM enrichment is supported.

## 1) Run Worker

```bash
cd worker
npm install
cp wrangler.example.jsonc wrangler.jsonc
npm run dev
```

## 2) Run Scheduler

```bash
cd scheduler
cp ../.env.example .env
cargo run
```

## 3) End-to-end check

1. Keep Worker running.
2. Run Scheduler once.
3. Confirm `scheduler/data/news.db` exists and has rows.
4. Confirm terminal prints shortlisted candidates.

## 4) Deploy suggestion

1. Deploy Worker to Cloudflare.
2. Put Worker domain/token into scheduler `.env`.
3. Run scheduler with cron/systemd timer every 5-15 minutes.
4. If one run is not enough, increase `HARVEST_ROUNDS`.
