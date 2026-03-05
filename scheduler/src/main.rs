use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::header::{HeaderMap, HeaderValue};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
struct Config {
    worker_base_url: String,
    worker_shared_token: String,
    sqlite_path: String,
    source_tier_config_path: String,
    notify_webhook: Option<String>,
    max_shards_fallback: usize,
    worker_page_size: usize,
    harvest_rounds: usize,
    round_sleep_secs: u64,
    candidate_min_score: i32,
    alert_retry_max_attempts: usize,
    alert_retry_base_secs: u64,
    semantic_dedupe_lookback_hours: i64,
    small_llm: Option<SmallLlmConfig>,
    big_llm: Option<BigLlmConfig>,
}

#[derive(Debug, Clone)]
struct SmallLlmConfig {
    api_base: String,
    api_key: String,
    model: String,
    concurrency: usize,
}

#[derive(Debug, Clone)]
struct BigLlmConfig {
    api_base: String,
    api_key: String,
    model: String,
    max_items: usize,
    concurrency: usize,
}

#[derive(Debug, Deserialize)]
struct FetchResponse {
    done: bool,
    next_cursor: Option<usize>,
    shard: usize,
    source_name: String,
    total: usize,
    items: Vec<RawItem>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SourcesResponse {
    sources: Vec<SourceMeta>,
}

#[derive(Debug, Clone, Deserialize)]
struct SourceMeta {
    id: usize,
    name: String,
    kind: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SourceTierConfig {
    default_tier: String,
    notify_min_score: std::collections::HashMap<String, i32>,
    rules: Vec<SourceTierRule>,
}

#[derive(Debug, Clone, Deserialize)]
struct SourceTierRule {
    tier: String,
    #[serde(default)]
    source_name_equals: Vec<String>,
    #[serde(default)]
    source_name_contains: Vec<String>,
    #[serde(default)]
    source_kind_equals: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawItem {
    id: String,
    title: String,
    url: String,
    source: String,
    published_at: String,
    content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Candidate {
    url_hash: String,
    story_key: String,
    story_fingerprint: String,
    published_day: String,
    tier: String,
    topic: String,
    score: i32,
    title: String,
    url: String,
    source: String,
    content: String,
    published_at: String,
    reason: String,
    summary: Option<String>,
    angle: Option<String>,
    opinion: Option<String>,
}

#[derive(Debug)]
struct AlertJob {
    id: i64,
    story_key: String,
    attempt_count: i32,
    payload_json: String,
}

#[derive(Debug)]
struct AlertSummary {
    id: i64,
    status: String,
}

#[derive(Debug)]
struct SemanticAlertHit {
    alert_id: i64,
    matched_by: &'static str,
    matched_story_key: String,
    matched_sent_at: Option<String>,
    matched_title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SmallLlmDecision {
    is_ai_related: Option<bool>,
    topic: Option<String>,
    score_adjustment: Option<i32>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BigLlmDecision {
    category: Option<String>,
    title_zh: Option<String>,
    summary_zh: Option<String>,
    topic: Option<String>,
    score_adjustment: Option<i32>,
    angle: Option<String>,
    opinion: Option<String>,
}

fn small_llm_system_prompt() -> &'static str {
    r#"You are the first-stage classifier for an AI news radar.

Classify each item using these strict definitions:

1. open_source_tool
- The item is about an AI-driven open-source tool or project.
- Usually the project is hosted on GitHub or similar open-source platforms.
- The core of the project should be AI-native or directly serve AI workflows, such as coding agents, model tooling, eval tooling, agent frameworks, or AI application infrastructure.
- Example mindset: projects like OpenClaw.
- Do NOT use this category for generic company news, closed-source product announcements, opinion pieces, or non-tool articles.

2. ai_news
- The item is about a major recent event in the AI industry or AI community, domestic or international.
- It must have strong timeliness and broad news value.
- A good test: this topic could be expanded into a few hundred Chinese words, and most AI-focused media outlets would likely cover it.
- Use this for major funding, regulation, strategic partnerships, major product moves, organizational changes, major incidents, or other widely reportable AI events.
- Do NOT use this for niche essays, routine blog posts, small product updates, or ordinary forum chatter.

3. model_eval
- The item is about newly released major models, major model upgrades, benchmark results, model capability comparisons, model API availability, or other important model-evaluation information.
- Example mindset: frontier model launches or important updates such as GPT-5.4.
- Use this when the main value is understanding model capability, release significance, or evaluation/comparison.

4. unknown
- Use unknown if the item is AI-related but does not clearly fit the three categories above.

Scoring guidance:
- score_adjustment should be positive when the item strongly matches one of the target categories and has clear importance.
- score_adjustment should be negative when the item is weak, niche, thin, speculative, or not clearly important.

Return strict JSON only with keys:
- is_ai_related: boolean
- topic: open_source_tool | ai_news | model_eval | unknown
- score_adjustment: integer from -20 to 20
- reason: short string explaining the classification basis"#
}

fn big_llm_system_prompt() -> &'static str {
    r#"You are a Chinese AI editor for an AI news radar.

You must rewrite and classify each item using these strict category definitions:

1. open_source_tool
- AI-driven open-source tools or projects.
- Usually distributed on GitHub or similar open-source platforms.
- The project itself should directly serve AI workflows such as agents, coding, model tooling, evaluation, orchestration, or AI application infrastructure.

2. ai_news
- Major recent events in the AI industry/community with strong timeliness.
- A good standard: the topic is important enough to be expanded into a few hundred Chinese words, and most AI media would likely report it.

3. model_eval
- Latest important model release information, major model upgrades, benchmark/evaluation results, capability comparisons, or API availability changes.
- Example mindset: important new-model information such as GPT-5.4.

Editorial requirements:
- title_zh should be factual, concise, and readable.
- summary_zh should be 2-4 Chinese sentences explaining what happened and why it matters.
- angle should be one Chinese sentence highlighting the most important observation.
- opinion should be one Chinese sentence with a clear but restrained viewpoint.
- Do not exaggerate. Do not fabricate facts not present in the input.

Return strict JSON only with keys:
- category: open_source_tool | ai_news | model_eval
- title_zh: string
- summary_zh: string
- angle: string
- opinion: string"#
}

fn strip_markdown_code_fence(raw: &str) -> String {
    let trimmed = raw.trim();
    if !trimmed.starts_with("```") {
        return trimmed.to_string();
    }

    let mut lines: Vec<&str> = trimmed.lines().collect();
    if lines.is_empty() {
        return String::new();
    }
    lines.remove(0);
    while lines
        .last()
        .map(|line| line.trim_start().starts_with("```"))
        .unwrap_or(false)
    {
        lines.pop();
    }
    lines.join("\n").trim().to_string()
}

fn parse_json_value(content: &str) -> Option<serde_json::Value> {
    let normalized = strip_markdown_code_fence(content);
    if normalized.trim().is_empty() {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(normalized.trim()).ok()
}

fn extract_text_from_chat_payload(payload: &serde_json::Value) -> Option<String> {
    let msg_content = payload
        .get("choices")?
        .as_array()?
        .first()?
        .get("message")?
        .get("content")?;

    if let Some(text) = msg_content.as_str() {
        return Some(text.to_string());
    }

    let mut parts = Vec::new();
    if let Some(arr) = msg_content.as_array() {
        for part in arr {
            if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
                parts.push(text.to_string());
                continue;
            }
            if let Some(text) = part
                .get("text")
                .and_then(|v| v.get("value"))
                .and_then(|v| v.as_str())
            {
                parts.push(text.to_string());
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn extract_text_from_responses_payload(payload: &serde_json::Value) -> Option<String> {
    if let Some(text) = payload.get("output_text").and_then(|v| v.as_str()) {
        return Some(text.to_string());
    }
    if let Some(arr) = payload.get("output_text").and_then(|v| v.as_array()) {
        let texts: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        if !texts.is_empty() {
            return Some(texts.join("\n"));
        }
    }

    let mut parts = Vec::new();
    if let Some(output_items) = payload.get("output").and_then(|v| v.as_array()) {
        for output in output_items {
            if let Some(content_items) = output.get("content").and_then(|v| v.as_array()) {
                for content in content_items {
                    if let Some(text) = content.get("text").and_then(|v| v.as_str()) {
                        parts.push(text.to_string());
                        continue;
                    }
                    if let Some(text) = content
                        .get("text")
                        .and_then(|v| v.get("value"))
                        .and_then(|v| v.as_str())
                    {
                        parts.push(text.to_string());
                    }
                }
            }
        }
    }

    if !parts.is_empty() {
        return Some(parts.join("\n"));
    }

    extract_text_from_chat_payload(payload)
}

impl SourceTierConfig {
    fn load(path: &str) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read tier file: {path}"))?;
        let cfg = serde_json::from_str::<SourceTierConfig>(&raw)
            .with_context(|| format!("failed to parse tier file as JSON: {path}"))?;
        Ok(cfg)
    }

    fn resolve_tier(&self, source_name: &str, source_kind: &str) -> String {
        let name_lc = source_name.to_lowercase();
        let kind_lc = source_kind.to_lowercase();

        for rule in &self.rules {
            if rule.tier.trim().is_empty() {
                continue;
            }
            let has_matcher = !rule.source_name_equals.is_empty()
                || !rule.source_name_contains.is_empty()
                || !rule.source_kind_equals.is_empty();
            if !has_matcher {
                continue;
            }

            let matched_equals = rule
                .source_name_equals
                .iter()
                .any(|name| name_lc == name.trim().to_lowercase());
            let matched_contains = rule
                .source_name_contains
                .iter()
                .any(|part| name_lc.contains(&part.trim().to_lowercase()));
            let matched_kind = rule
                .source_kind_equals
                .iter()
                .any(|kind| kind_lc == kind.trim().to_lowercase());

            if matched_equals || matched_contains || matched_kind {
                return rule.tier.trim().to_string();
            }
        }

        let fallback = self.default_tier.trim();
        if fallback.is_empty() {
            "P2".to_string()
        } else {
            fallback.to_string()
        }
    }

    fn min_score_for_tier(&self, tier: &str, fallback: i32) -> i32 {
        for (k, v) in &self.notify_min_score {
            if k.eq_ignore_ascii_case(tier) {
                return *v;
            }
        }
        fallback
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cfg = load_config()?;
    ensure_parent_dir(&cfg.sqlite_path)?;
    let tier_cfg = SourceTierConfig::load(&cfg.source_tier_config_path)?;

    let conn = Connection::open(&cfg.sqlite_path)
        .with_context(|| format!("failed to open sqlite at {}", cfg.sqlite_path))?;
    init_db(&conn)?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to build reqwest client")?;
    let mut candidates: Vec<Candidate> = Vec::new();

    let sources = discover_sources(&client, &cfg).await?;
    println!("[scheduler] source_count={}", sources.len());
    println!(
        "[scheduler] source_tier_config={}",
        cfg.source_tier_config_path
    );

    for round in 0..cfg.harvest_rounds {
        let mut round_new = 0usize;
        println!("[scheduler] round={}/{}", round + 1, cfg.harvest_rounds);
        for source in &sources {
            let source_tier = tier_cfg.resolve_tier(&source.name, &source.kind);
            let mut cursor = 0usize;
            loop {
                let resp = match fetch_shard_batch(&client, &cfg, source.id, cursor).await {
                    Ok(v) => v,
                    Err(e) => {
                        println!(
                            "[scheduler] source={} shard={} fetch_error={}",
                            source.name, source.id, e
                        );
                        break;
                    }
                };
                if let Some(err) = &resp.error {
                    println!(
                        "[scheduler] source={} shard={} source_error={}",
                        source.name, source.id, err
                    );
                }
                println!(
                    "[scheduler] source={} worker_source={} kind={} tier={} shard={} cursor={} got={} total={} done={}",
                    source.name,
                    resp.source_name,
                    source.kind,
                    source_tier,
                    resp.shard,
                    cursor,
                    resp.items.len(),
                    resp.total,
                    resp.done
                );

                for item in &resp.items {
                    if let Some(candidate) =
                        upsert_item_and_build_candidate(&conn, item, &source_tier)?
                    {
                        round_new += 1;
                        candidates.push(candidate);
                    }
                }

                if resp.done {
                    break;
                }
                cursor = resp.next_cursor.unwrap_or(cursor + cfg.worker_page_size);
            }
        }
        println!("[scheduler] round_new={}", round_new);
        if round_new == 0 {
            break;
        }
        if round + 1 < cfg.harvest_rounds && cfg.round_sleep_secs > 0 {
            sleep(Duration::from_secs(cfg.round_sleep_secs)).await;
        }
    }

    let mut shortlisted = Vec::new();

    if candidates.is_empty() {
        println!("[scheduler] no new candidates");
    } else {
        candidates = filter_candidates_by_small_llm(&client, &cfg, candidates).await?;
        if candidates.is_empty() {
            println!("[scheduler] no candidates after small-llm filtering");
        } else {
            let before_semantic_dedupe = candidates.len();
            candidates = dedupe_candidates_by_story(candidates);
            println!(
                "[scheduler] semantic_dedupe kept={} dropped={}",
                candidates.len(),
                before_semantic_dedupe.saturating_sub(candidates.len())
            );

            candidates = enrich_candidates_by_big_llm(&client, &cfg, candidates).await?;
            candidates.sort_by(|a, b| b.score.cmp(&a.score));

            shortlisted = candidates
                .into_iter()
                .filter(|c| {
                    c.score >= tier_cfg.min_score_for_tier(&c.tier, cfg.candidate_min_score)
                })
                .collect();

            println!(
                "[scheduler] shortlisted={} (tier-threshold, fallback >= {})",
                shortlisted.len(),
                cfg.candidate_min_score
            );
        }
    }

    let enqueued = enqueue_candidate_alerts(&conn, &cfg, shortlisted)?;
    println!("[scheduler] alert_enqueued={}", enqueued);

    let dispatched = dispatch_due_alerts(&conn, &client, &cfg).await?;
    println!("[scheduler] alert_dispatched={}", dispatched);
    Ok(())
}

fn load_config() -> Result<Config> {
    let worker_base_url = env::var("WORKER_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8787".to_string())
        .trim_end_matches('/')
        .to_string();
    let worker_shared_token =
        env::var("WORKER_SHARED_TOKEN").unwrap_or_else(|_| "change-me".to_string());
    let sqlite_path = env::var("SQLITE_PATH").unwrap_or_else(|_| "./data/news.db".to_string());
    let source_tier_config_path =
        env::var("SOURCE_TIER_CONFIG").unwrap_or_else(|_| "./config/source_tiers.json".to_string());
    let notify_webhook = env::var("NOTIFY_WEBHOOK").ok().and_then(|v| {
        let t = v.trim().to_string();
        if t.is_empty() {
            None
        } else {
            Some(t)
        }
    });
    let max_shards_fallback = env::var("MAX_SHARDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(6);
    let worker_page_size = env::var("WORKER_PAGE_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10);
    let harvest_rounds = env::var("HARVEST_ROUNDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1);
    let round_sleep_secs = env::var("ROUND_SLEEP_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(4);
    let candidate_min_score = env::var("CANDIDATE_MIN_SCORE")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(70);
    let alert_retry_max_attempts = env::var("ALERT_RETRY_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(6);
    let alert_retry_base_secs = env::var("ALERT_RETRY_BASE_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(120);
    let semantic_dedupe_lookback_hours = env::var("SEMANTIC_DEDUPE_LOOKBACK_HOURS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(72);

    let small_llm_api_base = env::var("SMALL_LLM_API_BASE")
        .ok()
        .map(|v| v.trim().to_string());
    let small_llm_api_key = env::var("SMALL_LLM_API_KEY")
        .ok()
        .map(|v| v.trim().to_string());
    let small_llm_model = env::var("SMALL_LLM_MODEL")
        .ok()
        .map(|v| v.trim().to_string());
    let small_llm_concurrency = env::var("SMALL_LLM_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);

    let legacy_llm_api_base = env::var("LLM_API_BASE").ok().map(|v| v.trim().to_string());
    let legacy_llm_api_key = env::var("LLM_API_KEY").ok().map(|v| v.trim().to_string());
    let legacy_llm_model = env::var("LLM_MODEL").ok().map(|v| v.trim().to_string());
    let legacy_llm_max_items = env::var("LLM_MAX_ITEMS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);

    let big_llm_api_base = env::var("BIG_LLM_API_BASE")
        .ok()
        .map(|v| v.trim().to_string())
        .or_else(|| legacy_llm_api_base.clone());
    let big_llm_api_key = env::var("BIG_LLM_API_KEY")
        .ok()
        .map(|v| v.trim().to_string())
        .or_else(|| legacy_llm_api_key.clone());
    let big_llm_model = env::var("BIG_LLM_MODEL")
        .ok()
        .map(|v| v.trim().to_string())
        .or_else(|| legacy_llm_model.clone());
    let big_llm_max_items = env::var("BIG_LLM_MAX_ITEMS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(legacy_llm_max_items);
    let big_llm_concurrency = env::var("BIG_LLM_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4);

    let small_llm = match (small_llm_api_base, small_llm_api_key, small_llm_model) {
        (Some(api_base), Some(api_key), Some(model))
            if !api_base.is_empty() && !api_key.is_empty() && !model.is_empty() =>
        {
            Some(SmallLlmConfig {
                api_base: api_base.trim_end_matches('/').to_string(),
                api_key,
                model,
                concurrency: small_llm_concurrency.max(1),
            })
        }
        _ => None,
    };

    let big_llm = match (big_llm_api_base, big_llm_api_key, big_llm_model) {
        (Some(api_base), Some(api_key), Some(model))
            if !api_base.is_empty() && !api_key.is_empty() && !model.is_empty() =>
        {
            Some(BigLlmConfig {
                api_base: api_base.trim_end_matches('/').to_string(),
                api_key,
                model,
                max_items: big_llm_max_items,
                concurrency: big_llm_concurrency.max(1),
            })
        }
        _ => None,
    };

    Ok(Config {
        worker_base_url,
        worker_shared_token,
        sqlite_path,
        source_tier_config_path,
        notify_webhook,
        max_shards_fallback,
        worker_page_size,
        harvest_rounds,
        round_sleep_secs,
        candidate_min_score,
        alert_retry_max_attempts,
        alert_retry_base_secs,
        semantic_dedupe_lookback_hours,
        small_llm,
        big_llm,
    })
}

fn ensure_parent_dir(path: &str) -> Result<()> {
    let p = Path::new(path);
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create dir {}", parent.display()))?;
        }
    }
    Ok(())
}

fn init_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
CREATE TABLE IF NOT EXISTS raw_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_hash TEXT NOT NULL UNIQUE,
  ext_id TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT NOT NULL,
  source TEXT NOT NULL,
  published_at TEXT NOT NULL,
  content TEXT NOT NULL,
  first_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_hash TEXT NOT NULL UNIQUE,
  topic TEXT NOT NULL,
  score INTEGER NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_hash TEXT NOT NULL UNIQUE,
  story_key TEXT,
  story_fingerprint TEXT NOT NULL DEFAULT '',
  published_day TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'sent',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_retry_at TEXT,
  last_error TEXT,
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL DEFAULT '',
  sent_at TEXT
);
"#,
    )?;
    migrate_alerts_table(conn)?;
    Ok(())
}

fn migrate_alerts_table(conn: &Connection) -> Result<()> {
    let columns = table_columns(conn, "alerts")?;
    let required_columns = [
        ("story_key", "TEXT"),
        ("story_fingerprint", "TEXT NOT NULL DEFAULT ''"),
        ("published_day", "TEXT NOT NULL DEFAULT ''"),
        ("status", "TEXT NOT NULL DEFAULT 'sent'"),
        ("attempt_count", "INTEGER NOT NULL DEFAULT 0"),
        ("next_retry_at", "TEXT"),
        ("last_error", "TEXT"),
        ("payload_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ];

    for (name, ddl) in required_columns {
        if !columns.contains(name) {
            conn.execute(&format!("ALTER TABLE alerts ADD COLUMN {name} {ddl}"), [])?;
        }
    }

    let now = Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE alerts
         SET status = COALESCE(NULLIF(status, ''), 'sent'),
             attempt_count = COALESCE(attempt_count, 0),
             payload_json = COALESCE(NULLIF(payload_json, ''), '{}'),
             created_at = COALESCE(NULLIF(created_at, ''), sent_at, ?1),
             story_fingerprint = COALESCE(story_fingerprint, ''),
             published_day = COALESCE(published_day, '')",
        [now.as_str()],
    )?;

    backfill_alert_story_fields(conn)?;

    conn.execute_batch(
        r#"
CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_story_key ON alerts(story_key);
CREATE INDEX IF NOT EXISTS idx_alerts_status_next_retry ON alerts(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_alerts_story_fingerprint_sent_at ON alerts(story_fingerprint, sent_at);
"#,
    )?;
    Ok(())
}

fn table_columns(conn: &Connection, table_name: &str) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table_name})"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = HashSet::new();
    for row in rows {
        columns.insert(row?);
    }
    Ok(columns)
}

fn backfill_alert_story_fields(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT id, url_hash, COALESCE(story_key, ''), COALESCE(story_fingerprint, ''),
                COALESCE(published_day, ''), sent_at
         FROM alerts",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;

    for row in rows {
        let (
            id,
            url_hash,
            existing_story_key,
            existing_story_fingerprint,
            existing_published_day,
            sent_at,
        ) = row?;
        let (raw_title, raw_published_at) = load_raw_story_seed(conn, &url_hash)?;
        let story_fingerprint = if existing_story_fingerprint.trim().is_empty() {
            if let Some(title) = raw_title.as_deref() {
                build_story_fingerprint(title, "")
            } else {
                url_hash.clone()
            }
        } else {
            existing_story_fingerprint
        };
        let published_day = if existing_published_day.trim().is_empty() {
            extract_published_day(raw_published_at.as_deref().or(sent_at.as_deref()))
        } else {
            existing_published_day
        };
        let mut story_key = if existing_story_key.trim().is_empty() {
            build_story_key(&story_fingerprint, &published_day)
        } else {
            existing_story_key
        };
        if alert_story_key_exists(conn, id, &story_key)? {
            story_key = format!("legacy:{}", url_hash);
        }

        conn.execute(
            "UPDATE alerts
             SET story_key = ?1,
                 story_fingerprint = ?2,
                 published_day = ?3,
                 created_at = COALESCE(NULLIF(created_at, ''), sent_at, ?4),
                 payload_json = COALESCE(NULLIF(payload_json, ''), '{}'),
                 status = COALESCE(NULLIF(status, ''), 'sent')
             WHERE id = ?5",
            params![
                story_key,
                story_fingerprint,
                published_day,
                Utc::now().to_rfc3339(),
                id
            ],
        )?;
    }
    Ok(())
}

fn load_raw_story_seed(
    conn: &Connection,
    url_hash: &str,
) -> Result<(Option<String>, Option<String>)> {
    let mut stmt = conn.prepare(
        "SELECT title, published_at FROM raw_items WHERE url_hash = ?1 ORDER BY id DESC LIMIT 1",
    )?;
    let row = stmt.query_row([url_hash], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    });
    match row {
        Ok((title, published_at)) => Ok((Some(title), Some(published_at))),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok((None, None)),
        Err(err) => Err(err.into()),
    }
}

fn alert_story_key_exists(conn: &Connection, current_id: i64, story_key: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(1) FROM alerts WHERE story_key = ?1 AND id != ?2",
        params![story_key, current_id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

async fn discover_sources(client: &reqwest::Client, cfg: &Config) -> Result<Vec<SourceMeta>> {
    let url = format!("{}/sources", cfg.worker_base_url);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-shared-token",
        HeaderValue::from_str(&cfg.worker_shared_token).context("invalid token header")?,
    );
    let resp = client.get(url).headers(headers).send().await;
    if let Ok(ok_resp) = resp {
        if let Ok(ok_resp) = ok_resp.error_for_status() {
            let payload = ok_resp.json::<SourcesResponse>().await;
            if let Ok(payload) = payload {
                if !payload.sources.is_empty() {
                    return Ok(payload.sources);
                }
            }
        }
    }
    let mut fallback = Vec::new();
    for id in 0..cfg.max_shards_fallback {
        fallback.push(SourceMeta {
            id,
            name: format!("fallback-shard-{id}"),
            kind: "unknown".to_string(),
        });
    }
    Ok(fallback)
}

async fn fetch_shard_batch(
    client: &reqwest::Client,
    cfg: &Config,
    shard: usize,
    cursor: usize,
) -> Result<FetchResponse> {
    let url = format!(
        "{}/fetch?shard={}&cursor={}&limit={}",
        cfg.worker_base_url, shard, cursor, cfg.worker_page_size
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-shared-token",
        HeaderValue::from_str(&cfg.worker_shared_token).context("invalid token header")?,
    );
    let resp = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .context("failed to call worker")?
        .error_for_status()
        .context("worker returned error status")?;
    let payload = resp
        .json::<FetchResponse>()
        .await
        .context("failed to parse worker response")?;
    Ok(payload)
}

fn upsert_item_and_build_candidate(
    conn: &Connection,
    item: &RawItem,
    source_tier: &str,
) -> Result<Option<Candidate>> {
    let url_hash = hash_url(&item.url);
    let now = Utc::now().to_rfc3339();
    let story_fingerprint = build_story_fingerprint(&item.title, &item.content);
    let published_day = extract_published_day(Some(&item.published_at));
    let story_key = build_story_key(&story_fingerprint, &published_day);

    let inserted = conn.execute(
        "INSERT OR IGNORE INTO raw_items (url_hash, ext_id, title, url, source, published_at, content, first_seen_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            url_hash,
            item.id,
            item.title,
            item.url,
            item.source,
            item.published_at,
            item.content,
            now
        ],
    )?;

    if inserted == 0 {
        return Ok(None);
    }

    let (topic, score, reason) = classify_and_score(item);
    conn.execute(
        "INSERT OR REPLACE INTO events (url_hash, topic, score, reason, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![url_hash, topic, score, reason, now],
    )?;

    Ok(Some(Candidate {
        url_hash,
        story_key,
        story_fingerprint,
        published_day,
        tier: source_tier.to_string(),
        topic,
        score,
        title: item.title.clone(),
        url: item.url.clone(),
        source: item.source.clone(),
        content: item.content.clone(),
        published_at: item.published_at.clone(),
        reason,
        summary: None,
        angle: None,
        opinion: None,
    }))
}

fn dedupe_candidates_by_story(candidates: Vec<Candidate>) -> Vec<Candidate> {
    let mut grouped: HashMap<String, Candidate> = HashMap::new();
    for candidate in candidates {
        match grouped.get_mut(&candidate.story_key) {
            Some(existing) => {
                let candidate_should_replace = candidate.score > existing.score
                    || (candidate.score == existing.score
                        && candidate.content.len() > existing.content.len());
                let kept_title = if candidate_should_replace {
                    candidate.title.clone()
                } else {
                    existing.title.clone()
                };
                let dropped_title = if candidate_should_replace {
                    existing.title.clone()
                } else {
                    candidate.title.clone()
                };
                let kept_score = if candidate_should_replace {
                    candidate.score
                } else {
                    existing.score
                };
                let dropped_score = if candidate_should_replace {
                    existing.score
                } else {
                    candidate.score
                };
                println!(
                    "[scheduler] semantic_dedupe_hit stage=in_batch matched_by=same_story_key story_key={} fingerprint={} kept_score={} dropped_score={} kept_title={} dropped_title={}",
                    candidate.story_key,
                    truncate_chars(&candidate.story_fingerprint, 80),
                    kept_score,
                    dropped_score,
                    truncate_chars(&kept_title, 80),
                    truncate_chars(&dropped_title, 80)
                );
                if candidate_should_replace {
                    *existing = candidate;
                }
            }
            None => {
                grouped.insert(candidate.story_key.clone(), candidate);
            }
        }
    }
    grouped.into_values().collect()
}

fn build_story_fingerprint(title: &str, content: &str) -> String {
    let mut tokens = extract_significant_tokens(title);
    if tokens.len() < 4 {
        tokens.extend(extract_significant_tokens(&truncate_chars(content, 240)));
    }
    tokens.sort();
    tokens.dedup();
    tokens.sort_by(|a, b| b.len().cmp(&a.len()).then(a.cmp(b)));
    let mut selected: Vec<String> = tokens.into_iter().take(8).collect();
    selected.sort();
    if selected.is_empty() {
        return normalize_free_text(title);
    }
    selected.join(" ")
}

fn build_story_key(story_fingerprint: &str, published_day: &str) -> String {
    hash_text(&format!(
        "{}|{}",
        story_fingerprint.trim(),
        published_day.trim()
    ))
}

fn extract_published_day(published_at: Option<&str>) -> String {
    let Some(published_at) = published_at.map(str::trim).filter(|text| !text.is_empty()) else {
        return Utc::now().format("%Y-%m-%d").to_string();
    };
    if let Ok(parsed) = DateTime::parse_from_rfc3339(published_at) {
        return parsed.format("%Y-%m-%d").to_string();
    }
    published_at.chars().take(10).collect::<String>()
}

fn extract_significant_tokens(text: &str) -> Vec<String> {
    let normalized = normalize_free_text(text);
    normalized
        .split_whitespace()
        .filter(|token| is_significant_token(token))
        .map(normalize_token)
        .filter(|token| is_significant_token(token))
        .collect()
}

fn normalize_free_text(text: &str) -> String {
    let mut normalized = String::new();
    for ch in text.to_lowercase().chars() {
        if ch.is_alphanumeric() || is_cjk(ch) {
            normalized.push(ch);
        } else {
            normalized.push(' ');
        }
    }
    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_significant_token(token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    let stop_words = [
        "the", "a", "an", "to", "for", "of", "and", "or", "on", "in", "by", "with", "from", "at",
        "into", "is", "are", "be", "official", "blog", "news", "update",
    ];
    if stop_words.contains(&token) {
        return false;
    }
    token.chars().count() >= 2 || token.chars().all(|ch| ch.is_ascii_digit())
}

fn normalize_token(token: &str) -> String {
    if !token.is_ascii() {
        return token.to_string();
    }
    let mut value = token.to_string();
    for suffix in ["ing", "ed", "es", "s"] {
        if value.len() > suffix.len() + 3 && value.ends_with(suffix) {
            value.truncate(value.len() - suffix.len());
            break;
        }
    }
    value
}

fn is_cjk(ch: char) -> bool {
    matches!(ch as u32,
        0x4E00..=0x9FFF |
        0x3400..=0x4DBF |
        0x20000..=0x2A6DF |
        0x2A700..=0x2B73F |
        0x2B740..=0x2B81F |
        0x2B820..=0x2CEAF |
        0xF900..=0xFAFF |
        0x2F800..=0x2FA1F
    )
}

fn classify_and_score(item: &RawItem) -> (String, i32, String) {
    let text = format!(
        "{} {}",
        item.title.to_lowercase(),
        item.content.to_lowercase()
    );

    let mut score = 20;
    let topic = if contains_any(
        &text,
        &[
            "benchmark",
            "eval",
            "test",
            "lite",
            "gemini",
            "gpt",
            "claude",
        ],
    ) {
        score += 30;
        "model_eval"
    } else if contains_any(
        &text,
        &[
            "open source",
            "opensource",
            "github",
            "deploy",
            "tool",
            "cli",
            "agent",
        ],
    ) {
        score += 30;
        "open_source_tool"
    } else {
        score += 20;
        "ai_news"
    };

    if contains_any(
        &text,
        &["release", "launch", "announce", "update", "new", "breaking"],
    ) {
        score += 20;
    }

    if contains_any(
        &item.source.to_lowercase(),
        &[
            "openai",
            "anthropic",
            "google",
            "github",
            "huggingface",
            "arxiv",
        ],
    ) {
        score += 10;
    }

    if is_recent_48h(&item.published_at) {
        score += 20;
    }

    (
        topic.to_string(),
        score.min(100),
        "rule-score-hit".to_string(),
    )
}

fn contains_any(text: &str, words: &[&str]) -> bool {
    words.iter().any(|w| text.contains(w))
}

fn is_recent_48h(published_at: &str) -> bool {
    let parsed = DateTime::parse_from_rfc3339(published_at);
    if let Ok(dt) = parsed {
        let hours = (Utc::now() - dt.with_timezone(&Utc)).num_hours();
        return (0..=48).contains(&hours);
    }
    false
}

fn normalize_topic(topic: &str) -> Option<String> {
    let t = topic.trim().to_lowercase();
    match t.as_str() {
        "open_source_tool" | "ai_news" | "model_eval" => Some(t),
        _ => None,
    }
}

fn topic_label_zh(topic: &str) -> &'static str {
    match topic {
        "open_source_tool" => "\u{5f00}\u{6e90}\u{5de5}\u{5177}",
        "model_eval" => "\u{6a21}\u{578b}\u{6d4b}\u{8bc4}",
        _ => "AI\u{8d44}\u{8baf}",
    }
}

fn is_ai_related_fallback(candidate: &Candidate) -> bool {
    let text = format!(
        "{} {} {}",
        candidate.title.to_lowercase(),
        candidate.source.to_lowercase(),
        candidate.content.to_lowercase()
    );
    contains_any(
        &text,
        &[
            "ai",
            "llm",
            "gpt",
            "gemini",
            "claude",
            "agent",
            "model",
            "openai",
            "anthropic",
            "deepseek",
            "qwen",
            "kimi",
            "doubao",
            "wenxin",
        ],
    )
}

async fn filter_candidates_by_small_llm(
    client: &reqwest::Client,
    cfg: &Config,
    candidates: Vec<Candidate>,
) -> Result<Vec<Candidate>> {
    let Some(llm) = &cfg.small_llm else {
        bail!("small-llm is required, set SMALL_LLM_API_BASE/KEY/MODEL");
    };

    let sem = Arc::new(Semaphore::new(llm.concurrency.max(1)));
    let mut set: JoinSet<(Candidate, Option<SmallLlmDecision>)> = JoinSet::new();

    for candidate in candidates {
        let client = client.clone();
        let llm_cfg = llm.clone();
        let sem = Arc::clone(&sem);
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.ok();
            let decision = call_small_llm_for_candidate(&client, &llm_cfg, &candidate)
                .await
                .ok()
                .flatten();
            (candidate, decision)
        });
    }

    let mut kept = Vec::new();
    let mut dropped = 0usize;
    let mut fallback_kept = 0usize;

    while let Some(joined) = set.join_next().await {
        let Ok((mut candidate, decision)) = joined else {
            continue;
        };

        match decision {
            Some(d) => {
                let mut keep = d
                    .is_ai_related
                    .unwrap_or_else(|| is_ai_related_fallback(&candidate));
                if !keep && is_ai_related_fallback(&candidate) {
                    keep = true;
                    fallback_kept += 1;
                }
                if !keep {
                    dropped += 1;
                    continue;
                }
                if let Some(topic) = d.topic.and_then(|t| normalize_topic(&t)) {
                    candidate.topic = topic;
                }
                if let Some(adjust) = d.score_adjustment {
                    candidate.score = (candidate.score + adjust).clamp(0, 100);
                }
                if let Some(reason) = d.reason {
                    let reason = reason.trim();
                    if !reason.is_empty() {
                        candidate.reason = format!("small-llm:{reason}");
                    }
                }
                kept.push(candidate);
            }
            None => {
                if is_ai_related_fallback(&candidate) {
                    fallback_kept += 1;
                    candidate.reason = "small-llm:fallback-rule".to_string();
                    kept.push(candidate);
                } else {
                    dropped += 1;
                }
            }
        }
    }

    println!(
        "[scheduler] small-llm filtered: kept={} dropped={} fallback_kept={}",
        kept.len(),
        dropped,
        fallback_kept
    );

    Ok(kept)
}

async fn enrich_candidates_by_big_llm(
    client: &reqwest::Client,
    cfg: &Config,
    mut candidates: Vec<Candidate>,
) -> Result<Vec<Candidate>> {
    let Some(llm) = &cfg.big_llm else {
        bail!("big-llm is required, set BIG_LLM_API_BASE/KEY/MODEL");
    };

    candidates.sort_by(|a, b| b.score.cmp(&a.score));
    let limit = if llm.max_items == 0 {
        candidates.len()
    } else {
        llm.max_items.min(candidates.len())
    };

    let sem = Arc::new(Semaphore::new(llm.concurrency.max(1)));
    let mut set: JoinSet<(usize, Candidate, Option<BigLlmDecision>)> = JoinSet::new();

    for (idx, item) in candidates.iter().take(limit).enumerate() {
        let client = client.clone();
        let llm_cfg = llm.clone();
        let sem = Arc::clone(&sem);
        let candidate = item.clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.ok();
            let decision = call_big_llm_for_candidate(&client, &llm_cfg, &candidate)
                .await
                .ok()
                .flatten();
            (idx, candidate, decision)
        });
    }

    while let Some(joined) = set.join_next().await {
        let Ok((idx, mut candidate, decision)) = joined else {
            continue;
        };
        if let Some(d) = decision {
            if let Some(topic) = d
                .category
                .as_deref()
                .and_then(normalize_topic)
                .or_else(|| d.topic.as_deref().and_then(normalize_topic))
            {
                candidate.topic = topic;
            }
            if let Some(adjust) = d.score_adjustment {
                candidate.score = (candidate.score + adjust).clamp(0, 100);
            }
            if let Some(title_zh) = d.title_zh {
                let text = title_zh.trim();
                if !text.is_empty() {
                    candidate.title = truncate_chars(text, 120);
                }
            }
            if let Some(summary_zh) = d.summary_zh {
                let text = summary_zh.trim();
                if !text.is_empty() {
                    candidate.summary = Some(truncate_chars(text, 600));
                }
            }
            if let Some(angle) = d.angle {
                let text = angle.trim();
                if !text.is_empty() {
                    candidate.angle = Some(truncate_chars(text, 200));
                }
            }
            if let Some(opinion) = d.opinion {
                let text = opinion.trim();
                if !text.is_empty() {
                    candidate.opinion = Some(truncate_chars(text, 200));
                }
            }
            candidate.reason = format!("{}+big-llm", candidate.reason);
        }
        candidates[idx] = candidate;
    }

    println!(
        "[scheduler] big-llm enriched: processed={} total={}",
        limit,
        candidates.len()
    );
    Ok(candidates)
}

async fn call_small_llm_for_candidate(
    client: &reqwest::Client,
    llm: &SmallLlmConfig,
    candidate: &Candidate,
) -> Result<Option<SmallLlmDecision>> {
    let user_prompt = format!(
        "title: {}\nsource: {}\nurl: {}\npublished_at: {}\nraw_content: {}\ncurrent_topic: {}\ncurrent_score: {}\n",
        candidate.title,
        candidate.source,
        candidate.url,
        candidate.published_at,
        truncate_chars(&candidate.content, 1200),
        candidate.topic,
        candidate.score
    );
    let Some(payload) = call_llm_json(
        client,
        &llm.api_base,
        &llm.api_key,
        &llm.model,
        small_llm_system_prompt(),
        &user_prompt,
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(serde_json::from_value::<SmallLlmDecision>(payload).ok())
}

async fn call_big_llm_for_candidate(
    client: &reqwest::Client,
    llm: &BigLlmConfig,
    candidate: &Candidate,
) -> Result<Option<BigLlmDecision>> {
    let user_prompt = format!(
        "Please rewrite this item into readable Chinese and classify it.\n\ntitle: {}\nsource: {}\nurl: {}\npublished_at: {}\nraw_content: {}\ncurrent_topic: {}\ncurrent_score: {}\n",
        candidate.title,
        candidate.source,
        candidate.url,
        candidate.published_at,
        truncate_chars(&candidate.content, 1500),
        candidate.topic,
        candidate.score
    );
    let Some(payload) = call_llm_json(
        client,
        &llm.api_base,
        &llm.api_key,
        &llm.model,
        big_llm_system_prompt(),
        &user_prompt,
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(serde_json::from_value::<BigLlmDecision>(payload).ok())
}

async fn call_llm_json(
    client: &reqwest::Client,
    api_base: &str,
    api_key: &str,
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<Option<serde_json::Value>> {
    if let Some(payload) =
        call_llm_json_via_responses(client, api_base, api_key, model, system_prompt, user_prompt)
            .await?
    {
        return Ok(Some(payload));
    }
    call_llm_json_via_chat(client, api_base, api_key, model, system_prompt, user_prompt).await
}

async fn call_llm_json_via_responses(
    client: &reqwest::Client,
    api_base: &str,
    api_key: &str,
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<Option<serde_json::Value>> {
    let url = format!("{}/responses", api_base);
    let body = serde_json::json!({
      "model": model,
      "temperature": 0.2,
      "instructions": system_prompt,
      "input": user_prompt,
      "text": { "format": { "type": "json_object" } }
    });
    let resp = client
        .post(url)
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await;
    let Ok(resp) = resp else {
        return Ok(None);
    };
    let resp = match resp.error_for_status() {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let payload = match resp.json::<serde_json::Value>().await {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let content = extract_text_from_responses_payload(&payload);
    let Some(content) = content else {
        return Ok(None);
    };
    Ok(parse_json_value(&content))
}

async fn call_llm_json_via_chat(
    client: &reqwest::Client,
    api_base: &str,
    api_key: &str,
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<Option<serde_json::Value>> {
    let url = format!("{}/chat/completions", api_base);
    let body = serde_json::json!({
      "model": model,
      "temperature": 0.2,
      "response_format": { "type": "json_object" },
      "messages": [
        {
          "role": "system",
          "content": system_prompt
        },
        {
          "role": "user",
          "content": user_prompt
        }
      ]
    });
    let resp = client
        .post(url)
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await;
    let Ok(resp) = resp else {
        return Ok(None);
    };
    let resp = match resp.error_for_status() {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let payload = match resp.json::<serde_json::Value>().await {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let content = extract_text_from_chat_payload(&payload);
    let Some(content) = content else {
        return Ok(None);
    };
    Ok(parse_json_value(&content))
}

fn enqueue_candidate_alerts(
    conn: &Connection,
    cfg: &Config,
    candidates: Vec<Candidate>,
) -> Result<usize> {
    let mut enqueued = 0usize;
    let now = Utc::now().to_rfc3339();
    for item in candidates {
        if let Some(hit) = already_alerted_semantically(conn, cfg, &item)? {
            println!(
                "[scheduler] semantic_skip stage=history matched_by={} alert_id={} current_story_key={} matched_story_key={} sent_at={} current_title={} matched_title={}",
                hit.matched_by,
                hit.alert_id,
                item.story_key,
                hit.matched_story_key,
                hit.matched_sent_at.unwrap_or_else(|| "-".to_string()),
                truncate_chars(&item.title, 80),
                truncate_chars(&hit.matched_title.unwrap_or_else(|| "-".to_string()), 80)
            );
            continue;
        }

        let payload_json = serde_json::to_string(&item)?;
        if let Some(existing) = get_alert_summary_by_story_key(conn, &item.story_key)? {
            if existing.status.eq_ignore_ascii_case("sent") {
                continue;
            }
            conn.execute(
                "UPDATE alerts
                 SET url_hash = ?1,
                     story_fingerprint = ?2,
                     published_day = ?3,
                     payload_json = ?4,
                     status = 'pending',
                     next_retry_at = ?5,
                     last_error = NULL
                 WHERE id = ?6",
                params![
                    item.url_hash,
                    item.story_fingerprint,
                    item.published_day,
                    payload_json,
                    now,
                    existing.id
                ],
            )?;
            enqueued += 1;
            continue;
        }

        conn.execute(
            "INSERT INTO alerts (
                url_hash, story_key, story_fingerprint, published_day, status,
                attempt_count, next_retry_at, last_error, payload_json, created_at, sent_at
             ) VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, NULL, ?6, ?7, NULL)",
            params![
                item.url_hash,
                item.story_key,
                item.story_fingerprint,
                item.published_day,
                now,
                payload_json,
                now
            ],
        )?;
        enqueued += 1;
    }
    Ok(enqueued)
}

fn already_alerted_semantically(
    conn: &Connection,
    cfg: &Config,
    item: &Candidate,
) -> Result<Option<SemanticAlertHit>> {
    let threshold =
        (Utc::now() - ChronoDuration::hours(cfg.semantic_dedupe_lookback_hours)).to_rfc3339();
    let mut stmt = conn.prepare(
        "SELECT id, story_key, sent_at, payload_json,
                CASE WHEN story_key = ?1 THEN 'story_key' ELSE 'story_fingerprint' END AS matched_by
         FROM alerts
         WHERE status = 'sent'
           AND (
             story_key = ?1
             OR (
               story_fingerprint = ?2
               AND sent_at IS NOT NULL
                AND sent_at >= ?3
              )
            )
         ORDER BY CASE WHEN story_key = ?1 THEN 0 ELSE 1 END, sent_at DESC, id DESC
         LIMIT 1",
    )?;
    let row = stmt.query_row(
        params![item.story_key, item.story_fingerprint, threshold],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        },
    );
    match row {
        Ok((alert_id, matched_story_key, matched_sent_at, payload_json, matched_by)) => {
            let matched_title = serde_json::from_str::<Candidate>(&payload_json)
                .ok()
                .map(|candidate| candidate.title);
            Ok(Some(SemanticAlertHit {
                alert_id,
                matched_by: if matched_by == "story_key" {
                    "story_key"
                } else {
                    "story_fingerprint"
                },
                matched_story_key,
                matched_sent_at,
                matched_title,
            }))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn get_alert_summary_by_story_key(
    conn: &Connection,
    story_key: &str,
) -> Result<Option<AlertSummary>> {
    let mut stmt = conn
        .prepare("SELECT id, status FROM alerts WHERE story_key = ?1 ORDER BY id DESC LIMIT 1")?;
    let row = stmt.query_row([story_key], |row| {
        Ok(AlertSummary {
            id: row.get(0)?,
            status: row.get(1)?,
        })
    });
    match row {
        Ok(value) => Ok(Some(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn dispatch_due_alerts(
    conn: &Connection,
    client: &reqwest::Client,
    cfg: &Config,
) -> Result<usize> {
    let now = Utc::now().to_rfc3339();
    let mut stmt = conn.prepare(
        "SELECT id, story_key, attempt_count, payload_json
         FROM alerts
         WHERE status != 'sent'
           AND attempt_count < ?1
           AND (next_retry_at IS NULL OR next_retry_at <= ?2)
         ORDER BY created_at ASC, id ASC",
    )?;
    let jobs = stmt.query_map(params![cfg.alert_retry_max_attempts as i64, now], |row| {
        Ok(AlertJob {
            id: row.get(0)?,
            story_key: row.get(1)?,
            attempt_count: row.get(2)?,
            payload_json: row.get(3)?,
        })
    })?;

    let mut dispatched = 0usize;
    for job in jobs {
        let job = job?;
        let item = serde_json::from_str::<Candidate>(&job.payload_json).with_context(|| {
            format!(
                "failed to deserialize alert payload story_key={}",
                job.story_key
            )
        })?;
        let send_result = send_candidate_alert(client, cfg, &item).await;
        match send_result {
            Ok(()) => {
                conn.execute(
                    "UPDATE alerts
                     SET status = 'sent',
                         sent_at = ?1,
                         next_retry_at = NULL,
                         last_error = NULL
                     WHERE id = ?2",
                    params![Utc::now().to_rfc3339(), job.id],
                )?;
                dispatched += 1;
            }
            Err(err) => {
                let next_attempt_count = job.attempt_count + 1;
                let next_retry_at = if next_attempt_count >= cfg.alert_retry_max_attempts as i32 {
                    None
                } else {
                    Some(
                        (Utc::now()
                            + ChronoDuration::seconds(compute_retry_delay_secs(
                                cfg,
                                next_attempt_count,
                            ) as i64))
                        .to_rfc3339(),
                    )
                };
                conn.execute(
                    "UPDATE alerts
                     SET status = 'failed',
                         attempt_count = ?1,
                         next_retry_at = ?2,
                         last_error = ?3
                     WHERE id = ?4",
                    params![
                        next_attempt_count,
                        next_retry_at,
                        truncate_chars(&err.to_string(), 500),
                        job.id
                    ],
                )?;
                println!(
                    "[scheduler] alert_retry_failed story_key={} attempt={} error={}",
                    job.story_key, next_attempt_count, err
                );
            }
        }
    }
    Ok(dispatched)
}

fn compute_retry_delay_secs(cfg: &Config, attempt_count: i32) -> u64 {
    let shift = (attempt_count.max(0) as u32).min(6);
    cfg.alert_retry_base_secs.saturating_mul(1u64 << shift)
}

async fn send_candidate_alert(
    client: &reqwest::Client,
    cfg: &Config,
    item: &Candidate,
) -> Result<()> {
    let msg = build_candidate_message(item);
    println!("{msg}");
    if let Some(webhook) = &cfg.notify_webhook {
        let payload = build_webhook_payload(webhook, item, &msg);
        let resp = client
            .post(webhook)
            .json(&payload)
            .send()
            .await
            .context("failed to send webhook request")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "webhook returned status={} body={}",
                status,
                truncate_chars(&body, 200)
            );
        }
    }
    Ok(())
}

fn build_candidate_message(item: &Candidate) -> String {
    let topic_zh = topic_label_zh(&item.topic);
    let summary = item.summary.clone().unwrap_or_else(|| "-".to_string());
    format!(
        "[{}][{}][score={}]\n{}\nsource={}\npublished_at={}\nurl={}\nsummary={}\nreason={}\nangle={}\nopinion={}",
        item.tier,
        topic_zh,
        item.score,
        item.title,
        item.source,
        item.published_at,
        item.url,
        summary,
        item.reason,
        item.angle.clone().unwrap_or_else(|| "-".to_string()),
        item.opinion.clone().unwrap_or_else(|| "-".to_string())
    )
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn feishu_header_color(score: i32) -> &'static str {
    if score >= 90 {
        "red"
    } else if score >= 80 {
        "orange"
    } else if score >= 70 {
        "yellow"
    } else {
        "blue"
    }
}

fn build_webhook_payload(webhook: &str, item: &Candidate, fallback_msg: &str) -> serde_json::Value {
    if webhook.contains("open.feishu.cn") {
        let angle = item.angle.clone().unwrap_or_else(|| "-".to_string());
        let opinion = item.opinion.clone().unwrap_or_else(|| "-".to_string());
        let summary = item.summary.clone().unwrap_or_else(|| "-".to_string());
        let title = truncate_chars(&item.title, 90);
        let reason = truncate_chars(&item.reason, 120);
        let topic_zh = topic_label_zh(&item.topic);
        serde_json::json!({
          "msg_type": "interactive",
          "card": {
            "config": {
              "wide_screen_mode": true,
              "enable_forward": true
            },
            "header": {
              "template": feishu_header_color(item.score),
              "title": {
                "tag": "plain_text",
                "content": format!("[{}][{}][{}] {}", item.tier, topic_zh, item.score, title)
              }
            },
            "elements": [
              {
                "tag": "div",
                "fields": [
                  {
                    "is_short": true,
                    "text": {
                      "tag": "lark_md",
                      "content": format!("**Source**\\n{}", truncate_chars(&item.source, 60))
                    }
                  },
                  {
                    "is_short": true,
                    "text": {
                      "tag": "lark_md",
                      "content": format!("**Published At**\\n{}", truncate_chars(&item.published_at, 32))
                    }
                  },
                  {
                    "is_short": true,
                    "text": {
                      "tag": "lark_md",
                      "content": format!("**Reason**\\n{}", reason)
                    }
                  },
                  {
                    "is_short": true,
                    "text": {
                      "tag": "lark_md",
                      "content": format!("**Score**\\n{}", item.score)
                    }
                  }
                ]
              },
              {
                "tag": "div",
                "text": {
                  "tag": "lark_md",
                  "content": format!("**Summary**\\n{}", truncate_chars(&summary, 600))
                }
              },
              {
                "tag": "div",
                "text": {
                  "tag": "lark_md",
                  "content": format!("**Angle**\\n{}", truncate_chars(&angle, 200))
                }
              },
              {
                "tag": "div",
                "text": {
                  "tag": "lark_md",
                  "content": format!("**Opinion**\\n{}", truncate_chars(&opinion, 200))
                }
              },
              {
                "tag": "action",
                "actions": [
                  {
                    "tag": "button",
                    "type": "primary",
                    "text": {
                      "tag": "plain_text",
                      "content": "Open URL"
                    },
                    "url": item.url
                  }
                ]
              }
            ]
          }
        })
    } else {
        serde_json::json!({ "text": fallback_msg })
    }
}

fn hash_text(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    hex::encode(hasher.finalize())
}

fn hash_url(url: &str) -> String {
    hash_text(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            worker_base_url: "http://127.0.0.1:8787".to_string(),
            worker_shared_token: "change-me".to_string(),
            sqlite_path: ":memory:".to_string(),
            source_tier_config_path: "./config/source_tiers.json".to_string(),
            notify_webhook: None,
            max_shards_fallback: 6,
            worker_page_size: 10,
            harvest_rounds: 1,
            round_sleep_secs: 1,
            candidate_min_score: 70,
            alert_retry_max_attempts: 6,
            alert_retry_base_secs: 120,
            semantic_dedupe_lookback_hours: 72,
            small_llm: None,
            big_llm: None,
        }
    }

    fn test_candidate(title: &str, url: &str) -> Candidate {
        let story_fingerprint =
            build_story_fingerprint(title, "OpenAI ships GPT-5.4 API for developers.");
        let published_day = "2026-03-06".to_string();
        Candidate {
            url_hash: hash_url(url),
            story_key: build_story_key(&story_fingerprint, &published_day),
            story_fingerprint,
            published_day,
            tier: "P1".to_string(),
            topic: "ai_news".to_string(),
            score: 88,
            title: title.to_string(),
            url: url.to_string(),
            source: "openai-news".to_string(),
            content: "OpenAI ships GPT-5.4 API for developers.".to_string(),
            published_at: "2026-03-06T10:00:00Z".to_string(),
            reason: "rule:launch".to_string(),
            summary: None,
            angle: None,
            opinion: None,
        }
    }

    #[test]
    fn story_fingerprint_handles_reordered_titles() {
        let left =
            build_story_fingerprint("OpenAI releases GPT-5.4 API for enterprise developers", "");
        let right = build_story_fingerprint(
            "GPT-5.4 API released for enterprise developers by OpenAI",
            "",
        );
        assert_eq!(left, right);
    }

    #[test]
    fn dedupe_candidates_keeps_single_story() {
        let first = test_candidate(
            "OpenAI releases GPT-5.4 API for enterprise developers",
            "https://example.com/a",
        );
        let mut second = test_candidate(
            "GPT-5.4 API released for enterprise developers by OpenAI",
            "https://example.com/b",
        );
        second.score = 92;

        let deduped = dedupe_candidates_by_story(vec![first, second]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].score, 92);
    }

    #[test]
    fn enqueue_respects_recent_semantic_alert() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        init_db(&conn)?;
        let cfg = test_config();
        let candidate = test_candidate(
            "OpenAI releases GPT-5.4 API for enterprise developers",
            "https://example.com/a",
        );
        let payload_json = serde_json::to_string(&candidate)?;
        conn.execute(
            "INSERT INTO alerts (
                url_hash, story_key, story_fingerprint, published_day, status,
                attempt_count, next_retry_at, last_error, payload_json, created_at, sent_at
             ) VALUES (?1, ?2, ?3, ?4, 'sent', 1, NULL, NULL, ?5, ?6, ?6)",
            params![
                candidate.url_hash,
                candidate.story_key,
                candidate.story_fingerprint,
                candidate.published_day,
                payload_json,
                Utc::now().to_rfc3339()
            ],
        )?;

        let enqueued = enqueue_candidate_alerts(&conn, &cfg, vec![candidate])?;
        assert_eq!(enqueued, 0);
        Ok(())
    }

    #[test]
    fn retry_delay_grows_exponentially() {
        let cfg = test_config();
        assert!(compute_retry_delay_secs(&cfg, 2) > compute_retry_delay_secs(&cfg, 1));
        assert!(compute_retry_delay_secs(&cfg, 6) >= compute_retry_delay_secs(&cfg, 5));
    }
}
