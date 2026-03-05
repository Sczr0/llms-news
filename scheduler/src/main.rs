use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
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

#[derive(Debug, Clone, Serialize)]
struct Candidate {
    url_hash: String,
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
    "You are an AI news gatekeeper. Return strict JSON with keys: is_ai_related(boolean), topic(open_source_tool|ai_news|model_eval|unknown), score_adjustment(integer -20..20), reason(short)."
}

fn big_llm_system_prompt() -> &'static str {
    "You are a Chinese AI editor. Return strict JSON with keys: category(open_source_tool|ai_news|model_eval), title_zh, summary_zh(2-4 sentences in Chinese), angle(one Chinese sentence), opinion(one Chinese sentence with viewpoint)."
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

    if candidates.is_empty() {
        println!("[scheduler] no new candidates");
        return Ok(());
    }

    candidates = filter_candidates_by_small_llm(&client, &cfg, candidates).await?;
    if candidates.is_empty() {
        println!("[scheduler] no candidates after small-llm filtering");
        return Ok(());
    }

    candidates = enrich_candidates_by_big_llm(&client, &cfg, candidates).await?;

    candidates.sort_by(|a, b| b.score.cmp(&a.score));

    let shortlisted: Vec<Candidate> = candidates
        .into_iter()
        .filter(|c| c.score >= tier_cfg.min_score_for_tier(&c.tier, cfg.candidate_min_score))
        .collect();

    println!(
        "[scheduler] shortlisted={} (tier-threshold, fallback >= {})",
        shortlisted.len(),
        cfg.candidate_min_score
    );
    notify_candidates(&conn, &client, &cfg, shortlisted).await?;
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
  sent_at TEXT NOT NULL
);
"#,
    )?;
    Ok(())
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
        url_hash: hash_url(&item.url),
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

async fn notify_candidates(
    conn: &Connection,
    client: &reqwest::Client,
    cfg: &Config,
    candidates: Vec<Candidate>,
) -> Result<()> {
    for item in candidates {
        if already_alerted(conn, &item.url_hash)? {
            continue;
        }
        let topic_zh = topic_label_zh(&item.topic);
        let summary = item.summary.clone().unwrap_or_else(|| "-".to_string());

        let msg = format!(
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
        );
        println!("{msg}");

        if let Some(webhook) = &cfg.notify_webhook {
            let payload = build_webhook_payload(webhook, &item, &msg);
            let _ = client.post(webhook).json(&payload).send().await;
        }

        mark_alerted(conn, &item.url_hash)?;
    }
    Ok(())
}

fn already_alerted(conn: &Connection, url_hash: &str) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT COUNT(1) FROM alerts WHERE url_hash = ?1")?;
    let count: i64 = stmt.query_row([url_hash], |r| r.get(0))?;
    Ok(count > 0)
}

fn mark_alerted(conn: &Connection, url_hash: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO alerts (url_hash, sent_at) VALUES (?1, ?2)",
        params![url_hash, Utc::now().to_rfc3339()],
    )?;
    Ok(())
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

fn hash_url(url: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    hex::encode(hasher.finalize())
}
