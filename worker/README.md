# worker

Cloudflare Worker real-fetch implementation.

## Endpoints

- `GET /health` public health check.
- `GET /sources` list source shards (requires `x-shared-token`).
- `GET /fetch?shard=0&cursor=0&limit=10` fetch paged items for one shard (requires token).

## Built-in source kinds

- RSS feeds (official vendors, media, research blogs, community feeds)
- Anthropic News web parser (`https://www.anthropic.com/news`)
- Domestic vendor web listing parser (Baidu / Aliyun / Zhipu / DeepSeek / Volcengine / MiniMax / Moonshot)
- GitHub Trending
- Hugging Face trending models API

Current built-in scale:

- 62 RSS feeds (official labs, media, research, community)
- 10 non-RSS live sources (Anthropic web, 7 domestic vendor web listings, GitHub Trending, Hugging Face models API)

## Local run

```bash
npm install
cp wrangler.example.jsonc wrangler.jsonc
npm run dev
```

## Request examples

```bash
curl -H "x-shared-token: change-me" "http://127.0.0.1:8787/sources"
curl -H "x-shared-token: change-me" "http://127.0.0.1:8787/fetch?shard=0&cursor=0&limit=10"
```
