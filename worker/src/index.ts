type Env = {
  SHARED_TOKEN: string;
  PER_SOURCE_LIMIT?: string;
  REQUEST_TIMEOUT_MS?: string;
  EXTRA_RSS_SOURCES?: string;
};

type SourceKind = "rss" | "anthropic_news" | "web_listing" | "github_trending" | "hf_models";

type SourceDef = {
  id: number;
  name: string;
  kind: SourceKind;
  url?: string;
  includePathHints?: string[];
  excludePathHints?: string[];
};

type RawItem = {
  id: string;
  title: string;
  url: string;
  source: string;
  published_at: string;
  content: string;
};

type FetchResponse = {
  done: boolean;
  next_cursor: number | null;
  shard: number;
  source_name: string;
  total: number;
  items: RawItem[];
};

const DEFAULT_RSS_SOURCES: Array<{ name: string; url: string }> = [
  { name: "openai-news", url: "https://openai.com/news/rss.xml" },
  { name: "google-deepmind", url: "https://deepmind.google/blog/rss.xml" },
  { name: "google-ai-blog", url: "https://blog.google/technology/ai/rss/" },
  { name: "huggingface-blog", url: "https://huggingface.co/blog/feed.xml" },
  { name: "stability-ai-news", url: "https://stability.ai/news?format=rss" },
  { name: "jiqizhixin", url: "https://www.jiqizhixin.com/rss" },
  { name: "juya-ai-daily", url: "https://imjuya.github.io/juya-ai-daily/rss.xml" },
  { name: "aws-ml-blog", url: "https://aws.amazon.com/blogs/machine-learning/feed/" },
  { name: "microsoft-research", url: "https://www.microsoft.com/en-us/research/feed/" },
  { name: "nvidia-blog", url: "https://blogs.nvidia.com/feed/" },
  { name: "nvidia-newsroom", url: "https://nvidianews.nvidia.com/rss" },
  { name: "apple-ml-research", url: "https://machinelearning.apple.com/rss.xml" },
  { name: "mit-machine-learning", url: "https://news.mit.edu/rss/topic/machine-learning" },
  { name: "tensorflow-blog", url: "https://blog.tensorflow.org/feeds/posts/default" },
  { name: "bair-blog", url: "https://bair.berkeley.edu/blog/feed.xml" },
  { name: "cmu-ml-blog", url: "https://blog.ml.cmu.edu/feed/" },
  { name: "kdnuggets", url: "https://www.kdnuggets.com/feed" },
  { name: "neptune-ai-blog", url: "https://neptune.ai/blog/feed" },
  { name: "bigml-blog", url: "https://blog.bigml.com/feed/" },
  { name: "cisco-ml", url: "https://blogs.cisco.com/tag/machine-learning/feed" },
  { name: "oreilly-ai-ml", url: "https://www.oreilly.com/radar/topics/ai-ml/feed/index.xml" },
  { name: "nanonets-blog", url: "https://nanonets.com/blog/rss/" },
  { name: "machinelearningmastery", url: "https://machinelearningmastery.com/feed/" },
  {
    name: "marktechpost-ai",
    url: "https://www.marktechpost.com/category/technology/artificial-intelligence/feed/"
  },
  { name: "pyimagesearch", url: "https://pyimagesearch.com/feed/" },
  { name: "towards-ai", url: "https://towardsai.net/ai/artificial-intelligence/feed/" },
  { name: "azure-blog", url: "https://azure.microsoft.com/en-us/blog/feed/" },
  { name: "distill", url: "https://distill.pub/rss.xml" },
  { name: "mlberkeley-substack", url: "https://mlberkeley.substack.com/feed" },
  { name: "becominghuman", url: "https://becominghuman.ai/feed" },
  { name: "ergodicity", url: "https://ergodicity.net/feed" },
  { name: "programmathically", url: "https://programmathically.com/feed" },
  { name: "datumbox-blog", url: "https://blog.datumbox.com/feed" },
  { name: "city-ml-blog", url: "https://blogs.city.ac.uk/ml/feed" },
  { name: "datadive", url: "https://blog.datadive.net/feed" },
  {
    name: "let-data-speak",
    url: "https://letdataspeak.blogspot.com/feeds/posts/default?alt=rss"
  },
  { name: "inference-vc", url: "https://inference.vc/rss" },
  {
    name: "sciencedaily-ai",
    url: "https://www.sciencedaily.com/rss/computers_math/artificial_intelligence.xml"
  },
  { name: "fastai", url: "https://www.fast.ai/index.xml" },
  { name: "analytics-vidhya", url: "https://www.analyticsvidhya.com/feed/" },
  { name: "techcrunch-ai", url: "https://techcrunch.com/category/artificial-intelligence/feed/" },
  { name: "venturebeat-ai", url: "https://venturebeat.com/category/ai/feed/" },
  { name: "theverge-ai", url: "https://www.theverge.com/rss/ai/index.xml" },
  { name: "wired-ai", url: "https://www.wired.com/feed/tag/ai/latest/rss" },
  { name: "arxiv-cs-ai", url: "https://rss.arxiv.org/rss/cs.AI" },
  { name: "arxiv-cs-lg", url: "https://rss.arxiv.org/rss/cs.LG" },
  { name: "arxiv-cs-cl", url: "https://rss.arxiv.org/rss/cs.CL" },
  { name: "arxiv-cs-cv", url: "https://rss.arxiv.org/rss/cs.CV" },
  { name: "linuxdo-latest", url: "https://linux.do/latest.rss" },
  { name: "linuxdo-top", url: "https://linux.do/top.rss" },
  { name: "v2ex-hot", url: "https://www.v2ex.com/feed/tab/hot.xml" },
  { name: "v2ex-tech", url: "https://www.v2ex.com/feed/tab/tech.xml" },
  { name: "ithome", url: "https://www.ithome.com/rss/" },
  { name: "mit-technology-review", url: "https://www.technologyreview.com/feed/" },
  { name: "ars-technica", url: "https://feeds.arstechnica.com/arstechnica/index" },
  { name: "reddit-machinelearning", url: "https://www.reddit.com/r/MachineLearning/.rss" },
  { name: "reddit-localllama", url: "https://www.reddit.com/r/LocalLLaMA/.rss" },
  { name: "reddit-artificial", url: "https://www.reddit.com/r/artificial/.rss" },
  { name: "reddit-singularity", url: "https://www.reddit.com/r/singularity/.rss" },
  { name: "hn-ai", url: "https://hnrss.org/newest?q=AI" },
  { name: "hn-llm", url: "https://hnrss.org/newest?q=LLM" },
  { name: "hn-gemini-gpt", url: "https://hnrss.org/newest?q=Gemini+OR+GPT" }
];

function toJson(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" }
  });
}

function unauthorized(): Response {
  return toJson({ error: "unauthorized" }, 401);
}

function getEnvNumber(raw: string | undefined, fallback: number): number {
  if (!raw) {
    return fallback;
  }
  const v = Number(raw);
  return Number.isFinite(v) && v > 0 ? v : fallback;
}

function parseExtraRssSources(raw: string | undefined): Array<{ name: string; url: string }> {
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .map((item) => {
        if (!item || typeof item !== "object") {
          return null;
        }
        const name = String((item as Record<string, unknown>).name || "").trim();
        const url = String((item as Record<string, unknown>).url || "").trim();
        if (!name || !url) {
          return null;
        }
        return { name, url };
      })
      .filter((item): item is { name: string; url: string } => Boolean(item));
  } catch {
    return [];
  }
}

function buildSources(env: Env): SourceDef[] {
  const rss = [...DEFAULT_RSS_SOURCES, ...parseExtraRssSources(env.EXTRA_RSS_SOURCES)];
  const out: SourceDef[] = [];
  const seenFeedUrl = new Set<string>();
  for (const source of rss) {
    const feedUrl = source.url.trim();
    if (!feedUrl.startsWith("http://") && !feedUrl.startsWith("https://")) {
      continue;
    }
    const normalizedFeedUrl = feedUrl.toLowerCase();
    if (seenFeedUrl.has(normalizedFeedUrl)) {
      continue;
    }
    seenFeedUrl.add(normalizedFeedUrl);
    out.push({
      id: out.length,
      name: source.name,
      kind: "rss",
      url: feedUrl
    });
  }
  out.push({
    id: out.length,
    name: "anthropic-news-web",
    kind: "anthropic_news",
    url: "https://www.anthropic.com/news"
  });
  out.push({
    id: out.length,
    name: "baidu-qianfan-web",
    kind: "web_listing",
    url: "https://cloud.baidu.com/doc/WENXINWORKSHOP/index.html",
    includePathHints: ["/doc/wenxinworkshop/s/"]
  });
  out.push({
    id: out.length,
    name: "aliyun-modelstudio-release-notes",
    kind: "web_listing",
    url: "https://help.aliyun.com/zh/model-studio/application-release-notes",
    includePathHints: ["/zh/model-studio/application-release-notes", "/zh/model-studio/"]
  });
  out.push({
    id: out.length,
    name: "zhipu-news-web",
    kind: "web_listing",
    url: "https://www.zhipuai.cn/zh/news",
    includePathHints: ["/zh/news/"]
  });
  out.push({
    id: out.length,
    name: "deepseek-updates-web",
    kind: "web_listing",
    url: "https://www.deepseek.com/updates",
    includePathHints: ["/updates/"]
  });
  out.push({
    id: out.length,
    name: "volcengine-ark-docs-web",
    kind: "web_listing",
    url: "https://www.volcengine.com/docs/82379",
    includePathHints: ["/docs/82379/"]
  });
  out.push({
    id: out.length,
    name: "minimax-news-web",
    kind: "web_listing",
    url: "https://www.minimax.io/news",
    includePathHints: ["/news/"]
  });
  out.push({
    id: out.length,
    name: "moonshot-blog-web",
    kind: "web_listing",
    url: "https://platform.moonshot.cn/blog",
    includePathHints: ["/blog/"]
  });
  out.push({
    id: out.length,
    name: "github-trending-ai",
    kind: "github_trending"
  });
  out.push({
    id: out.length,
    name: "huggingface-trending-models",
    kind: "hf_models"
  });
  return out;
}

function decodeHtml(text: string): string {
  const map: Record<string, string> = {
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": "\"",
    "&#39;": "'",
    "&nbsp;": " "
  };
  return text.replace(/&amp;|&lt;|&gt;|&quot;|&#39;|&nbsp;/g, (s) => map[s] || s);
}

function stripTags(text: string): string {
  return decodeHtml(
    text
      .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
      .replace(/<[^>]*>/g, " ")
      .replace(/\s+/g, " ")
      .trim()
  );
}

function pickTag(block: string, tag: string): string | null {
  const regex = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const match = block.match(regex);
  return match ? stripTags(match[1]) : null;
}

function pickLink(block: string): string | null {
  const rssMatch = block.match(/<link[^>]*>([\s\S]*?)<\/link>/i);
  if (rssMatch?.[1]) {
    return stripTags(rssMatch[1]);
  }
  const atomMatch = block.match(/<link[^>]*href=["']([^"']+)["'][^>]*\/?>/i);
  if (atomMatch?.[1]) {
    return atomMatch[1].trim();
  }
  return null;
}

function absoluteUrl(input: string, base?: string): string | null {
  try {
    if (base) {
      return new URL(input, base).toString();
    }
    return new URL(input).toString();
  } catch {
    return null;
  }
}

function parseRssOrAtom(xml: string, sourceName: string, feedUrl: string): RawItem[] {
  const now = new Date().toISOString();
  const itemBlocks = xml.match(/<item\b[\s\S]*?<\/item>/gi) || [];
  const entryBlocks = itemBlocks.length === 0 ? xml.match(/<entry\b[\s\S]*?<\/entry>/gi) || [] : [];
  const blocks = itemBlocks.length > 0 ? itemBlocks : entryBlocks;

  const items: RawItem[] = [];
  for (const block of blocks) {
    const title = pickTag(block, "title") || "";
    const linkRaw = pickLink(block);
    const link = linkRaw ? absoluteUrl(linkRaw, feedUrl) : null;
    const published =
      pickTag(block, "pubDate") ||
      pickTag(block, "published") ||
      pickTag(block, "updated") ||
      now;
    const description =
      pickTag(block, "description") ||
      pickTag(block, "summary") ||
      pickTag(block, "content") ||
      "";
    const guid = pickTag(block, "guid") || pickTag(block, "id") || link || title;
    if (!title || !link) {
      continue;
    }
    items.push({
      id: `${sourceName}:${guid}`.slice(0, 300),
      title: title.slice(0, 300),
      url: link,
      source: sourceName,
      published_at: normalizeIsoDate(published) || now,
      content: description.slice(0, 1000)
    });
  }
  return items;
}

function normalizeIsoDate(input: string): string | null {
  const parsed = new Date(input);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

async function fetchTextWithTimeout(url: string, timeoutMs: number): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("timeout"), timeoutMs);
  try {
    const resp = await fetch(url, {
      headers: {
        "user-agent": "news-radar-worker/0.2",
        accept: "text/html,application/xml,text/xml,application/atom+xml"
      },
      signal: controller.signal
    });
    if (!resp.ok) {
      throw new Error(`fetch failed: ${resp.status} ${resp.statusText}`);
    }
    return await resp.text();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchRssSource(source: SourceDef, limit: number, timeoutMs: number): Promise<RawItem[]> {
  const url = source.url!;
  const text = await fetchTextWithTimeout(url, timeoutMs);
  return parseRssOrAtom(text, source.name, url).slice(0, limit);
}

async function fetchGitHubTrending(limit: number, timeoutMs: number): Promise<RawItem[]> {
  const html = await fetchTextWithTimeout("https://github.com/trending?since=daily", timeoutMs);
  const now = new Date().toISOString();
  const regex = /<h2[^>]*>\s*<a[^>]*href="\/([^"?#]+\/[^"?#]+)"/gi;
  const seen = new Set<string>();
  const out: RawItem[] = [];
  let match: RegExpExecArray | null = regex.exec(html);
  while (match && out.length < limit) {
    const repo = (match[1] || "").trim();
    if (repo && !seen.has(repo)) {
      seen.add(repo);
      out.push({
        id: `github:${repo}`,
        title: `GitHub Trending: ${repo}`,
        url: `https://github.com/${repo}`,
        source: "github-trending-ai",
        published_at: now,
        content: "Trending repository from GitHub daily ranking."
      });
    }
    match = regex.exec(html);
  }
  return out;
}

type HuggingFaceModel = {
  id?: string;
  likes?: number;
  downloads?: number;
  lastModified?: string;
  pipeline_tag?: string;
};

async function fetchHfTrending(limit: number, timeoutMs: number): Promise<RawItem[]> {
  const url = `https://huggingface.co/api/models?sort=trendingScore&direction=-1&limit=${Math.max(
    5,
    limit
  )}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("timeout"), timeoutMs);
  try {
    const resp = await fetch(url, {
      headers: {
        "user-agent": "news-radar-worker/0.2",
        accept: "application/json"
      },
      signal: controller.signal
    });
    if (!resp.ok) {
      throw new Error(`hf models api failed: ${resp.status} ${resp.statusText}`);
    }
    const payload = (await resp.json()) as HuggingFaceModel[];
    const now = new Date().toISOString();
    return payload.slice(0, limit).flatMap((model) => {
      if (!model.id) {
        return [];
      }
      const detail = [
        `pipeline=${model.pipeline_tag || "unknown"}`,
        `likes=${model.likes ?? 0}`,
        `downloads=${model.downloads ?? 0}`
      ].join(", ");
      return [
        {
          id: `hf:${model.id}`,
          title: `HF Trending Model: ${model.id}`,
          url: `https://huggingface.co/${model.id}`,
          source: "huggingface-trending-models",
          published_at: normalizeIsoDate(model.lastModified || "") || now,
          content: detail
        }
      ];
    });
  } finally {
    clearTimeout(timer);
  }
}

function titleFromSlug(pathname: string): string {
  const last = pathname.split("/").filter(Boolean).pop() || "news";
  return last
    .split("-")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

async function fetchAnthropicNews(source: SourceDef, limit: number, timeoutMs: number): Promise<RawItem[]> {
  const newsUrl = source.url || "https://www.anthropic.com/news";
  const html = await fetchTextWithTimeout(newsUrl, timeoutMs);
  const now = new Date().toISOString();
  const regex = /<a[^>]*href=["'](\/news\/[^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  const seen = new Set<string>();
  const out: RawItem[] = [];

  let match: RegExpExecArray | null = regex.exec(html);
  while (match && out.length < limit) {
    const href = (match[1] || "").trim();
    const abs = absoluteUrl(href, newsUrl);
    const anchorText = stripTags(match[2] || "");
    if (!abs) {
      match = regex.exec(html);
      continue;
    }
    let path: string;
    try {
      path = new URL(abs).pathname;
    } catch {
      match = regex.exec(html);
      continue;
    }
    // 过滤目录页和重复链接，只保留具体文章页。
    if (path === "/news" || path === "/news/" || seen.has(abs)) {
      match = regex.exec(html);
      continue;
    }
    seen.add(abs);

    const contextStart = Math.max(0, match.index - 260);
    const contextEnd = Math.min(html.length, match.index + 260);
    const context = html.slice(contextStart, contextEnd);
    const dateMatch = context.match(/datetime=["']([^"']+)["']/i);
    const dateCandidate = dateMatch?.[1] || "";
    const publishedAt = normalizeIsoDate(dateCandidate) || now;

    const title = anchorText.length >= 4 ? anchorText : titleFromSlug(path);
    out.push({
      id: `anthropic:${path}`.slice(0, 300),
      title: title.slice(0, 300),
      url: abs,
      source: source.name,
      published_at: publishedAt,
      content: stripTags(context).slice(0, 1000)
    });
    match = regex.exec(html);
  }
  return out;
}

function normalizeHintList(hints: string[] | undefined): string[] {
  if (!Array.isArray(hints)) {
    return [];
  }
  return hints.map((hint) => hint.trim().toLowerCase()).filter((hint) => hint.length > 0);
}

function shouldKeepWebLink(pathAndQuery: string, includeHints: string[], excludeHints: string[]): boolean {
  if (excludeHints.some((hint) => pathAndQuery.includes(hint))) {
    return false;
  }
  if (includeHints.length === 0) {
    return true;
  }
  return includeHints.some((hint) => pathAndQuery.includes(hint));
}

function pickDateFromHtmlContext(context: string): string {
  const datetimeMatch = context.match(/datetime=["']([^"']+)["']/i);
  if (datetimeMatch?.[1]) {
    return datetimeMatch[1];
  }
  const dateMatch = context.match(
    /\b(20\d{2}[-/.]\d{1,2}[-/.]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?)\b/
  );
  return dateMatch?.[1] || "";
}

async function fetchWebListingSource(
  source: SourceDef,
  limit: number,
  timeoutMs: number
): Promise<RawItem[]> {
  const listingUrl = source.url;
  if (!listingUrl) {
    throw new Error(`missing source url: ${source.name}`);
  }
  const html = await fetchTextWithTimeout(listingUrl, timeoutMs);
  const now = new Date().toISOString();
  const includeHints = normalizeHintList(source.includePathHints);
  const excludeHints = normalizeHintList(source.excludePathHints);
  const anchorRegex = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  const seen = new Set<string>();
  const out: RawItem[] = [];
  const baseUrl = new URL(listingUrl);
  const basePath = baseUrl.pathname.replace(/\/+$/, "").toLowerCase();

  let match: RegExpExecArray | null = anchorRegex.exec(html);
  while (match && out.length < limit) {
    const hrefRaw = decodeHtml((match[1] || "").trim());
    const hrefLower = hrefRaw.toLowerCase();
    if (
      !hrefRaw ||
      hrefLower.startsWith("#") ||
      hrefLower.startsWith("javascript:") ||
      hrefLower.startsWith("mailto:")
    ) {
      match = anchorRegex.exec(html);
      continue;
    }

    const abs = absoluteUrl(hrefRaw, listingUrl);
    if (!abs || seen.has(abs)) {
      match = anchorRegex.exec(html);
      continue;
    }

    let target: URL;
    try {
      target = new URL(abs);
    } catch {
      match = anchorRegex.exec(html);
      continue;
    }
    if (target.hostname !== baseUrl.hostname) {
      match = anchorRegex.exec(html);
      continue;
    }

    const targetPath = target.pathname.replace(/\/+$/, "").toLowerCase();
    const pathAndQuery = `${target.pathname}${target.search}`.toLowerCase();
    if (!targetPath || targetPath === basePath) {
      match = anchorRegex.exec(html);
      continue;
    }
    if (!shouldKeepWebLink(pathAndQuery, includeHints, excludeHints)) {
      match = anchorRegex.exec(html);
      continue;
    }

    seen.add(abs);
    const contextStart = Math.max(0, match.index - 260);
    const contextEnd = Math.min(html.length, match.index + 260);
    const context = html.slice(contextStart, contextEnd);
    const dateCandidate = pickDateFromHtmlContext(context);
    const publishedAt = normalizeIsoDate(dateCandidate) || now;
    const anchorText = stripTags(match[2] || "");
    const title = anchorText.length >= 4 ? anchorText : titleFromSlug(target.pathname);

    out.push({
      id: `${source.name}:${target.pathname}${target.search}`.slice(0, 300),
      title: title.slice(0, 300),
      url: abs,
      source: source.name,
      published_at: publishedAt,
      content: stripTags(context).slice(0, 1000)
    });
    match = anchorRegex.exec(html);
  }
  return out;
}

async function loadItems(source: SourceDef, limit: number, timeoutMs: number): Promise<RawItem[]> {
  if (source.kind === "rss") {
    return fetchRssSource(source, limit, timeoutMs);
  }
  if (source.kind === "anthropic_news") {
    return fetchAnthropicNews(source, limit, timeoutMs);
  }
  if (source.kind === "web_listing") {
    return fetchWebListingSource(source, limit, timeoutMs);
  }
  if (source.kind === "github_trending") {
    return fetchGitHubTrending(limit, timeoutMs);
  }
  return fetchHfTrending(limit, timeoutMs);
}

function paginateItems(items: RawItem[], cursor: number, pageSize: number): FetchResponse {
  const start = Math.min(cursor, items.length);
  const end = Math.min(start + pageSize, items.length);
  const done = end >= items.length;
  return {
    done,
    next_cursor: done ? null : end,
    shard: 0,
    source_name: "",
    total: items.length,
    items: items.slice(start, end)
  };
}

function requireToken(request: Request, env: Env): boolean {
  const token = request.headers.get("x-shared-token") || "";
  return Boolean(env.SHARED_TOKEN) && token === env.SHARED_TOKEN;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const sources = buildSources(env);
    const perSourceLimit = getEnvNumber(env.PER_SOURCE_LIMIT, 20);
    const timeoutMs = getEnvNumber(env.REQUEST_TIMEOUT_MS, 8000);

    if (url.pathname === "/health") {
      return toJson({ ok: true, source_count: sources.length });
    }

    if (url.pathname === "/sources") {
      if (!requireToken(request, env)) {
        return unauthorized();
      }
      return toJson({
        sources: sources.map((s) => ({ id: s.id, name: s.name, kind: s.kind }))
      });
    }

    if (url.pathname === "/fetch") {
      if (!requireToken(request, env)) {
        return unauthorized();
      }
      const shard = Number(url.searchParams.get("shard") || "0");
      const cursor = Number(url.searchParams.get("cursor") || "0");
      const pageSize = Number(url.searchParams.get("limit") || "10");
      if (
        Number.isNaN(shard) ||
        Number.isNaN(cursor) ||
        Number.isNaN(pageSize) ||
        shard < 0 ||
        cursor < 0 ||
        pageSize <= 0
      ) {
        return toJson({ error: "invalid query params" }, 400);
      }
      const source = sources[shard];
      if (!source) {
        return toJson({ error: "shard out of range" }, 404);
      }
      try {
        const allItems = await loadItems(source, perSourceLimit, timeoutMs);
        const paged = paginateItems(allItems, cursor, pageSize);
        paged.shard = shard;
        paged.source_name = source.name;
        return toJson(paged);
      } catch (error) {
        const message = error instanceof Error ? error.message : "unknown error";
        return toJson(
          {
            done: true,
            next_cursor: null,
            shard,
            source_name: source.name,
            total: 0,
            items: [],
            error: message
          },
          200
        );
      }
    }

    return toJson({ error: "not found" }, 404);
  },

  async scheduled(_event: ScheduledEvent, _env: Env, _ctx: ExecutionContext): Promise<void> {
    console.log("scheduled tick");
  }
};
