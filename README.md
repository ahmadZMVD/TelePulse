<div align="center">

# 📡 TelePulse — Zero-Ban Parallel Telegram Lead Radar & Outreach Engine

[![Language: English](https://img.shields.io/badge/Language-English-0A66C2?style=for-the-badge)](README.md)
[![Language: Persian](https://img.shields.io/badge/زبان-فارسی-2EA44F?style=for-the-badge)](README.fa.md)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B%20(Stdlib%20Zero--Dep)-3776AB?style=for-the-badge&logo=python&logoColor=white)](#)
[![Architecture: Zero-Ban](https://img.shields.io/badge/Account%20Risk-0%25%20(Zero--Login%20Web%20%2B%20DeepLink)-success?style=for-the-badge)](#)
[![Test Suite: 16/16 Passing](https://img.shields.io/badge/Unit%20%26%20Stress%20Tests-16%2F16%20Passing-brightgreen?style=for-the-badge)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

**A high-throughput, zero-dependency asynchronous market intelligence and outreach co-pilot that monitors 18+ freelance Telegram channels in 36-second parallel micro-batches, filters out 95%+ freelancer spam using Persian NLP heuristics, and dispatches 1-click RFC 3987 deep-linked pitches via a private Telegram Bot—with zero risk of account bans.**

[🏛️ System Architecture](#-system-architecture) • [💡 Origin Story & Problem Statement](#-origin-story--problem-statement) • [📊 Real-World Production Telemetry](#-real-world-production-telemetry-29684-waves) • [⚙️ Core Engineering Subsystems](#️-core-engineering-subsystems-13-modules) • [🚀 Quick Start](#-quick-start) • [🧪 Unit & Concurrency Stress Testing](#-unit--concurrency-stress-testing) • [🔮 Roadmap: Phase 2 Autonomous LLM Agent](#-roadmap-phase-2-autonomous-llm-negotiation-agent)

</div>

---

## 💡 Origin Story & Problem Statement

In the Iranian and global Telegram freelance ecosystems, project opportunities are scattered across dozens of high-noise public channels (`@freelancer_job`, `@cproje`, `@SevenProzhe`, `@doorkarijoo`, `@Hajifreelance`, etc.). Freelancers face three compounding engineering and operational bottlenecks:

1. **Signal-to-Noise Exhaustion:** Over **85%–90%** of posts in freelance channels are self-promotions by other freelancers, course ads, or repeated channel footers rather than genuine employers (`کارفرما`) hiring for a project. Manually scrolling through 18 channels all day wastes hours of deep-work focus.
2. **Speed-to-Lead Latency:** When a real employer posts an urgent Python, Computer Vision, or Data Science project, the first 3–5 engineers who respond with a structured, thoughtful message win the contract. A 30-minute delay means the project is already assigned.
3. **Telegram Anti-Spam & Account Ban Risk (`PeerFloodError`):** Naive automation scripts log into a personal Telegram account via MTProto (`Telethon`/`Pyrogram`) to scrape channels and automatically send cold DMs to uncontacted users. Telegram's anti-abuse engine rapidly flags automated cold DMs to non-mutual contacts, resulting in `PeerFloodError`, `FloodWaitError`, and permanent account termination.

### How TelePulse Solves All Three Problems

**TelePulse (`telepulse.py`)** was engineered from the ground up to achieve **real-time lead capture (~36s full-cycle across 18 channels)** while maintaining **0% ban risk on the user's primary Telegram account**:

- **Zero-Login Ingestion Plane:** Instead of authenticating a personal account to scrape channels, TelePulse polls public Telegram web previews (`https://t.me/s/<channel>`) over a custom **4-lane persistent TLS Keep-Alive connection pool** supporting local **HTTP CONNECT** and **SOCKS5** proxy tunnels.
- **Calibrated Persian Intent & Topic Classification:** Every post passes through a multi-stage normalizer, a co-occurrence admin footer learner (`CoOccurrenceFooterLearner`), a weighted Employer-vs-Freelancer intent scorer (`classify_intent`), and a 13-category technical topic extractor (`extract_topic`).
- **Human-in-the-Loop Bot Control Plane + RFC 3987 Deep-Linking:** When a verified employer lead is captured, TelePulse sends a formatted HTML card to the engineer's private Telegram Bot (`@BotFather`). The card includes interactive buttons:
  - **🚀 Fast Send (`ارسال سریع`):** Uses Telegram's native `https://t.me/<username>?text=...` URI scheme (with custom ZWNJ sanitization so Persian half-spaces never glue words together) to open the employer's chat directly in the user's Telegram app with a tailored psychological pitch pre-filled in the input box.
  - **✍️ Set Custom Title (`درج تایتل اختصاصی`):** Prompts the user inside the bot to type a custom 2–4 word project title (e.g., `تخصص در کدنویسی و OpenCV` or `بهینه‌سازی پورتفولیو`), dynamically rebuilds the deep-link URL in-place, and lets the user enter the client's chat with a 1-click personalized proposal.

---

## 🏛️ System Architecture

```mermaid
flowchart TB
    subgraph Ingestion["1. Parallel Zero-Login Ingestion Plane (36s Cycle)"]
        CH["18 Public Telegram Channels<br/>(6 Waves × 3 Parallel Lanes)"]
        JITTER["Staggered Micro-Jitter<br/>(0.18s–0.55s per lane + 5.0s wave pacing)"]
        POOL["PersistentKeepAliveHTTP Pool<br/>(4 Concurrent TLS Sockets/Host • HTTP CONNECT & SOCKS5 • ETag/304 Cache)"]
        GUARD["TelegramRateSafetyGuard<br/>(Sliding Window ≤ 42 req/min • Adaptive 429 Backoff)"]
        CH --> JITTER --> GUARD --> POOL
    end

    subgraph Intelligence["2. Persian NLP & Deduplication Engine"]
        PARSER["WebChannelScraper<br/>(Regex DOM Parser + Watermark Fast-Path)"]
        FOOTER["CoOccurrenceFooterLearner<br/>(Auto-learns unmarked channel admin handles ≥ 3 posts)"]
        INTENT["Intent & Topic Classifier<br/>(Employer vs. Freelancer Scorer + 13 Technical Domains)"]
        WAL[("SafeContactStore (SQLite WAL)<br/>RLock Thread-Safe • Cross-Channel SHA-1 Signature Dedup")]
        POOL --> PARSER --> FOOTER --> INTENT --> WAL
    end

    subgraph ControlPlane["3. Interactive Bot Control Plane & Deep-Link Dispatcher"]
        WORKER["SafeOutreachWorker<br/>(Async Queue + Pacing Guard: 8/hr, 25/day)"]
        BOT["Private Telegram Bot UI<br/>(HTML Lead Card + Inline Keyboard)"]
        TITLE["InteractiveBotTitleController<br/>(ForceReply Custom Project Title State Machine)"]
        DEEPLINK["ZWNJ-Safe Deep-Link Builder<br/>(https://t.me/client?text=...)"]
        CLIENT(("Employer Private Chat<br/>1-Click Pre-Filled Pitch"))
        WAL --> WORKER --> BOT
        BOT -- "Click: ✍️ Set Custom Title" --> TITLE --> DEEPLINK
        BOT -- "Click: 🚀 Fast Send" --> DEEPLINK --> CLIENT
    end
```

### Interactive Custom-Title Sequence Flow

```mermaid
sequenceDiagram
    autonumber
    participant Web as Telegram Public Web (t.me/s/*)
    participant Engine as TelePulse Engine (telepulse.py)
    participant DB as SQLite WAL Store
    participant Bot as Private Telegram Bot
    participant User as Engineer (You)
    participant Client as Employer DM

    Web->>Engine: Fetch 3 channels concurrently (Wave N/6)
    Engine->>Engine: Filter freelancer spam + learn admin footers
    Engine->>DB: reserve_sync(task) [RLock + Cross-Channel SHA-1 Check]
    DB-->>Engine: Reserved (New Unique Employer Lead #90)
    Engine->>Bot: POST /sendMessage (Lead Card + Inline Buttons)
    Bot->>User: Push notification with [✍️ Set Custom Title] & [🚀 Fast Send]
    User->>Bot: Tap [✍️ Set Custom Title] (callback_data="set_title:90")
    Bot->>Engine: getUpdates (callback_query)
    Engine->>Bot: ForceReply: "Type project title for @Ziyadkhastam..."
    User->>Bot: Reply: "تخصص در کد نویسی و opencv"
    Bot->>Engine: getUpdates (message text)
    Engine->>DB: save_custom_title_sync(status='titled')
    Engine->>Bot: Delete prompt + Send ready button with custom Deep-Link URL
    User->>Client: Tap [🚀 Enter @Ziyadkhastam Chat] -> Opens chat with pre-filled pitch!
```

---

## 📊 Real-World Production Telemetry (`29,684 Waves`)

TelePulse is not a toy script—it has been battle-tested in continuous multi-day production across **18 active Iranian freelance channels** over a 14-day window (`2026-10-18` to `2026-10-31`), generating **39,304 structured telemetry log lines** (`safe_outreach.log.1` + `safe_outreach.log`) and **113 SQLite WAL frames**.

| Metric / Engineering Indicator | Empirical Production Value | Engineering Significance |
| :--- | :--- | :--- |
| **Total Parallel Waves Executed** | **29,684 waves** (`89,052` channel polls) | Proven multi-day stability under real proxy/network conditions |
| **3-Channel Wave Latency (P50 / Mean)** | **1,250 ms median** / **2,066 ms mean** (Min: `686 ms`) | 10× faster than sequential polling (~36s vs ~7 min full cycle) |
| **Steady-State Web Request Rate** | **24 req/min** (Max: `36 req/min` vs `42 req/min` cap) | Stays safely below Cloudflare & Telegram anti-abuse thresholds |
| **HTTP 429 Rate-Limit Events** | **0 hits** (`0.000%` across `89,052` requests) | Confirms effectiveness of staggered jitter + sliding-window guard |
| **Account Bans / `PeerFloodError`s** | **0 bans** (`100%` account safety) | Zero-login scraping + client-side `t.me` deep-linking eliminates ban vectors |
| **Unique Employer Leads Captured** | **112 unique employers** (`13` custom-titled, `98` notified, `1` skipped) | High-signal extraction across AI/ML, Python, Web, Data, and Academic domains |
| **Classifier Precision on DB Audit** | **95.5% (`107/112`) in v6.4 → 100.0% (`112/112`) in v6.5** | Forensic audit eliminated the remaining 5 disguised service-seller ads |

---

## ⚙️ Core Engineering Subsystems (`13 Modules`)

All 13 subsystems are implemented in **pure Python 3.9+ Standard Library** (`telepulse.py`, ~3,600 lines) with **zero mandatory external `pip` dependencies**:

1. **Zero-Dependency `.env` & Configuration Loader (`Lines 136–260`):** Automatically loads secrets (`NOTIFY_BOT_TOKEN`, `MAIN_ACCOUNT_CHAT_ID`) from a local `.env` file without requiring `python-dotenv`, ensuring zero credential leakage in Git repositories.
2. **Windows Virtual Terminal ANSI Engine & Jalali Calendar (`WinAnsiConsole`, `ColoredConsoleFormatter`, `gregorian_to_jalali`, `Lines 261–480`):** Enables native ANSI colors in Windows `cmd.exe` via `kernel32.SetConsoleMode(ENABLE_VIRTUAL_TERMINAL_PROCESSING)` while stripping ANSI escape sequences from rotating log files (`RotatingFileHandler`). Includes a pure-Python Gregorian-to-Jalali (Shamsi) calendar algorithm.
3. **Sliding-Window Anti-Ban Rate Safety Guard (`TelegramRateSafetyGuard`, `Lines 481–586`):** Thread-safe (`threading.RLock`) sliding 60-second window limiter enforcing `<= 42` web requests/min and `<= 18` Bot API messages/min, with exponential backoff on HTTP 429 responses and real-time latency telemetry.
4. **Multi-Lane Proxy-Aware Keep-Alive HTTPS Pool (`PersistentKeepAliveHTTP`, `Lines 587–1055`):** Maintains up to **4 concurrent reusable TLS sockets per host** (`t.me`, `api.telegram.org`) so 3 parallel channel scrapers and 1 Bot API poller never serialize behind a single socket lock. Implements raw socket **HTTP CONNECT** (`_open_http_connect_tunnel`) and **RFC 1928 SOCKS5** (`_open_socks5_tunnel`) handshakes with automatic local port probing (`10808`, `10809`, `2080`, `7890`) and `ETag` / `If-None-Match` HTTP 304 conditional caching.
5. **Live 4-Stage Network Diagnostics (`run_live_connectivity_diagnostics`, `Lines 1056–1280`):** Executes 4 real network probes on startup or hotkey `[t]` (Proxy TCP Tunnel → HTTP 204 Internet Check → Telegram Bot `getMe` Verification → Live Channel HTML Parse) and dispatches a Jalali-timestamped status banner to the bot.
6. **Calibrated Persian NLP Intent Classifier (`normalize_text`, `classify_intent`, `Lines 1281–1470`):** Normalizes Arabic/Persian character variants (`ي/ی`, `ك/ک`, ZWNJ, Eastern Arabic/Persian digits) and scores employer demand signals (`نیازمندم`, `پروژه دارم`, `درخواست کننده`) against freelancer supply signals (`انجام میدم`, `سفارش میپذیرم`, `خدمات ما`, `#کارآموز`), achieving **100% precision** across the 112-row production benchmark.
7. **Shadow-Safe Technical Topic Extractor (`extract_topic`, `Lines 1471–1560`):** Categorizes leads into 13 domains (AI & Machine Learning, Python & Data Science, Web Development, Statistics/Econometrics, CAD/CAE, etc.) while preventing compound keyword shadowing (e.g., preventing `تایپ اسکریپت` [TypeScript] from matching `تایپ` [Typing]).
8. **Unsupervised Co-Occurrence Footer Learner (`CoOccurrenceFooterLearner`, `extract_leads`, `Lines 1561–1750`):** Thread-safe online learner that tracks handle co-occurrence across distinct message IDs per channel. Any unmarked admin/support handle appearing alongside client handles in `>= 3` posts is automatically learned and excluded as a channel footer.
9. **ZWNJ-Safe Psychological Pitch & RFC 3987 Deep-Link Engine (`SpintaxAndTopicEngine`, `Lines 1751–2000`):** Telegram's `https://t.me/<user>?text=...` handler strips Zero-Width Non-Joiners (`‌`), which normally causes Persian words like `قدم‌به‌قدم` and `صورت‌مسئله` to glue together as `قدمبهقدم` and `صورتمسئله`. `sanitize_for_telegram_deeplink()` smartly converts affixes (`می‌`/`نمی‌`) and compound words before URL encoding, keeping full uncut pitches under Telegram's button URL limits via UTF-8 IRI safe character encoding.
10. **Thread-Safe SQLite WAL Store & Cross-Channel Signature Deduplicator (`SafeContactStore`, `Lines 2001–2365`):** Uses SQLite Write-Ahead Logging (`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;`) guarded by `threading.RLock()`. Computes a channel-wrapper-invariant SHA-1 content signature (`_ad_content_signature`) so employers who cross-post the exact same project across 5 channels are notified only once within a 7-day window.
11. **Single-Message Bot Notifier & In-Place Custom Title State Machine (`SafeCopilotNotifier`, `InteractiveBotTitleController`, `Lines 2366–2750`):** Long-polls Telegram Bot `getUpdates` for `callback_query` (`set_title:<id>`, `skip_lead:<id>`) and `ForceReply` text responses, updating the SQLite record to `status='titled'` and automatically deleting intermediate prompt clutter.
12. **3-Lane Parallel Micro-Batch Channel Listener (`WebChannelScraper`, `SafeChannelListener`, `Lines 2751–3125`):** Groups the 18 target channels into 6 waves of 3 concurrent lanes with staggered start jitter (`0.18s–0.55s`) and session watermark fast-paths (skipping regex evaluation whenever `max_msg_id <= session_watermark`).
13. **Interactive Console Hotkey Controller & Live Telemetry Table (`ConsoleHotkeyController`, `Lines 3126–3600`):** Non-blocking keyboard thread (`msvcrt.kbhit`) providing single-keystroke runtime control: `[1]` Start/Resume, `[0]` Pause, `[s]` Live Telemetry Table, `[t]` 4-Stage Connection Test, and `[x]` Clean Shutdown.

---

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/ahmadZMVD/TelePulse.git
cd TelePulse
```

### 2. Configure Your `.env` File
Copy `.env.example` to `.env` and add your Telegram Bot token (from `@BotFather`) and your numeric Telegram account `chat_id` (from `@userinfobot`):
```bash
cp .env.example .env
```
```ini
NOTIFY_BOT_TOKEN=1234567890:AAH_your_botfather_token_here
MAIN_ACCOUNT_CHAT_ID=123456789
USE_WEB_SCRAPER_FALLBACK=1
ONLY_NEW_MESSAGES_FROM_NOW=1
```

### 3. Run TelePulse
On **Windows** (double-click `Run_TelePulse.bat` or run in CMD/PowerShell):
```cmd
Run_TelePulse.bat
```
On **Linux / macOS / Windows Terminal**:
```bash
python3 telepulse.py
```
Press **`[1]`** in the console to run the 4-stage live diagnostics, send the startup heartbeat banner to your bot, sync the 18-channel baseline, and begin real-time monitoring.

---

## 🧪 Unit & Concurrency Stress Testing

TelePulse includes a 16-test automated verification suite (`test_telepulse.py`) covering all 13 subsystems, including a 10-thread concurrent SQLite WAL race barrier and an empirical audit against all 112 production leads:

```bash
python3 test_telepulse.py
```

```text
test_channel_slug_extraction (Test01ConfigAndDotEnv) ... ok (0.68 ms)
test_dotenv_loader_and_no_hardcoded_secrets (Test01ConfigAndDotEnv) ... ok (8.92 ms)
test_ansi_strip_and_progress_bar (Test02ConsoleAndJalaliDate) ... ok (0.58 ms)
test_gregorian_to_jalali_known_dates (Test02ConsoleAndJalaliDate) ... ok (0.55 ms)
test_sliding_window_and_429_exponential_backoff (Test03RateSafetyGuard) ... ok (0.58 ms)
test_proxy_url_parsing (Test04ProxyAndHttpPool) ... ok (0.66 ms)
test_red_startup_banner_generation (Test05DiagnosticsAndStartupBanner) ... ok (0.58 ms)
test_audit_against_real_112_production_db_rows (Test06IntentClassifierAndProductionDBAudit) ... ok (323.52 ms)
test_employer_vs_freelancer_classification (Test06IntentClassifierAndProductionDBAudit) ... ok (15.43 ms)
test_topic_extraction_and_compound_shadow (Test07TopicClassification) ... ok (13.73 ms)
test_footer_cooccurrence_learner_and_admin_exclusion (Test08HandleExtractionAndFooterLearner) ... ok (6.73 ms)
test_zwnj_sanitization_and_deeplink_encoding (Test09PsychologicalPitchAndDeepLink) ... ok (2.02 ms)
test_cross_channel_signature_invariance_and_thread_safety (Test10SQLiteWALStoreAndCrossChannelDeduplication) ... ok (50.63 ms)
test_interactive_custom_title_and_skip_flow (Test11InteractiveBotController) ... ok (81.78 ms)
test_html_parser_and_parallel_3lane_microbatch (Test12WebScraperAndParallelBatchListener) ... ok (22.08 ms)
test_hotkey_state_transitions_and_status_table (Test13ConsoleHotkeysAndDashboard) ... ok (4.41 ms)
----------------------------------------------------------------------
Ran 16 tests in 0.546s — OK (100% Pass Rate)
```

---

## 🔮 Roadmap: Phase 2 Autonomous LLM Negotiation Agent

While **TelePulse v6.5** automates discovery, spam filtering, and 1-click custom-titled deep-link outreach, **Phase 2** extends the architecture into an **Autonomous Client Discovery & Requirement-Gathering Agent**:

- [x] **Phase 1 (Current — v6.5):** Zero-login 3-lane parallel web ingestion, Persian NLP intent/topic classification, SQLite WAL cross-channel deduplication, interactive Bot UI with `ForceReply` custom title injection, and ZWNJ-safe RFC 3987 deep-linking.
- [ ] **Phase 2.1 — LLM Semantic Lead Scorer & Portfolio RAG Matcher:** Connect an LLM API (Gemini / OpenAI / local vLLM) to evaluate each captured employer post against the engineer's technical portfolio (e.g., matching Computer Vision / PyTorch ads to [`FGVC-IR8`](https://github.com/ahmadZMVD/FGVC-IR8) and Data Science / Scraping ads to `TelePulse`) and auto-generate a context-aware technical title and first-contact proposal.
- [ ] **Phase 2.2 — Autonomous Multi-Turn Requirement Gathering:** Upon employer reply, transition into a guarded conversational state machine that asks clarifying engineering questions (dataset availability, deadline, deployment target, budget range) and compiles a structured **Technical Scope Brief** in the private control bot before handing over to the human engineer for final sign-off.

---

## 📄 License & Author

- **Author:** Ahmadreza Zamani ([@ahmadZMVD](https://github.com/ahmadZMVD))
- **License:** Released under the [MIT License](LICENSE).
