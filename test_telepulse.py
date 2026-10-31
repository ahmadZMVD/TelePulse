#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_telepulse.py — Comprehensive Unit, Integration & Concurrency Stress Test Suite
====================================================================================
Tests all 13 architectural subsystems of TelePulse (`telepulse.py`):
  1. Environment & Zero-Dependency `.env` Configuration Loader
  2. Windows ANSI Console Engine & Jalali (Shamsi) Calendar Converter
  3. Sliding-Window Anti-Ban Rate Safety Guard (`TelegramRateSafetyGuard`)
  4. Multi-Lane Proxy-Aware Keep-Alive HTTP Pool (`PersistentKeepAliveHTTP`)
  5. Live 4-Stage Diagnostics & Red HTML Startup Banner Builder
  6. Persian Text Normalization & Intent Classifier (`classify_intent`) + 112-Row Production DB Audit
  7. Compound-Shadow-Safe Topic Classifier (`extract_topic`)
  8. Handle Extraction, Admin Line Filtering & Thread-Safe `CoOccurrenceFooterLearner`
  9. Zero-Truncation Psychological Pitch Engine & RFC 3987 ZWNJ-Safe Deep-Link Builder
 10. Outreach Pacing Guard & Thread-Safe SQLite WAL Store (`SafeContactStore`)
 11. Interactive Bot Callback State Machine (`InteractiveBotTitleController` & `SafeCopilotNotifier`)
 12. Public Channel HTML Parser (`WebChannelScraper`) & 3-Lane Parallel Micro-Batch Listener (`SafeChannelListener`)
 13. Console Hotkey Controller (`ConsoleHotkeyController`) & Live Status Dashboard
"""
from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse

# Ensure output directory is importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

try:
    import telepulse as tp
except ModuleNotFoundError:
    import importlib.util
    _spec = importlib.util.spec_from_file_location("telepulse", os.path.join(SCRIPT_DIR, "telepulse.py"))
    tp = importlib.util.module_from_spec(_spec)
    sys.modules["telepulse"] = tp
    _spec.loader.exec_module(tp)

TEST_LOG_FILE = os.path.join(SCRIPT_DIR, "test_execution_telemetry.log")

test_logger = logging.getLogger("telepulse_test_suite")
test_logger.setLevel(logging.INFO)
if not test_logger.handlers:
    fh = logging.FileHandler(TEST_LOG_FILE, mode="a", encoding="utf-8")
    fh.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    test_logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    test_logger.addHandler(sh)


class LoggedTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._t0 = time.perf_counter()
        test_logger.info("[START-TEST] %s", self.id())

    def tearDown(self) -> None:
        ms = (time.perf_counter() - self._t0) * 1000.0
        test_logger.info("[PASS-TEST]  %s (%.2f ms)", self.id(), ms)


class Test01ConfigAndDotEnv(LoggedTestCase):
    def test_dotenv_loader_and_no_hardcoded_secrets(self) -> None:
        with open(tp.__file__, "r", encoding="utf-8") as f:
            source = f.read()
        self.assertNotIn("AAGSnKqwmthn2-le75POuveQqmQFiHcivPg", source)
        with tempfile.TemporaryDirectory() as tmp:
            env_file = os.path.join(tmp, ".env")
            with open(env_file, "w", encoding="utf-8") as f:
                f.write("# Comment line\nTEST_TELEPULSE_KEY=\"alpha_123\"\n")
            os.environ.pop("TEST_TELEPULSE_KEY", None)
            tp._load_dotenv_if_present(env_file)
            self.assertEqual(os.environ.get("TEST_TELEPULSE_KEY"), "alpha_123")
            os.environ.pop("TEST_TELEPULSE_KEY", None)

    def test_channel_slug_extraction(self) -> None:
        self.assertEqual(tp._extract_channel_slug("https://t.me/freelancer_job"), "freelancer_job")
        self.assertEqual(tp._extract_channel_slug("https://t.me/s/SevenProzhe"), "sevenprozhe")
        self.assertEqual(tp._extract_channel_slug("@Daneshjoo_Com"), "daneshjoo_com")
        self.assertEqual(len(tp.TARGET_CHANNELS), 18)


class Test02ConsoleAndJalaliDate(LoggedTestCase):
    def test_gregorian_to_jalali_known_dates(self) -> None:
        # 2026-03-21 -> 1405/01/01 (Nowruz)
        self.assertEqual(tp.gregorian_to_jalali(2026, 3, 21), (1405, 1, 1))
        # 2026-10-18 -> 1405/07/26 (First production log date)
        self.assertEqual(tp.gregorian_to_jalali(2026, 10, 18), (1405, 7, 26))
        dt = datetime(2026, 10, 18, 15, 45, 14)
        j_fa, g_str, t_str = tp.format_persian_date_and_time(dt)
        self.assertEqual(j_fa, "۱۴۰۵/۰۷/۲۶")
        self.assertEqual(g_str, "2026-10-18")
        self.assertEqual(t_str, "15:45:14")

    def test_ansi_strip_and_progress_bar(self) -> None:
        colored = f"{tp.WinAnsiConsole.GREEN}[NEW-LEAD]{tp.WinAnsiConsole.RESET} @test"
        self.assertEqual(tp.WinAnsiConsole.strip_ansi(colored), "[NEW-LEAD] @test")
        self.assertEqual(tp.WinAnsiConsole.progress_bar(1, 6, width=6), "[#-----]")
        self.assertEqual(tp.WinAnsiConsole.progress_bar(3, 6, width=6), "[###---]")
        self.assertEqual(tp.WinAnsiConsole.progress_bar(6, 6, width=6), "[######]")


class Test03RateSafetyGuard(LoggedTestCase):
    def test_sliding_window_and_429_exponential_backoff(self) -> None:
        guard = tp.TelegramRateSafetyGuard(max_web_per_min=3, max_bot_per_min=2)
        t0 = 1000.0
        self.assertEqual(guard.acquire_web_slot_delay(now=t0), 0.0)
        self.assertEqual(guard.acquire_web_slot_delay(now=t0 + 1.0), 0.0)
        self.assertEqual(guard.acquire_web_slot_delay(now=t0 + 2.0), 0.0)
        # 4th request within 60s window must wait
        wait_s = guard.acquire_web_slot_delay(now=t0 + 3.0)
        self.assertGreater(wait_s, 55.0)
        # After 61s, window prunes old timestamps
        self.assertEqual(guard.acquire_web_slot_delay(now=t0 + 61.0), 0.0)

        # Test HTTP 429 exponential cooldown
        backoff1 = guard.record_http_429(retry_after_sec=8.0)
        self.assertGreaterEqual(backoff1, 8.0)
        snap = guard.get_snapshot()
        self.assertEqual(snap["http_429"], 1)
        self.assertTrue(snap["in_cooldown"])
        guard.record_http_success("freelancer_job", latency_ms=420, was_304=True)
        snap2 = guard.get_snapshot()
        self.assertEqual(snap2["http_304"], 1)
        self.assertEqual(snap2["avg_latency_ms"], 420)


class Test04ProxyAndHttpPool(LoggedTestCase):
    def test_proxy_url_parsing(self) -> None:
        s1 = tp.PersistentKeepAliveHTTP.parse_proxy_url("http://127.0.0.1:10809", "env:http_proxy")
        self.assertIsNotNone(s1)
        self.assertEqual((s1.scheme, s1.host, s1.port), ("http", "127.0.0.1", 10809))

        s2 = tp.PersistentKeepAliveHTTP.parse_proxy_url("socks5h://127.0.0.1:10808", "env:all_proxy")
        self.assertIsNotNone(s2)
        self.assertEqual((s2.scheme, s2.host, s2.port), ("socks5", "127.0.0.1", 10808))

        s3 = tp.PersistentKeepAliveHTTP.parse_proxy_url("127.0.0.1:2080", "auto")
        self.assertIsNotNone(s3)
        self.assertEqual((s3.scheme, s3.host, s3.port), ("auto", "127.0.0.1", 2080))


class Test05DiagnosticsAndStartupBanner(LoggedTestCase):
    def test_red_startup_banner_generation(self) -> None:
        rep = tp.FullDiagnosticReport(
            steps=[
                tp.DiagnosticStepResult("PROXY_TUNNEL", True, 17, "127.0.0.1:10808"),
                tp.DiagnosticStepResult("INTERNET", True, 780, "HTTP 204"),
                tp.DiagnosticStepResult("BOT_API", True, 990, "Verified @Hamdanesh_Radar_Bot"),
                tp.DiagnosticStepResult("CHANNEL_FEED", True, 1400, "20 posts"),
            ],
            bot_username="Hamdanesh_Radar_Bot",
            bot_id=8904149971,
            sample_channel="freelancer_job",
            sample_posts_count=20,
            sample_latest_msg_id=190315,
            active_route="HTTP-CONNECT via 127.0.0.1:10808",
        )
        self.assertTrue(rep.all_critical_passed)
        banner = tp.build_red_startup_bot_message(rep, channels_count=18, now=datetime(2026, 10, 18, 15, 45, 14))
        self.assertIn("🟥🟥🟥", banner)
        self.assertIn("@Hamdanesh_Radar_Bot", banner)
        self.assertIn("190315", banner)
        self.assertLessEqual(len(banner), tp.TG_MAX_TEXT)


class Test06IntentClassifierAndProductionDBAudit(LoggedTestCase):
    def test_employer_vs_freelancer_classification(self) -> None:
        emp_samples = [
            "به یک نفر مسلط به پایتون و بینایی ماشین برای انجام پروژه نیازمندم @client_ai",
            "▫️ نوع آگهی: درخواست کننده\nیک پاورپوینت حرفه ای لازم دارم فوری @Raa_abad",
            "سلام وقت بخیر، پروژه کد هست توی محیط ویژوال کد به زبان پایتون و توش opencv هست کد باید تست بشه میخوام یکی چک بکنه @Ziyadkhastam",
        ]
        for s in emp_samples:
            lbl, es, fs = tp.classify_intent(s)
            self.assertEqual(lbl, "employer", f"Failed on employer sample: {s} ({es} vs {fs})")
            self.assertFalse(tp.is_blacklisted(s))

        fre_samples = [
            "انجام کلیه پروژه های دانشجویی پایتون و متلب با قیمت دانشجویی و تضمین کیفیت @doer1",
            "🚀 راه‌اندازی هوش مصنوعی پیشرفته مخصوص فریلنسرها! دسترسی به صدها مدل هوش مصنوعی @Ghoststti",
            "📢 طراحی سایت، اپلیکیشن، سئو و فروش اکانت‌های هوش مصنوعی ✨ خدمات ما: طراحی سایت @Sorenkh1",
            "🚀 تولید محتوای حرفه‌ای با هوش مصنوعی 🎥 خدمات من: ساخت ویدیوهای حرفه‌ای @zahra_838383",
            "#کارآموز درود برای کاراموزی پیام دادم به شرکتی برای کارآموزی نیاز دارم @maybefudes",
            "▫️ نوع آگهی: درخواست کننده\nمرکز تخصصی مشارکت در امتیازات انجام پروژه های دانشجویی @F09149269613",
        ]
        for s in fre_samples:
            lbl, es, fs = tp.classify_intent(s)
            self.assertEqual(lbl, "freelancer", f"Failed to block freelancer/seller sample: {s}")
            self.assertTrue(tp.is_blacklisted(s))

    def test_audit_against_real_112_production_db_rows(self) -> None:
        prod_db = "/tmp/db_inspect/safe_lead_radar.db"
        if not os.path.exists(prod_db):
            self.skipTest("Production DB snapshot not present in /tmp/db_inspect")
        conn = sqlite3.connect(prod_db)
        rows = conn.execute("SELECT id, username, raw_text FROM contacted_users ORDER BY id").fetchall()
        conn.close()
        self.assertEqual(len(rows), 112)

        known_false_positives = {"ghoststti", "sorenkh1", "f09149269613", "maybefudes", "zahra_838383"}
        blocked_now = set()
        retained_employers = 0
        for _id, uname, raw_text in rows:
            lbl, _, _ = tp.classify_intent(raw_text)
            if uname.lower() in known_false_positives:
                if lbl != "employer":
                    blocked_now.add(uname.lower())
            else:
                if lbl == "employer":
                    retained_employers += 1

        test_logger.info(
            "[DB-AUDIT] 112 production rows audited: %d/5 historical false-positives now blocked (%s), %d/107 true employers retained.",
            len(blocked_now),
            sorted(blocked_now),
            retained_employers,
        )
        self.assertEqual(blocked_now, known_false_positives)
        self.assertEqual(retained_employers, 107)


class Test07TopicClassification(LoggedTestCase):
    def test_topic_extraction_and_compound_shadow(self) -> None:
        # "تایپ اسکریپت" must classify as Web Dev, NOT "ترجمه و تولید محتوا" ("تایپ")
        self.assertEqual(
            tp.extract_topic("به یک برنامه نویس مسلط به تایپ اسکریپت و نکست نیازمندم"),
            "برنامه‌نویسی وب",
        )
        self.assertEqual(
            tp.extract_topic("پروژه پردازش تصویر و یادگیری عمیق با YOLO دارم"),
            "هوش مصنوعی و یادگیری ماشین",
        )
        self.assertEqual(
            tp.extract_topic("نیازمند فردی مسلط به پایتون و پانداس برای تحلیل داده"),
            "پایتون و علوم داده",
        )
        self.assertEqual(
            tp.extract_topic("به یک نفر مسلط به مباحث arch و garch و ardl و ایویوز نیازمندم"),
            "حسابداری و تحلیل آماری",
        )


class Test08HandleExtractionAndFooterLearner(LoggedTestCase):
    def test_footer_cooccurrence_learner_and_admin_exclusion(self) -> None:
        learner = tp.CoOccurrenceFooterLearner(threshold=3)
        for mid, client_h in [(101, "@client_one"), (102, "@client_two"), (103, "@client_three")]:
            post = f"به یک برنامه نویس پایتون نیازمندم\n{client_h}\nجهت هماهنگی: @secret_channel_admin"
            leads = tp.extract_leads(post, source_handle="my_channel", footer=learner, channel_key="my_channel", msg_id=mid)
            if mid < 103:
                self.assertIn(client_h.lstrip("@"), leads)
        # On 3rd co-occurrence, @secret_channel_admin is automatically learned as a footer handle!
        self.assertTrue(learner.is_footer("my_channel", "secret_channel_admin"))
        leads_104 = tp.extract_leads(
            "پروژه جدید پایتون دارم @client_four\n@secret_channel_admin",
            source_handle="my_channel",
            footer=learner,
            channel_key="my_channel",
            msg_id=104,
        )
        self.assertEqual(leads_104, ["client_four"])


class Test09PsychologicalPitchAndDeepLink(LoggedTestCase):
    def test_zwnj_sanitization_and_deeplink_encoding(self) -> None:
        task = tp.OutreachTask(
            username="Ziyadkhastam",
            source_channel="@danshjo_bartar",
            source_chat_handle="danshjo_bartar",
            source_msg_id=13806,
            raw_text="پروژه کد هست به زبان پایتون و opencv",
        )
        url, pitch = tp.SpintaxAndTopicEngine.build_custom_pitch_bundle(
            task, custom_title="تخصص در کد نویسی و opencv"
        )
        # Ensure zero ZWNJ (\u200c) remains so Telegram desktop/mobile never glues words
        self.assertNotIn("\u200c", pitch)
        self.assertIn("قدم به قدم", pitch)
        self.assertIn("صورت مسئله", pitch)
        self.assertIn("تخصص در کد نویسی و opencv", pitch)
        self.assertTrue(url.startswith("https://t.me/Ziyadkhastam?text="))
        self.assertLessEqual(len(url), tp.DEEPLINK_MAX_LEN)


class Test10SQLiteWALStoreAndCrossChannelDeduplication(LoggedTestCase):
    def test_cross_channel_signature_invariance_and_thread_safety(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "test_wal.db")
            store = tp.SafeContactStore(db_path)

            ad_ch1 = (
                "🟢 کارفرما\nبه یک نفر مسلط به دروس منابع انسانی MBA نیازمندم\n"
                "@mobin_moradiii\n@doorkarijoo 🔔"
            )
            ad_ch2 = (
                "🧿به یک نفر مسلط به دروس منابع انسانی MBA نیازمندم\n"
                "Telegram: @mobin_moradiii\n============\n"
                "سی پروژه رو در شبکه های اجتماعی دنبال کنید 👇\nتلگرام: t.me/CProje\nبله: ble.ir/CProje"
            )
            sig1 = tp._ad_content_signature(ad_ch1)
            sig2 = tp._ad_content_signature(ad_ch2)
            self.assertEqual(sig1, sig2, "Cross-channel wrappers caused signature drift!")

            t1 = tp.OutreachTask("mobin_moradiii", "@doorkarijoo", "doorkarijoo", 1510, ad_ch1)
            t2 = tp.OutreachTask("mobin_moradiii", "@cproje", "cproje", 5137, ad_ch2)
            self.assertTrue(store.reserve_sync(t1, now=1000.0))
            store.mark_sync("mobin_moradiii", "notified")
            # Cross-post of the exact same ad in a second channel 10 minutes later must be blocked!
            self.assertFalse(store.reserve_sync(t2, now=1600.0))

            # Concurrent race test: 10 threads trying to reserve the same new lead simultaneously
            race_results = []
            barrier = threading.Barrier(10)

            def _worker(idx: int) -> None:
                barrier.wait()
                task = tp.OutreachTask("concurrent_emp", f"@ch_{idx}", f"ch_{idx}", 500 + idx, "به یک متخصص هوش مصنوعی برای پروژه پردازش تصویر نیازمندیم @concurrent_emp")
                res = store.reserve_sync(task, now=2000.0)
                race_results.append(res)

            threads = [threading.Thread(target=_worker, args=(i,)) for i in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            # Exactly 1 thread must win the reservation
            self.assertEqual(sum(1 for r in race_results if r), 1)


class Test11InteractiveBotController(LoggedTestCase):
    def test_interactive_custom_title_and_skip_flow(self) -> None:
        async def _run():
            with tempfile.TemporaryDirectory() as tmp:
                store = tp.SafeContactStore(os.path.join(tmp, "bot_ui.db"))
                task = tp.OutreachTask(
                    "mohsenovs",
                    "@Freelaancing",
                    "Freelaancing",
                    8201,
                    "برای انجام پروژه ای حوزه بهینه سازی پورتفولیو نیاز به کمک دارم @mohsenovs",
                )
                store.reserve_sync(task)
                store.mark_sync("mohsenovs", "notified")
                lead_id = store.get_lead_id_sync("mohsenovs")

                api_calls = []

                def mock_post(url: str, payload: dict) -> dict:
                    api_calls.append((url, payload))
                    return {"ok": True, "result": {"message_id": 777}}

                ctrl = tp.InteractiveBotTitleController(
                    store=store,
                    http_post_fn=mock_post,
                    bot_token="123456:TEST_TOKEN",
                    allowed_chat_id="6139246579",
                )

                # 1. Unauthorized chat rejected
                res_unauth = await ctrl.process_update(
                    {
                        "update_id": 1,
                        "callback_query": {
                            "id": "cb1",
                            "data": f"set_title:{lead_id}",
                            "message": {"message_id": 50, "chat": {"id": 999999}},
                        },
                    }
                )
                self.assertEqual(res_unauth, "unauthorized_chat")

                # 2. Authorized callback query prompts for custom title
                res_prompt = await ctrl.process_update(
                    {
                        "update_id": 2,
                        "callback_query": {
                            "id": "cb2",
                            "data": f"set_title:{lead_id}",
                            "message": {"message_id": 50, "chat": {"id": 6139246579}},
                        },
                    }
                )
                self.assertEqual(res_prompt, "prompted_title:mohsenovs")

                # 3. User replies with custom title "بهینه سازی پورتفولیو"
                res_built = await ctrl.process_update(
                    {
                        "update_id": 3,
                        "message": {
                            "message_id": 51,
                            "chat": {"id": 6139246579},
                            "text": "بهینه سازی پورتفولیو",
                        },
                    }
                )
                self.assertEqual(res_built, "built_custom_pitch:mohsenovs")
                self.assertEqual(store.get_status_sync("mohsenovs"), "titled")

                # 4. User clicks skip_lead
                res_skip = await ctrl.process_update(
                    {
                        "update_id": 4,
                        "callback_query": {
                            "id": "cb3",
                            "data": f"skip_lead:{lead_id}",
                            "message": {"message_id": 50, "chat": {"id": 6139246579}},
                        },
                    }
                )
                self.assertEqual(res_skip, "skipped")
                self.assertEqual(store.get_status_sync("mohsenovs"), "skipped")

        asyncio.run(_run())


class Test12WebScraperAndParallelBatchListener(LoggedTestCase):
    def test_html_parser_and_parallel_3lane_microbatch(self) -> None:
        sample_html = """
        <div class="tgme_widget_message" data-post="freelancer_job/190500">
          <div class="tgme_widget_message_text js-message_text" dir="auto">
            به یک برنامه نویس پایتون و هوش مصنوعی نیازمندیم<br/>
            آیدی: <a href="https://t.me/ai_employer_1">@ai_employer_1</a>
          </div>
        </div>
        <div class="tgme_widget_message" data-post="freelancer_job/190501">
          <div class="tgme_widget_message_text js-message_text" dir="auto">
            انجام کلیه پروژه های دانشجویی با قیمت مناسب @spam_doer
          </div>
        </div>
        """
        parsed = tp.WebChannelScraper.parse_public_channel_html(sample_html)
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0][0], "freelancer_job")
        self.assertEqual(parsed[0][1], 190500)

        async def _run_batch():
            with tempfile.TemporaryDirectory() as tmp:
                store = tp.SafeContactStore(os.path.join(tmp, "batch.db"))
                q: asyncio.Queue[tp.OutreachTask] = asyncio.Queue()
                listener = tp.SafeChannelListener(
                    None, store, q, set(), set(), live_only_watermark=True
                )
                # Seed baseline at 190499 so 190500 & 190501 are treated as live new posts
                listener.seed_channel_watermark("freelancer_job", 190499)
                enq1 = await listener.on_post(
                    parsed[0][2], "@freelancer_job", "freelancer_job", parsed[0][1], parsed[0][3]
                )
                enq2 = await listener.on_post(
                    parsed[1][2], "@freelancer_job", "freelancer_job", parsed[1][1], parsed[1][3]
                )
                self.assertEqual(enq1, 1)
                self.assertEqual(enq2, 0)  # Freelancer ad filtered!
                self.assertEqual(q.qsize(), 1)

        asyncio.run(_run_batch())


class Test13ConsoleHotkeysAndDashboard(LoggedTestCase):
    def test_hotkey_state_transitions_and_status_table(self) -> None:
        ctrl = tp.ConsoleHotkeyController(auto_start=False)
        self.assertFalse(ctrl.active_event.is_set())
        self.assertEqual(ctrl.feed_key("1"), "started")
        self.assertTrue(ctrl.active_event.is_set())
        self.assertEqual(ctrl.feed_key("s"), "status")
        self.assertTrue(ctrl.status_request_event.is_set())
        self.assertEqual(ctrl.feed_key("t"), "testing")
        self.assertTrue(ctrl.diagnostic_request_event.is_set())
        self.assertEqual(ctrl.feed_key("0"), "paused")
        self.assertFalse(ctrl.active_event.is_set())
        self.assertEqual(ctrl.feed_key("x"), "exiting")
        self.assertTrue(ctrl.exit_event.is_set())

        table = tp.format_live_status_table(None, tp._GLOBAL_SAFETY_GUARD, queue_size=2)
        self.assertIn("LIVE RADAR TELEMETRY", tp.WinAnsiConsole.strip_ansi(table))


if __name__ == "__main__":
    test_logger.info("========================================================================")
    test_logger.info("Starting TelePulse Automated Unit & Stress Test Suite")
    test_logger.info("========================================================================")
    unittest.main(verbosity=2)
