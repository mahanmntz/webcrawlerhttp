#!/usr/bin/env python3
"""
SpiderRAG - Interactive Terminal UI & Microservice Supervisor
Monitors distributed Go, Python, and Fastify workers with real-time telemetry,
collapsible verbose logs, interactive URL/File ingestion, and queue flushing.
"""

import sys
import os
import time
import signal
import subprocess
import threading
import select
import tty
import termios
import json
import argparse
import re
from urllib.parse import urlparse
from datetime import datetime
from collections import deque

import redis
from rich import box
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.layout import Layout
from rich.text import Text

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:3000")

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
VENV_PYTHON = os.path.join(ROOT_DIR, "services", "parser-scraper", ".venv", "bin", "python")
if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = sys.executable

console = Console()

class Supervisor:
    def __init__(self, target_url=None, target_file=None, flush_on_start=False, max_depth=2, stay_in_domain=True, max_pages=None, lang_filter=None):
        self.target_url = target_url
        self.target_file = target_file
        self.flush_on_start = flush_on_start
        self.max_depth = max_depth
        self.stay_in_domain = stay_in_domain
        self.max_pages = max_pages
        self.lang_filter = [l.strip().lower() for l in lang_filter.split(",")] if lang_filter else None
        self.start_time = time.time()
        self.mission_complete = False
        self.completion_time = None
        self.saved_md_path = None
        self.paused = False

        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.initial_doc_count = 0
        try:
            self.initial_doc_count = int(self.r.hget("stats:totals", "documents") or 0)
        except Exception:
            pass

        self.processes = {}
        self.logs = deque(maxlen=200)
        self.show_logs = False
        self.running = True
        self.lock = threading.Lock()
        self.status_msg = "Initializing SpiderRAG..."

    def log(self, tag: str, message: str):
        with self.lock:
            ts = datetime.now().strftime("%H:%M:%S")
            self.logs.append(f"[{ts}] [{tag}] {message}")

    def flush_redis(self):
        try:
            # Keep in sync with POST /api/cluster/reset in the gateway.
            keys = [
                "frontier:queue", "frontier:processing", "frontier:hosts",
                "frontier:scheduled", "frontier:leases",
                "frontier:delayed", "frontier:dead", "frontier:redeliveries",
                "frontier:seen", "frontier:bloom:url",
                "queue:raw_pages", "queue:raw_pages:processing", "queue:raw_pages:leases",
                "queue:raw_pages:dead", "queue:raw_pages:redeliveries",
                "queue:parsed_docs", "content:seen", "stats:totals",
            ]
            self.r.unlink(*keys)
            for pattern in ["job:*", "frontier:host:*", "raw_page:*"]:
                # SCAN instead of KEYS so a large keyspace doesn't block Redis.
                matched = list(self.r.scan_iter(match=pattern, count=500))
                if matched:
                    self.r.unlink(*matched)
            self.initial_doc_count = 0
            self.status_msg = "🧹 All Redis queues & state successfully flushed!"
            self.log("SYSTEM", "Redis state and queues flushed cleanly.")
        except Exception as e:
            self.status_msg = f"❌ Error flushing Redis: {e}"
            self.log("ERROR", f"Flush error: {e}")

    def pause_crawling(self):
        """Freezes crawl engine workers from pulling new jobs from the frontier."""
        self.paused = True
        try:
            self.r.set("crawler:paused", "1")
        except Exception:
            pass
        self.status_msg = "⏸️ Crawl paused! Workers are standing by. Press [C] to continue crawling more pages."
        self.log("SYSTEM", "Crawler engine paused at target limit.")

    def resume_crawling(self, additional_pages: int = 50):
        """Resumes crawl engine workers and raises target page limit."""
        self.paused = False
        self.mission_complete = False
        self.completion_time = None
        try:
            self.r.delete("crawler:paused")
        except Exception:
            pass
        if self.max_pages is not None:
            self.max_pages += additional_pages
        else:
            self.max_pages = additional_pages
        self.status_msg = f"▶️ Resumed crawling! New target limit: {self.max_pages} pages."
        self.log("SYSTEM", f"Crawler resumed. Target extended by +{additional_pages} to {self.max_pages} pages.")

    def recrawl_target(self):
        """Flushes Redis cache and queues, and enqueues the target URL again cleanly from scratch."""
        self.flush_redis()
        self.mission_complete = False
        self.completion_time = None
        self.start_time = time.time()
        self.paused = False
        try:
            self.r.delete("crawler:paused")
        except Exception:
            pass
        if self.target_url:
            self.status_msg = f"🔄 Re-crawling target from scratch: {self.target_url}"
            self.log("SYSTEM", f"Re-crawling target URL: {self.target_url}")
            self.enqueue_url(self.target_url)
        elif self.target_file:
            self.status_msg = f"🔄 Re-crawling seeds file: {self.target_file}"
            self.log("SYSTEM", f"Re-crawling target file: {self.target_file}")
            self.enqueue_file(self.target_file)
        else:
            self.status_msg = "🧹 Redis flushed! Ready for next mission."

    def enqueue_url(self, url: str):
        import urllib.request
        import urllib.error

        payload = json.dumps({
            "url": url,
            "max_depth": self.max_depth,
            "priority": 5,
            "stay_in_domain": self.stay_in_domain,
            "force": True
        }).encode('utf-8')

        for attempt in range(12):
            try:
                req = urllib.request.Request(
                    f"{GATEWAY_URL}/api/jobs",
                    data=payload,
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    if resp.status in (200, 201):
                        self.status_msg = f"🚀 Enqueued to Go Frontier: {url}"
                        self.log("GATEWAY", f"Target URL enqueued successfully: {url}")
                        return
            except urllib.error.HTTPError as e:
                if e.code == 409:
                    self.status_msg = f"♻️ URL already seen in Redis (Bloom). Press [F] to flush & recrawl."
                    self.log("GATEWAY", f"Seed already in Redis: {url}")
                    return
                elif e.code == 400:
                    self.status_msg = f"❌ Invalid URL: {url}"
                    self.log("ERROR", f"Invalid URL: {url}")
                    return
                time.sleep(0.75)
            except Exception:
                time.sleep(0.75)

        self.status_msg = f"⚠️ Could not reach Gateway to enqueue: {url}"
        self.log("WARN", f"Failed to enqueue {url} after 12 retries")

    def enqueue_file(self, file_path: str):
        import urllib.request
        import urllib.error

        full_path = os.path.abspath(file_path)
        if not os.path.exists(full_path):
            self.status_msg = f"❌ File not found: {file_path}"
            return
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
            
            payload = json.dumps({
                "urls": urls,
                "max_depth": self.max_depth,
                "priority": 5,
                "stay_in_domain": self.stay_in_domain,
                "force": True
            }).encode('utf-8')

            for attempt in range(12):
                try:
                    req = urllib.request.Request(
                        f"{GATEWAY_URL}/api/jobs/batch",
                        data=payload,
                        headers={"Content-Type": "application/json"}
                    )
                    with urllib.request.urlopen(req, timeout=4) as resp:
                        res = json.loads(resp.read().decode('utf-8'))
                        self.status_msg = f"📁 Batch enqueued {res.get('enqueued_count', 0)} URLs ({res.get('deduplicated_count', 0)} deduplicated)"
                        self.log("GATEWAY", self.status_msg)
                        return
                except urllib.error.HTTPError as e:
                    if e.code == 400:
                        self.status_msg = f"❌ Bad batch payload: {e}"
                        return
                    time.sleep(0.75)
                except Exception:
                    time.sleep(0.75)
            self.status_msg = f"⚠️ Could not reach Gateway for batch file"
        except Exception as e:
            self.status_msg = f"❌ Batch enqueue error: {e}"
            self.log("ERROR", str(e))

    @staticmethod
    def url_to_slug(url: str) -> str:
        try:
            parsed = urlparse(url)
            slug = (parsed.netloc + parsed.path).strip("/")
            slug = re.sub(r'[^a-zA-Z0-9_\-]', '_', slug).strip('_')
            return slug[:80] if slug else "scraped_document"
        except Exception:
            return "scraped_document"

    def auto_save_documents(self) -> str:
        try:
            docs_raw = self.r.lrange("queue:parsed_docs", 0, -1)
            if not docs_raw:
                return ""

            output_dir = os.path.join(ROOT_DIR, "output")
            os.makedirs(output_dir, exist_ok=True)

            docs = []
            for item in docs_raw:
                try:
                    docs.append(json.loads(item))
                except (json.JSONDecodeError, Exception):
                    pass

            if not docs:
                return ""

            # Apply Language Filter if specified (e.g. only 'fa' or 'en')
            if self.lang_filter:
                from app.extractor import detect_language
                from bs4 import BeautifulSoup
                filtered_docs = []
                for d in docs:
                    lang = d.get("language")
                    if not lang:
                        # Fallback heuristic detection on markdown/title
                        lang = detect_language(BeautifulSoup(d.get("markdown", "")[:500], "html.parser"), d.get("markdown", "")[:500])
                    if lang in self.lang_filter or not lang:
                        filtered_docs.append(d)
                if filtered_docs:
                    docs = filtered_docs

            slug = self.url_to_slug(self.target_url) if self.target_url else "scraped_corpus"

            # Create dedicated directory per website: output/<slug>/
            site_dir = os.path.join(output_dir, slug)
            os.makedirs(site_dir, exist_ok=True)

            # Helper to generate rich YAML Frontmatter
            def make_frontmatter(d: dict) -> str:
                d_url = d.get("url", "")
                d_title = (d.get("title") or "Untitled Document").replace('"', '\\"')
                d_desc = (d.get("meta_description") or "").replace('"', '\\"')
                d_tokens = d.get("estimated_tokens", 0)
                d_savings = d.get("token_savings_pct", 0)
                d_lang = d.get("language", "en")
                meta_extra = d.get("metadata", {})
                
                fm_lines = [
                    "---",
                    f"url: {d_url}",
                    f"title: \"{d_title}\"",
                    f"description: \"{d_desc}\"",
                    f"language: {d_lang}",
                    f"estimated_tokens: {d_tokens}",
                    f"token_savings: -{d_savings}%",
                    f"parsed_at: {d.get('parsed_at', '')}",
                ]
                if isinstance(meta_extra, dict):
                    for k, v in meta_extra.items():
                        clean_v = str(v).replace('"', '\\"')
                        fm_lines.append(f"{k}: \"{clean_v}\"")
                fm_lines.append("---\n\n")
                return "\n".join(fm_lines)

            # 1. Single Page scrape: write exactly 1 clean markdown file inside output/<slug>/
            if len(docs) == 1:
                doc = docs[0]
                markdown = doc.get("markdown", "")
                out_path = os.path.join(site_dir, f"{slug}.md")
                frontmatter = make_frontmatter(doc)
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(frontmatter + markdown)
            else:
                # 2. Multi-page / Recursive crawl: bundle ALL pages into ONE unified knowledge base!
                out_path = os.path.join(site_dir, f"{slug}_combined.md")
                total_tokens = sum(d.get("estimated_tokens", 0) for d in docs)

                lines = [
                    f"# 📚 SpiderRAG Unified Knowledge Base: {self.target_url or 'Crawl Corpus'}",
                    f"- **Total Documents:** {len(docs)}",
                    f"- **Total Estimated LLM Tokens:** ~{total_tokens:,}",
                    f"- **Generated At:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "\n## 📑 Table of Contents",
                ]
                for idx, d in enumerate(docs, 1):
                    d_title = d.get("title") or f"Document {idx}"
                    lines.append(f"{idx}. [{d_title}](#doc-{idx}) — `{d.get('url', '')}`")

                lines.append("\n---\n")

                for idx, d in enumerate(docs, 1):
                    d_title = d.get("title") or f"Document {idx}"
                    d_url = d.get("url", "")
                    d_tokens = d.get("estimated_tokens", 0)
                    d_savings = d.get("token_savings_pct", 0)
                    d_md = d.get("markdown", "")
                    fm = make_frontmatter(d)

                    lines.append(f'<a name="doc-{idx}"></a>')
                    lines.append(f"## 📄 [{idx}/{len(docs)}] {d_title}")
                    lines.append(f"> 🔗 **URL:** {d_url}  |  🧠 **Tokens:** ~{d_tokens:,}  |  📉 **Savings:** -{d_savings}%\n")
                    lines.append(fm)
                    lines.append(d_md)
                    lines.append("\n\n---\n")

                with open(out_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(lines))

            # 3. Export RAG Semantic Chunks (_chunks.json) for Vector DBs
            from app.extractor import chunk_markdown
            all_chunks = []
            for d in docs:
                chunks = chunk_markdown(
                    markdown=d.get("markdown", ""),
                    doc_url=d.get("url", ""),
                    doc_title=d.get("title", ""),
                    max_tokens=400,
                    overlap_tokens=50
                )
                all_chunks.extend(chunks)

            chunks_path = os.path.join(site_dir, f"{slug}_chunks.json")
            with open(chunks_path, "w", encoding="utf-8") as f:
                json.dump({
                    "total_chunks": len(all_chunks),
                    "chunk_size_tokens": 400,
                    "overlap_tokens": 50,
                    "chunks": all_chunks
                }, f, indent=2, ensure_ascii=False)

            # 4. Also save ONE single consolidated JSON corpus file in output/<slug>/
            json_path = os.path.join(site_dir, f"{slug}_corpus.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump({"total_count": len(docs), "documents": docs}, f, indent=2, ensure_ascii=False)

            self.saved_md_path = os.path.relpath(out_path, ROOT_DIR)
            return out_path
        except Exception as e:
            self.log("ERROR", f"Auto-save error: {e}")
            return ""

    def export_documents(self):
        try:
            saved = self.auto_save_documents()
            if saved:
                self.status_msg = f"💾 Consolidated all documents into: {self.saved_md_path}"
                self.log("EXPORT", f"Saved consolidated knowledge base to {saved}")
            else:
                self.status_msg = "⚠️ No documents found to export."
        except Exception as e:
            self.status_msg = f"❌ Export error: {e}"
            self.log("ERROR", f"Export failed: {e}")

    def _stream_process_output(self, name: str, proc: subprocess.Popen):
        for line in iter(proc.stdout.readline, ''):
            if not self.running:
                break
            clean_line = line.strip()
            if clean_line:
                self.log(name, clean_line)
        proc.stdout.close()

    def start_processes(self):
        # 1. API Gateway
        self.log("SYSTEM", "Starting API Gateway (Node.js/Fastify)...")
        p_gateway = subprocess.Popen(
            ["npm", "--prefix", "services/api-gateway", "run", "dev"],
            cwd=ROOT_DIR,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        self.processes["GATEWAY"] = p_gateway
        threading.Thread(target=self._stream_process_output, args=("GATEWAY", p_gateway), daemon=True).start()

        # 2. Python Parser
        self.log("SYSTEM", "Starting Parser & Scraper (Python)...")
        p_parser = subprocess.Popen(
            [VENV_PYTHON, "services/parser-scraper/parser_worker.py"],
            cwd=ROOT_DIR,
            env=dict(os.environ, PYTHONPATH="services/parser-scraper"),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        self.processes["PARSER"] = p_parser
        threading.Thread(target=self._stream_process_output, args=("PARSER", p_parser), daemon=True).start()

        # 3. Go Crawler Engine
        self.log("SYSTEM", "Starting Downloader Engine (Go)...")
        p_crawler = subprocess.Popen(
            ["go", "run", "./services/crawler-engine"],
            cwd=ROOT_DIR,
            env=dict(os.environ, REDIS_ADDR=f"{REDIS_HOST}:{REDIS_PORT}", WORKER_COUNT="3"),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        self.processes["CRAWLER"] = p_crawler
        threading.Thread(target=self._stream_process_output, args=("CRAWLER", p_crawler), daemon=True).start()

    def stop_processes(self):
        self.running = False
        self.status_msg = "🛑 Stopping all microservices gracefully..."
        for name, proc in self.processes.items():
            try:
                proc.send_signal(signal.SIGINT)
            except Exception:
                pass
        time.sleep(0.8)
        for name, proc in self.processes.items():
            try:
                proc.terminate()
            except Exception:
                pass

    def get_metrics(self):
        try:
            # Ingest list plus targets already routed to per-host queues.
            pending = self.r.llen("frontier:queue") + int(self.r.get("frontier:scheduled") or 0)
            in_flight = self.r.llen("frontier:processing")
            parsed_count = int(self.r.hget("stats:totals", "documents") or 0)
            
            # Unique URLs seen: exact Set fallback plus RedisBloom, if loaded.
            seen = self.r.scard("frontier:seen")
            try:
                info = self.r.execute_command("BF.INFO", "frontier:bloom:url")
                if isinstance(info, (list, tuple)):
                    for i in range(0, len(info) - 1, 2):
                        if "inserted" in str(info[i]).lower():
                            seen += int(info[i + 1])
                            break
            except Exception:
                pass

            # Running totals maintained by the parser for every document.
            totals = self.r.hgetall("stats:totals")
            raw_bytes = int(totals.get("raw_html_bytes", 0))
            md_bytes = int(totals.get("markdown_bytes", 0))
            est_tokens = int(totals.get("markdown_tokens", 0))

            # Memory from Redis
            try:
                mem_info = self.r.info("memory")
                redis_ram = mem_info.get("used_memory_human", "N/A")
            except Exception:
                redis_ram = "N/A"

            elapsed = max(1.0, time.time() - self.start_time)
            speed_pages = f"{parsed_count / elapsed:.1f} doc/s" if parsed_count > 0 else "0.0 doc/s"

            def fmt_bytes(b):
                if b >= 1024 * 1024:
                    return f"{b / (1024*1024):.2f} MB"
                elif b >= 1024:
                    return f"{b / 1024:.1f} KB"
                return f"{b} B"

            raw_str = fmt_bytes(raw_bytes)
            md_str = fmt_bytes(md_bytes)

            session_parsed = max(0, parsed_count - self.initial_doc_count)

            latest_doc = None
            if session_parsed > 0 or not self.target_url:
                latest_raw = self.r.lindex("queue:parsed_docs", 0)
                if latest_raw:
                    try:
                        latest_doc = json.loads(latest_raw)
                    except json.JSONDecodeError:
                        pass

            savings_pct = round((1 - (md_bytes / raw_bytes)) * 100, 1) if raw_bytes > 0 else 0.0

            # Check completion goal based on new documents scraped in this session
            effective_count = session_parsed if self.target_url else parsed_count
            if self.max_pages and effective_count >= self.max_pages and not self.mission_complete:
                self.mission_complete = True
                self.completion_time = elapsed
                self.pause_crawling()
                saved = self.auto_save_documents()
                if saved:
                    self.saved_md_path = os.path.relpath(saved, ROOT_DIR)
                self.status_msg = f"🎉 TARGET REACHED ({effective_count}/{self.max_pages}) in {elapsed:.1f}s! Workers paused."
                self.log("DONE", self.status_msg)

            if self.mission_complete and self.completion_time is not None:
                elapsed_str = f"{self.completion_time:.1f}s ✓"
            else:
                elapsed_str = f"{int(elapsed)}s"

            return {
                "pending": pending,
                "in_flight": in_flight,
                "parsed": effective_count,
                "seen": seen,
                "savings_pct": savings_pct,
                "est_tokens": est_tokens,
                "raw_str": raw_str,
                "md_str": md_str,
                "redis_ram": redis_ram,
                "speed_pages": speed_pages,
                "elapsed": elapsed_str,
                "latest_doc": latest_doc
            }
        except Exception:
            return {
                "pending": 0, "in_flight": 0, "parsed": 0, "seen": 0,
                "savings_pct": 0.0, "est_tokens": 0, "raw_str": "0 B", "md_str": "0 B",
                "redis_ram": "N/A", "speed_pages": "0.0 doc/s", "elapsed": "0s", "latest_doc": None
            }

    def render(self) -> Layout:
        metrics = self.get_metrics()
        layout = Layout()

        term_height = console.size.height or 24
        header_size = 9 if term_height < 28 else 11

        layout.split_column(
            Layout(name="header", size=header_size),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=3)
        )

        # Header: 2-Row Comprehensive Metrics Grid
        grid = Table.grid(expand=True)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)

        docs_label = f"{metrics['parsed']:,}"
        if self.max_pages:
            docs_label += f" [dim]/ {self.max_pages}[/dim]"

        grid.add_row(
            f"[bold yellow]⏳ Pending[/bold yellow]\n[bold white]{metrics['pending']:,}[/bold white]",
            f"[bold cyan]⚡ In-Flight[/bold cyan]\n[bold white]{metrics['in_flight']} workers[/bold white]",
            f"[bold green]📄 Clean Docs[/bold green]\n[bold white]{docs_label}[/bold white]",
            f"[bold emerald]📉 Token Savings[/bold emerald]\n[bold green]-{metrics['savings_pct']}%[/bold green]",
            f"[bold magenta]🧠 LLM Tokens[/bold magenta]\n[bold white]~{metrics['est_tokens']:,}[/bold white]",
        )
        if term_height >= 28:
            grid.add_row("", "", "", "", "")  # Subtle Row Spacer
        grid.add_row(
            f"[bold blue]🌐 Data Traffic[/bold blue]\n[dim]{metrics['raw_str']} ➔ [bold green]{metrics['md_str']}[/bold green][/dim]",
            f"[bold red]💾 Redis RAM[/bold red]\n[bold white]{metrics['redis_ram']}[/bold white]",
            f"[bold yellow]⚡ Throughput[/bold yellow]\n[bold white]{metrics['speed_pages']}[/bold white]",
            f"[bold cyan]⏱️ Elapsed[/bold cyan]\n[bold white]{metrics['elapsed']}[/bold white]",
            f"[bold white]🎯 Target Mode[/bold white]\n[dim]{'Single Page' if self.max_depth == 0 else 'Recursive Crawl'}[/dim]",
        )

        header_panel = Panel(
            grid,
            title="[bold cyan]🕷️ SpiderRAG | Distributed Web-to-Markdown Engine[/bold cyan]",
            subtitle=f"[dim]Web Dashboard: {GATEWAY_URL} | Redis: {REDIS_HOST}:{REDIS_PORT}[/dim]",
            border_style="cyan"
        )
        layout["header"].update(header_panel)

        # Body: Either latest card or logs
        if self.show_logs:
            log_text = Text()
            with self.lock:
                for line in list(self.logs)[-18:]:
                    if "FAIL" in line or "error" in line.lower():
                        log_text.append(line + "\n", style="bold red")
                    elif "OK" in line or "LLM_READY" in line:
                        log_text.append(line + "\n", style="green")
                    elif "DEDUP" in line:
                        log_text.append(line + "\n", style="yellow")
                    else:
                        log_text.append(line + "\n", style="dim white")

            body_panel = Panel(
                log_text,
                title="[bold yellow]📜 Live Cluster Logs[/bold yellow]",
                subtitle="[bold white][L][/bold white] Collapse Logs  │  [bold white][Q][/bold white] Quit",
                border_style="yellow"
            )
            layout["body"].update(body_panel)
        else:
            latest = metrics.get("latest_doc")
            if latest:
                info_text = (
                    f"[bold white]Title:[/bold white] {latest.get('title', 'N/A')}\n"
                    f"[bold cyan]URL:[/bold cyan] {latest.get('url', 'N/A')}\n"
                    f"[bold green]Tokens:[/bold green] ~{latest.get('estimated_tokens', 0):,}  |  "
                    f"[bold emerald]Payload Savings:[/bold emerald] -{latest.get('token_savings_pct', 0)}%  |  "
                    f"[bold blue]Discovered Links:[/bold blue] {len(latest.get('links', []))}\n\n"
                    f"[dim italic]{(latest.get('markdown') or '')[:250]}...[/dim italic]"
                )
            else:
                info_text = "[dim]No documents scraped yet. Submit a URL mission or wait for Go fetchers...[/dim]"

            status_color = "green" if "successfully" in self.status_msg or "Enqueued" in self.status_msg or "COMPLETE" in self.status_msg else "white"

            if self.mission_complete:
                save_msg = (
                    f"[bold yellow]💾 Unified Output File:[/bold yellow] [bold underline cyan]{self.saved_md_path or 'output/'}[/bold underline cyan]\n"
                    f"[bold green]⏸️ Crawling paused at {metrics['parsed']} pages limit.[/bold green]\n"
                    f"[bold white]👉 Press [bold cyan][C][/bold cyan] to crawl +50 more  │  Press [bold cyan][R][/bold cyan] to re-crawl from scratch  │  Press [bold yellow][Q][/bold yellow] or [bold yellow][Enter][/bold yellow] to finish.[/bold white]\n\n"
                )
                card_title = f"[bold green]🏁 TARGET LIMIT REACHED ({metrics['parsed']} Pages) — [C] Crawl More  [R] Re-crawl  [Q] Exit[/bold green]"
                border_col = "green"
            else:
                save_msg = ""
                card_title = "[bold green]📡 Real-time Extractor Overview[/bold green]"
                border_col = "green"

            panel_subtitle = "[bold white][C][/bold white] More  │  [bold white][R][/bold white] Re-crawl  │  [bold white][Q][/bold white] Quit  │  [bold white][L][/bold white] Logs" if term_height < 26 else None

            body_panel = Panel(
                f"[bold {status_color}]Status:[/bold {status_color}] {self.status_msg}\n\n"
                f"{save_msg}"
                f"[bold underline]Latest Extracted Document:[/bold underline]\n{info_text}",
                title=card_title,
                subtitle=panel_subtitle,
                border_style=border_col
            )
            layout["body"].update(body_panel)

        # Footer Hotkeys
        log_status = "[bold yellow]ON[/bold yellow]" if self.show_logs else "[dim]OFF[/dim]"
        footer_items = []
        if self.mission_complete:
            footer_items.append("[bold cyan]\\[C][/bold cyan] Crawl +50 More")
        footer_items.extend([
            f"[bold cyan]\\[R][/bold cyan] Re-crawl Target",
            f"[bold white]\\[L][/bold white] Toggle Logs ({log_status})",
            f"[bold white]\\[F][/bold white] Flush Queues",
            f"[bold white]\\[E][/bold white] Export JSON",
            f"[bold white]\\[Q][/bold white] Quit Cleanly"
        ])
        footer_text = Text.from_markup("  │  ".join(footer_items))
        layout["footer"].update(Panel(footer_text, border_style="dim", box=box.ROUNDED))

        return layout


def normalize_input(text: str) -> str:
    persian_digits = {'۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4', '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'}
    for p, e in persian_digits.items():
        text = text.replace(p, e)
    return text.strip()


def key_listener(sup: Supervisor):
    # Non-blocking key capture for macOS and Linux
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        while sup.running:
            rlist, _, _ = select.select([sys.stdin], [], [], 0.2)
            if rlist:
                key = sys.stdin.read(1)
                if not key:
                    continue
                k = key.lower()
                # Support both English and Persian layout hotkeys
                if k in ('l', 'م'):
                    sup.show_logs = not sup.show_logs
                elif k in ('c', 'ژ', '+'):
                    # Extend and continue crawling
                    sup.resume_crawling(additional_pages=50)
                elif k in ('r', 'ق'):
                    # Re-crawl target cleanly from scratch
                    sup.recrawl_target()
                elif k in ('p', 'ح'):
                    # Manual pause toggle
                    if sup.paused:
                        sup.resume_crawling(additional_pages=50)
                    else:
                        sup.pause_crawling()
                elif k in ('f', 'ب'):
                    sup.flush_redis()
                elif k in ('e', 'ث'):
                    sup.export_documents()
                elif k in ('q', 'ض', '\x03'):  # Explicit quit only
                    sup.running = False
                    break
                elif k in ('\r', '\n', ' ') and sup.mission_complete:
                    sup.running = False
                    break
    except Exception:
        pass
    finally:
        # Ensure crawler:paused key is cleaned up on exit
        try:
            sup.r.delete("crawler:paused")
        except Exception:
            pass
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


def prompt_user_menu():
    console.clear()
    banner = Text.from_markup(
        """[bold cyan]
  ███████╗██████╗ ██╗██████╗ ███████╗██████╗ ██████╗  █████╗  ██████╗ 
  ██╔════╝██╔══██╗██║██╔══██╗██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝ 
  ███████╗██████╔╝██║██║  ██║█████╗  ██████╔╝██████╔╝███████║██║  ███╗
  ╚════██║██╔═══╝ ██║██║  ██║██╔══╝  ██╔══██╗██╔══██╗██╔══██║██║   ██║
  ███████║██║     ██║██████╔╝███████╗██║  ██║██║  ██║██║  ██║╚██████╔╝
  ╚══════╝╚═╝     ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ 
        Distributed Web-to-Markdown Engine for AI & RAG
[/bold cyan]"""
    )
    console.print(banner)
    console.print("[bold white]Choose your mission mode:[/bold white]\n")
    console.print("  [bold cyan]1[/bold cyan] 📄 Single Page Scrape (Exact 1 URL, depth 0 - clean fresh run)")
    console.print("  [bold cyan]2[/bold cyan] 🌐 Recursive Site Crawl (Crawls domain links with page limit)")
    console.print("  [bold cyan]3[/bold cyan] 📁 File Batch Mission (e.g. seeds.txt)")
    console.print("  [bold cyan]4[/bold cyan] 🧹 Flush Redis queues & start clean")
    console.print("  [bold cyan]5[/bold cyan] 🚀 Resume existing Redis queue directly (Keep previous state)\n")

    raw_choice = input("Enter option [1-5] or paste URL directly (default 1): ").strip() or "1"
    choice = normalize_input(raw_choice)
    url, file_path, flush = None, None, False
    depth, max_pages = 2, None

    # Check if user directly pasted a URL
    if choice.startswith("http://") or choice.startswith("https://") or ("." in choice and "/" in choice):
        url = choice
        flush = True
        sub = normalize_input(input("Mode: [1] Single Page only (fast), [2] Recursive crawl [default 1]: ").strip() or "1")
        if sub == "1":
            depth = 0
            max_pages = 1
        else:
            depth = 2
            max_pages = int(normalize_input(input("Max pages to scrape [default 30]: ").strip() or "30"))
    elif choice == "1":
        raw_url = input("Enter Target URL [https://fastify.dev]: ").strip() or "https://fastify.dev"
        url = raw_url
        flush = True
        depth = 0
        max_pages = 1
    elif choice == "2":
        raw_url = input("Enter Target URL [https://fastify.dev]: ").strip() or "https://fastify.dev"
        url = raw_url
        flush = True
        depth = 2
        limit_input = normalize_input(input("Max pages to scrape [default 30]: ").strip() or "30")
        max_pages = int(limit_input) if limit_input.isdigit() else 30
    elif choice == "3":
        file_path = input("Enter Seed File Path [seeds.txt]: ").strip() or "seeds.txt"
        flush = True
    elif choice == "4":
        flush = True
        sub_choice = normalize_input(input("Flush done! Now crawl: [1] Single Page, [2] Recursive, [3] File, [4] Just start: ").strip() or "1")
        if sub_choice == "1":
            url = input("Enter Target URL [https://fastify.dev]: ").strip() or "https://fastify.dev"
            depth = 0
            max_pages = 1
        elif sub_choice == "2":
            url = input("Enter Target URL [https://fastify.dev]: ").strip() or "https://fastify.dev"
            depth = 2
            max_pages = 30
        elif sub_choice == "3":
            file_path = input("Enter Seed File Path [seeds.txt]: ").strip() or "seeds.txt"
    elif choice == "5":
        flush = False  # Resume existing Redis state
    else:
        # If user entered a domain without protocol (e.g. fastify.dev or wikipedia.org)
        if "." in choice:
            url = choice
            flush = True
            depth = 0
            max_pages = 1

    # Ensure URL has protocol
    if url and not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url

    return url, file_path, flush, depth, max_pages


def main():
    parser = argparse.ArgumentParser(description="SpiderRAG Distributed Crawler TUI")
    parser.add_argument("--url", help="Target URL to crawl")
    parser.add_argument("--file", help="File containing list of URLs")
    parser.add_argument("--flush", action="store_true", help="Flush Redis state before starting")
    parser.add_argument("--depth", type=int, default=2, help="Crawl depth (default: 2)")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of pages to scrape")
    parser.add_argument("--lang", help="Comma-separated language filter (e.g. en,fa)")
    parser.add_argument("--no-guard", action="store_true", help="Disable domain boundary guard")
    parser.add_argument("--no-tui", action="store_true", help="Run in headless terminal mode")
    args = parser.parse_args()

    # If no flags provided and interactive terminal, show menu
    url = args.url
    file_path = args.file
    flush = args.flush
    depth = args.depth
    max_pages = args.max_pages
    lang_filter = args.lang

    if not url and not file_path and not flush and sys.stdin.isatty():
        url, file_path, flush, depth, max_pages = prompt_user_menu()

    sup = Supervisor(
        target_url=url,
        target_file=file_path,
        flush_on_start=flush,
        max_depth=depth,
        stay_in_domain=not args.no_guard,
        max_pages=max_pages,
        lang_filter=lang_filter
    )

    if flush:
        sup.flush_redis()

    sup.start_processes()

    # Enqueue seeds after services initialize
    def delayed_enqueue():
        time.sleep(2.0)
        if url:
            sup.enqueue_url(url)
        elif file_path:
            sup.enqueue_file(file_path)

    threading.Thread(target=delayed_enqueue, daemon=True).start()

    # Start non-blocking key listener
    if sys.stdin.isatty():
        threading.Thread(target=key_listener, args=(sup,), daemon=True).start()

    # Run Live TUI Loop
    try:
        with Live(sup.render(), refresh_per_second=4, screen=True) as live:
            while sup.running:
                live.update(sup.render())
                time.sleep(0.25)
    except KeyboardInterrupt:
        pass
    finally:
        sup.stop_processes()
        console.print("\n[bold green]✅ SpiderRAG shutdown cleanly. All workers stopped.[/bold green]")


if __name__ == "__main__":
    main()
