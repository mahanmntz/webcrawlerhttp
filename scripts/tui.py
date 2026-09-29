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
    def __init__(self, target_url=None, target_file=None, flush_on_start=False, max_depth=2, stay_in_domain=True):
        self.target_url = target_url
        self.target_file = target_file
        self.flush_on_start = flush_on_start
        self.max_depth = max_depth
        self.stay_in_domain = stay_in_domain

        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
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
            self.status_msg = "🧹 All Redis queues & state successfully flushed!"
            self.log("SYSTEM", "Redis state and queues flushed cleanly.")
        except Exception as e:
            self.status_msg = f"❌ Error flushing Redis: {e}"
            self.log("ERROR", f"Flush error: {e}")

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

    def export_documents(self):
        try:
            docs_raw = self.r.lrange("queue:parsed_docs", 0, -1)
            docs = []
            for item in docs_raw:
                try:
                    docs.append(json.loads(item))
                except:
                    pass
            filename = f"crawled_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            out_path = os.path.join(ROOT_DIR, filename)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({"total_count": len(docs), "documents": docs}, f, indent=2, ensure_ascii=False)
            self.status_msg = f"💾 Exported {len(docs)} documents to {filename}"
            self.log("EXPORT", f"Saved {len(docs)} documents to {out_path}")
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
                # Pairs like "Number of filters", n, "Number of items inserted", n
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

            latest_doc = None
            latest_raw = self.r.lindex("queue:parsed_docs", 0)
            if latest_raw:
                try:
                    latest_doc = json.loads(latest_raw)
                except json.JSONDecodeError:
                    pass

            savings_pct = round((1 - (md_bytes / raw_bytes)) * 100, 1) if raw_bytes > 0 else 0.0

            return {
                "pending": pending,
                "in_flight": in_flight,
                "parsed": parsed_count,
                "seen": seen,
                "savings_pct": savings_pct,
                "est_tokens": est_tokens,
                "latest_doc": latest_doc
            }
        except Exception:
            return {"pending": 0, "in_flight": 0, "parsed": 0, "seen": 0, "savings_pct": 0.0, "est_tokens": 0, "latest_doc": None}

    def render(self) -> Layout:
        metrics = self.get_metrics()
        layout = Layout()

        if self.show_logs:
            layout.split_column(
                Layout(name="header", size=10),
                Layout(name="body", ratio=1),
                Layout(name="footer", size=3)
            )
        else:
            layout.split_column(
                Layout(name="header", size=13),
                Layout(name="body", ratio=1),
                Layout(name="footer", size=3)
            )

        # Header: Metrics Table
        grid = Table.grid(expand=True)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="center", ratio=1)

        grid.add_row(
            f"[bold yellow]⏳ Pending[/bold yellow]\n[bold white]{metrics['pending']:,}[/bold white]",
            f"[bold cyan]⚡ In-Flight[/bold cyan]\n[bold white]{metrics['in_flight']}[/bold white]",
            f"[bold blue]🛡️ Filtered (Seen)[/bold blue]\n[bold white]{metrics['seen']:,}[/bold white]",
            f"[bold green]📄 Clean Docs[/bold green]\n[bold white]{metrics['parsed']:,}[/bold white]",
            f"[bold emerald]📉 Token Savings[/bold emerald]\n[bold green]-{metrics['savings_pct']}%[/bold green]",
            f"[bold magenta]🧠 LLM Tokens[/bold magenta]\n[bold white]~{metrics['est_tokens']:,}[/bold white]",
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
                title="[bold yellow]📜 Live Cluster Logs (Press [bold white]L[/bold white] to collapse)[/bold yellow]",
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

            status_color = "green" if "successfully" in self.status_msg or "Enqueued" in self.status_msg else "white"
            body_panel = Panel(
                f"[bold {status_color}]Status:[/bold {status_color}] {self.status_msg}\n\n"
                f"[bold underline]Latest Extracted Document:[/bold underline]\n{info_text}",
                title="[bold green]📡 Real-time Extractor Overview[/bold green]",
                border_style="green"
            )
            layout["body"].update(body_panel)

        # Footer Hotkeys
        log_status = "[bold yellow]ON[/bold yellow]" if self.show_logs else "[dim]OFF[/dim]"
        footer_text = Text.from_markup(
            f"[bold white]\\[L][/bold white] Toggle Logs ({log_status})  │  "
            f"[bold white]\\[F][/bold white] Flush Queues  │  "
            f"[bold white]\\[E][/bold white] Export JSON  │  "
            f"[bold white]\\[Q][/bold white] Quit Cleanly"
        )
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
                elif k in ('f', 'ب'):
                    sup.flush_redis()
                elif k in ('e', 'ث'):
                    sup.export_documents()
                elif k in ('q', 'ض', '\x03'):  # Explicit quit only
                    sup.running = False
                    break
    except Exception:
        pass
    finally:
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
    console.print("  [bold cyan]1[/bold cyan] 🎯 Single URL Mission (e.g. https://fastify.dev)")
    console.print("  [bold cyan]2[/bold cyan] 📁 File Batch Mission (e.g. seeds.txt)")
    console.print("  [bold cyan]3[/bold cyan] 🧹 Flush Redis queues & start clean")
    console.print("  [bold cyan]4[/bold cyan] 🚀 Resume existing Redis queue directly\n")

    raw_choice = input("Enter option [1-4] or paste URL directly (default 1): ").strip() or "1"
    choice = normalize_input(raw_choice)
    url, file_path, flush = None, None, False

    # Check if user directly pasted a URL
    if choice.startswith("http://") or choice.startswith("https://") or ("." in choice and "/" in choice):
        url = choice
    elif choice == "1":
        raw_url = input("Enter Target URL [https://fastify.dev]: ").strip() or "https://fastify.dev"
        url = raw_url
    elif choice == "2":
        file_path = input("Enter Seed File Path [seeds.txt]: ").strip() or "seeds.txt"
    elif choice == "3":
        flush = True
        sub_choice = normalize_input(input("Flush done! Now crawl: [1] URL, [2] File, [3] Just start: ").strip() or "1")
        if sub_choice.startswith("http://") or sub_choice.startswith("https://"):
            url = sub_choice
        elif sub_choice == "1":
            url = input("Enter Target URL [https://fastify.dev]: ").strip() or "https://fastify.dev"
        elif sub_choice == "2":
            file_path = input("Enter Seed File Path [seeds.txt]: ").strip() or "seeds.txt"
    elif choice == "4":
        pass  # Resume existing Redis state
    else:
        # If user entered a domain without protocol (e.g. fastify.dev or mahanmontazeri.ir)
        if "." in choice:
            url = choice

    # Ensure URL has protocol
    if url and not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url

    return url, file_path, flush


def main():
    parser = argparse.ArgumentParser(description="SpiderRAG Distributed Crawler TUI")
    parser.add_argument("--url", help="Target URL to crawl")
    parser.add_argument("--file", help="File containing list of URLs")
    parser.add_argument("--flush", action="store_true", help="Flush Redis state before starting")
    parser.add_argument("--depth", type=int, default=2, help="Crawl depth (default: 2)")
    parser.add_argument("--no-guard", action="store_true", help="Disable domain boundary guard")
    parser.add_argument("--no-tui", action="store_true", help="Run in headless terminal mode")
    args = parser.parse_args()

    # If no flags provided and interactive terminal, show menu
    url = args.url
    file_path = args.file
    flush = args.flush

    if not url and not file_path and not flush and sys.stdin.isatty():
        url, file_path, flush = prompt_user_menu()

    sup = Supervisor(
        target_url=url,
        target_file=file_path,
        flush_on_start=flush,
        max_depth=args.depth,
        stay_in_domain=not args.no_guard
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
