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
            keys = [
                "frontier:queue", "frontier:processing", "frontier:seen",
                "frontier:bloom:url", "queue:raw_pages", "queue:parsed_docs",
                "content:seen"
            ]
            self.r.delete(*keys)
            for pattern in ["job:*", "politeness:*"]:
                matched = self.r.keys(pattern)
                if matched:
                    self.r.delete(*matched)
            self.status_msg = "🧹 All Redis queues & state successfully flushed!"
            self.log("SYSTEM", "Redis state and queues flushed cleanly.")
        except Exception as e:
            self.status_msg = f"❌ Error flushing Redis: {e}"
            self.log("ERROR", f"Flush error: {e}")

    def enqueue_url(self, url: str):
        try:
            import urllib.request
            payload = json.dumps({
                "url": url,
                "max_depth": self.max_depth,
                "priority": 5,
                "stay_in_domain": self.stay_in_domain
            }).encode('utf-8')
            req = urllib.request.Request(
                f"{GATEWAY_URL}/api/jobs",
                data=payload,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req) as resp:
                if resp.status == 201:
                    self.status_msg = f"🚀 Enqueued: {url}"
                    self.log("GATEWAY", f"Target enqueued: {url}")
        except Exception as e:
            self.status_msg = f"⚠️ Submit note: {e}"

    def enqueue_file(self, file_path: str):
        full_path = os.path.abspath(file_path)
        if not os.path.exists(full_path):
            self.status_msg = f"❌ File not found: {file_path}"
            return
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
            
            import urllib.request
            payload = json.dumps({
                "urls": urls,
                "max_depth": self.max_depth,
                "priority": 5,
                "stay_in_domain": self.stay_in_domain
            }).encode('utf-8')
            req = urllib.request.Request(
                f"{GATEWAY_URL}/api/jobs/batch",
                data=payload,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req) as resp:
                res = json.loads(resp.read().decode('utf-8'))
                self.status_msg = f"📁 Batch enqueued {res.get('enqueued_count', 0)} URLs ({res.get('deduplicated_count', 0)} deduplicated)"
                self.log("GATEWAY", self.status_msg)
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
            pending = self.r.llen("frontier:queue")
            in_flight = self.r.llen("frontier:processing")
            parsed_count = self.r.llen("queue:parsed_docs")
            
            # Unique URLs seen
            try:
                seen_count = self.r.execute_command("BF.INFO", "frontier:bloom:url")
                # parse bloom
                seen = 0
                if isinstance(seen_count, list):
                    for i in range(0, len(seen_count)-1, 2):
                        if "items" in str(seen_count[i]).lower() or "number" in str(seen_count[i]).lower():
                            seen = int(seen_count[i+1])
                            break
            except:
                seen = self.r.scard("frontier:seen")

            # Sample docs for token savings
            sample_docs = self.r.lrange("queue:parsed_docs", 0, 30)
            raw_bytes, md_bytes, est_tokens = 0, 0, 0
            latest_doc = None

            for s in sample_docs:
                try:
                    d = json.loads(s)
                    if not latest_doc:
                        latest_doc = d
                    raw_bytes += d.get("raw_html_bytes", 0)
                    md_bytes += d.get("markdown_bytes", 0)
                    est_tokens += d.get("estimated_tokens", 0)
                except:
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


def key_listener(sup: Supervisor):
    # Non-blocking key capture for macOS and Linux
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        while sup.running:
            rlist, _, _ = select.select([sys.stdin], [], [], 0.2)
            if rlist:
                key = sys.stdin.read(1).lower()
                if key == 'l':
                    sup.show_logs = not sup.show_logs
                elif key == 'f':
                    sup.flush_redis()
                elif key == 'e':
                    sup.export_documents()
                elif key == 'q':
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

    choice = input("Enter option [1-4] (default 1): ").strip() or "1"
    url, file_path, flush = None, None, False

    if choice == "1":
        url = input("Enter Target URL [https://fastify.dev]: ").strip() or "https://fastify.dev"
    elif choice == "2":
        file_path = input("Enter Seed File Path [seeds.txt]: ").strip() or "seeds.txt"
    elif choice == "3":
        flush = True
        sub_choice = input("Flush done! Now crawl: [1] URL, [2] File, [3] Just start: ").strip() or "1"
        if sub_choice == "1":
            url = input("Enter Target URL [https://fastify.dev]: ").strip() or "https://fastify.dev"
        elif sub_choice == "2":
            file_path = input("Enter Seed File Path [seeds.txt]: ").strip() or "seeds.txt"

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
