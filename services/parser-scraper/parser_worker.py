import json
import logging
import signal
import sys
import time
import redis
from pydantic import ValidationError

from app.config import Config
from app.models import RawPage
from app.pipeline import ParserPipeline
from app.reliable_queue import ReliableQueue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ParserWorker] %(message)s"
)
logger = logging.getLogger("Main")

class Worker:
    def __init__(self, rdb: redis.Redis | None = None):
        self.running = True
        self.rdb = rdb or redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            decode_responses=True
        )
        self.pipeline = ParserPipeline(self.rdb)
        self.queue = ReliableQueue(
            self.rdb,
            Config.QUEUE_RAW_PAGES,
            visibility_timeout_sec=Config.VISIBILITY_TIMEOUT_SEC,
            max_redeliveries=Config.MAX_REDELIVERIES,
        )
        self.next_reap = 0.0

        # Register OS signals for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logger.info("Caught shutdown signal. Completing current work and stopping...")
        self.running = False

    def maybe_reap(self):
        now = time.monotonic()
        if now < self.next_reap:
            return
        self.next_reap = now + Config.REAP_INTERVAL_SEC
        requeued, dead = self.queue.reap()
        if requeued or dead:
            logger.warning(f"♻️  Recovered {requeued} stalled raw pages, dead-lettered {dead}")

    def handle(self, raw_json: str):
        # Validate against Shared Contract
        try:
            raw_page = RawPage.model_validate(json.loads(raw_json))
        except (json.JSONDecodeError, ValidationError) as err:
            logger.error(f"Malformed RawPage payload dead-lettered: {err}")
            self.queue.dead_letter(raw_json, f"malformed RawPage: {err}")
            return

        # Process page through parsing pipeline
        try:
            self.pipeline.process_raw_page(raw_page)
        except redis.RedisError:
            raise
        except Exception as ex:
            # Deterministic failure (e.g. parser bug on this HTML): retrying won't help.
            logger.exception(f"Failed to process '{raw_page.url}', dead-lettering: {ex}")
            self.queue.dead_letter(raw_json, f"{type(ex).__name__}: {ex}")
            return

        self.queue.ack(raw_json)

    def run(self):
        logger.info("==================================================================")
        logger.info(" Python Parser & Link Extraction Service Started ")
        logger.info(f" Connected to Redis at {Config.REDIS_HOST}:{Config.REDIS_PORT} ")
        logger.info(f" Listening on queue: '{Config.QUEUE_RAW_PAGES}' (at-least-once) ")
        logger.info("==================================================================")

        # Test Redis Connection
        try:
            self.rdb.ping()
        except redis.ConnectionError as e:
            logger.critical(f"Failed to connect to Redis: {e}")
            sys.exit(1)

        while self.running:
            try:
                self.maybe_reap()

                # Blocking move with timeout allows checking self.running periodically
                raw_json = self.queue.take(Config.POLL_TIMEOUT_SEC)
                if raw_json is None:
                    continue

                self.handle(raw_json)

            except redis.RedisError as rerr:
                # Anything taken but not acknowledged is redelivered by the reaper.
                logger.error(f"Redis error: {rerr}. Retrying in 1 second...")
                time.sleep(1)
            except Exception as ex:
                logger.exception(f"Unexpected worker error: {ex}")
                time.sleep(1)

        logger.info("Parser service shut down cleanly. Goodbye!")

if __name__ == "__main__":
    worker = Worker()
    worker.run()
