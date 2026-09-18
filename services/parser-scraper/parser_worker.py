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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ParserWorker] %(message)s"
)
logger = logging.getLogger("Main")

class Worker:
    def __init__(self):
        self.running = True
        self.rdb = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            decode_responses=True
        )
        self.pipeline = ParserPipeline(self.rdb)

        # Register OS signals for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logger.info("Caught shutdown signal. Completing current work and stopping...")
        self.running = False

    def run(self):
        logger.info("==================================================================")
        logger.info(" Python Parser & Link Extraction Service Started ")
        logger.info(f" Connected to Redis at {Config.REDIS_HOST}:{Config.REDIS_PORT} ")
        logger.info(f" Listening on queue: '{Config.QUEUE_RAW_PAGES}' ")
        logger.info("==================================================================")

        # Test Redis Connection
        try:
            self.rdb.ping()
        except redis.ConnectionError as e:
            logger.critical(f"Failed to connect to Redis: {e}")
            sys.exit(1)

        while self.running:
            try:
                # Blocking pop with timeout allows checking self.running periodically
                result = self.rdb.brpop(Config.QUEUE_RAW_PAGES, timeout=Config.POLL_TIMEOUT_SEC)
                if not result:
                    continue

                _, raw_json = result
                
                # Validate against Shared Contract
                try:
                    payload = json.loads(raw_json)
                    raw_page = RawPage.model_validate(payload)
                except (json.JSONDecodeError, ValidationError) as err:
                    logger.error(f"Malformed RawPage payload rejected: {err}")
                    continue

                # Process page through parsing pipeline
                self.pipeline.process_raw_page(raw_page)

            except redis.RedisError as rerr:
                logger.error(f"Redis error: {rerr}. Retrying in 1 second...")
                time.sleep(1)
            except Exception as ex:
                logger.exception(f"Unexpected error processing page: {ex}")

        logger.info("Parser service shut down cleanly. Goodbye!")

if __name__ == "__main__":
    worker = Worker()
    worker.run()
