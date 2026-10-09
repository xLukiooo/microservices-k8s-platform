import json
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import redis
import psycopg2
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from src.config import settings

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "service": "order-worker", "worker_id": "' + settings.worker_id + '", "message": "%(message)s"}'
)
logger = logging.getLogger("order-worker")

# Prometheus Metrics
ORDERS_PROCESSED_TOTAL = Counter(
    "orders_processed_total",
    "Total orders processed by workers",
    ["status", "worker_id"]
)
ORDER_PROCESSING_DURATION_SECONDS = Histogram(
    "order_processing_duration_seconds",
    "Time taken to process an individual order in seconds",
    ["worker_id"]
)
ORDERS_IN_FLIGHT = Gauge(
    "orders_in_flight",
    "Number of orders currently being processed by this worker",
    ["worker_id"]
)

# State flags
is_running = True
is_healthy = True
current_processing_job: Optional[str] = None
redis_client: Optional[redis.Redis] = None


def get_pg_connection():
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname=settings.postgres_db,
        connect_timeout=3,
    )


# HTTP Handler for Kubernetes Health Probes & Prometheus Metrics
class HealthAndMetricsHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_GET(self):
        global redis_client, is_healthy, is_running
        
        if self.path == "/healthz":
            if is_running and is_healthy:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "alive", "service": "order-worker"}\n')
            else:
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "shutting_down_or_unhealthy"}\n')
                
        elif self.path == "/ready":
            redis_ok = False
            pg_ok = False
            try:
                if redis_client and redis_client.ping():
                    redis_ok = True
            except Exception:
                pass

            try:
                with get_pg_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1;")
                        pg_ok = True
            except Exception:
                pass

            if redis_ok:  # Worker strictly depends on Redis queue
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(f'{{"status": "ready", "redis": {str(redis_ok).lower()}, "postgres": {str(pg_ok).lower()}}}\n'.encode("utf-8"))
            else:
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "unready", "error": "Redis not reachable"}\n')
                
        elif self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest())
            
        else:
            self.send_response(404)
            self.end_headers()


def start_health_server(port: int):
    server = HTTPServer(("0.0.0.0", port), HealthAndMetricsHandler)
    logger.info(f"Health check and metrics server listening on port {port}")
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    return server


def handle_shutdown_signal(signum, frame):
    global is_running
    sig_name = signal.Signals(signum).name
    logger.info(f"Received signal {sig_name}. Initiating graceful shutdown...")
    is_running = False


def update_order_in_pg(order_id: str, status: str, worker_id: str, processed_at: Optional[datetime] = None):
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                if processed_at:
                    cur.execute("""
                        UPDATE orders 
                        SET status = %s, worker_id = %s, processed_at = %s 
                        WHERE order_id = %s;
                    """, (status, worker_id, processed_at, order_id))
                else:
                    cur.execute("""
                        UPDATE orders 
                        SET status = %s, worker_id = %s 
                        WHERE order_id = %s;
                    """, (status, worker_id, order_id))
                conn.commit()
    except Exception as exc:
        logger.warning(f"Could not update order {order_id} in PostgreSQL: {exc}")


def process_order(order_id: str):
    global current_processing_job, redis_client
    current_processing_job = order_id
    ORDERS_IN_FLIGHT.labels(worker_id=settings.worker_id).inc()
    
    start_time = time.time()
    logger.info(f"Starting processing for order {order_id}...")
    
    try:
        # 1. Update status to PROCESSING
        update_order_in_pg(order_id, "PROCESSING", settings.worker_id)
        if redis_client:
            redis_client.hset(f"order:{order_id}", mapping={
                "status": "PROCESSING",
                "worker_id": settings.worker_id,
            })
        
        # 2. Simulate processing work
        time.sleep(settings.processing_delay_seconds)
        
        # 3. Update status to COMPLETED
        completion_time = datetime.now(timezone.utc)
        duration = time.time() - start_time
        
        update_order_in_pg(order_id, "COMPLETED", settings.worker_id, processed_at=completion_time)
        if redis_client:
            redis_client.hset(f"order:{order_id}", mapping={
                "status": "COMPLETED",
                "processed_at": str(time.time()),
            })
        
        ORDERS_PROCESSED_TOTAL.labels(status="success", worker_id=settings.worker_id).inc()
        ORDER_PROCESSING_DURATION_SECONDS.labels(worker_id=settings.worker_id).observe(duration)
        logger.info(f"Successfully completed order {order_id} in {duration:.2f}s")
        
    except Exception as exc:
        ORDERS_PROCESSED_TOTAL.labels(status="error", worker_id=settings.worker_id).inc()
        logger.error(f"Error while processing order {order_id}: {exc}")
        update_order_in_pg(order_id, "FAILED", settings.worker_id)
    finally:
        ORDERS_IN_FLIGHT.labels(worker_id=settings.worker_id).dec()
        current_processing_job = None


def main():
    global is_running, is_healthy, redis_client
    
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    
    start_health_server(settings.metrics_port)
    
    logger.info(f"Connecting worker to Redis at {settings.redis_host}:{settings.redis_port}...")
    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password if settings.redis_password else None,
        db=settings.redis_db,
        decode_responses=True,
        socket_connect_timeout=3,
    )
    
    while is_running:
        try:
            job = redis_client.brpop(settings.queue_name, timeout=1)
            if job:
                _, order_id = job
                process_order(order_id)
        except redis.ConnectionError as exc:
            logger.warning(f"Redis connection issue: {exc}. Retrying in 2 seconds...")
            time.sleep(2)
        except Exception as exc:
            logger.error(f"Unexpected worker loop error: {exc}")
            time.sleep(1)
            
    logger.info("Worker loop stopped cleanly. Shutting down worker process.")
    sys.exit(0)


if __name__ == "__main__":
    main()
