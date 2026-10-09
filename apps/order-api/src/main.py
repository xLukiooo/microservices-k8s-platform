import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional, List

import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from src.config import settings

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "service": "order-api", "message": "%(message)s"}'
)
logger = logging.getLogger("order-api")

# Prometheus Metrics
ORDERS_CREATED_TOTAL = Counter(
    "orders_created_total",
    "Total number of orders created",
    ["status"]
)
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests handled",
    ["method", "endpoint", "status_code"]
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"]
)

# Global Clients
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


def init_db():
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        order_id VARCHAR(64) PRIMARY KEY,
                        customer_id VARCHAR(100) NOT NULL,
                        item VARCHAR(100) NOT NULL,
                        quantity INT NOT NULL,
                        amount NUMERIC(10, 2) NOT NULL,
                        status VARCHAR(30) NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        worker_id VARCHAR(100),
                        processed_at TIMESTAMP WITH TIME ZONE
                    );
                """)
                conn.commit()
                logger.info("PostgreSQL database table 'orders' initialized successfully.")
    except Exception as exc:
        logger.warning(f"Could not initialize PostgreSQL (might still be starting up): {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    logger.info(f"Connecting to Redis at {settings.redis_host}:{settings.redis_port}...")
    try:
        redis_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password if settings.redis_password else None,
            db=settings.redis_db,
            decode_responses=True,
            socket_connect_timeout=3,
        )
        redis_client.ping()
        logger.info("Connected to Redis successfully.")
    except Exception as exc:
        logger.warning(f"Redis not available at startup: {exc}")

    # Initialize Postgres
    init_db()

    yield

    if redis_client:
        logger.info("Closing Redis connection...")
        redis_client.close()
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Cloud-Native Orders API",
    version=settings.app_version,
    description="Microservice for accepting customer orders, storing in PostgreSQL, and enqueuing in Redis.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request & Response Schemas
class CreateOrderRequest(BaseModel):
    customer_id: str = Field(..., example="cust_101")
    item: str = Field(..., example="DevOps Pro Laptop")
    quantity: int = Field(default=1, gt=0, example=2)
    amount: float = Field(..., gt=0.0, example=1999.99)


class OrderResponse(BaseModel):
    order_id: str
    customer_id: str
    item: str
    quantity: int
    amount: float
    status: str
    worker_id: Optional[str] = None


@app.middleware("http")
async def monitor_requests(request, call_next):
    start_time = time.time()
    path = request.url.path
    response = await call_next(request)
    duration = time.time() - start_time

    if not path.startswith(("/metrics", "/healthz", "/ready")):
        HTTP_REQUESTS_TOTAL.labels(method=request.method, endpoint=path, status_code=response.status_code).inc()
        HTTP_REQUEST_DURATION_SECONDS.labels(method=request.method, endpoint=path).observe(duration)

    return response


# Kubernetes Probes
@app.get("/healthz", tags=["Health"])
def liveness():
    return {"status": "ok", "service": settings.app_name, "version": settings.app_version}


@app.get("/ready", tags=["Health"])
def readiness():
    # 1. Check Redis
    redis_ok = False
    try:
        if redis_client and redis_client.ping():
            redis_ok = True
    except Exception:
        pass

    # 2. Check Postgres
    pg_ok = False
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                pg_ok = True
    except Exception:
        pass

    if redis_ok and pg_ok:
        return {"status": "ready", "redis": "connected", "postgres": "connected"}
    
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={"status": "unhealthy", "redis": redis_ok, "postgres": pg_ok}
    )


@app.get("/metrics", tags=["Monitoring"])
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# Business Endpoints
@app.post("/api/v1/orders", status_code=status.HTTP_201_CREATED, tags=["Orders"])
def create_order(order_req: CreateOrderRequest):
    order_id = str(uuid.uuid4())
    
    # 1. Save to PostgreSQL
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orders (order_id, customer_id, item, quantity, amount, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (order_id, order_req.customer_id, order_req.item, order_req.quantity, order_req.amount, "PENDING"))
                conn.commit()
    except Exception as exc:
        logger.error(f"Failed to insert order into PostgreSQL: {exc}")
        # Fallback to redis state if PG is down
        if redis_client:
            redis_client.hset(f"order:{order_id}", mapping={
                "order_id": order_id, "customer_id": order_req.customer_id,
                "item": order_req.item, "quantity": str(order_req.quantity),
                "amount": str(order_req.amount), "status": "PENDING"
            })

    # 2. Push to Redis queue for Worker processing
    try:
        if redis_client:
            redis_client.lpush(settings.queue_name, order_id)
            # Also keep a fast index
            redis_client.lpush("recent_orders", order_id)
            redis_client.ltrim("recent_orders", 0, 99)
        
        ORDERS_CREATED_TOTAL.labels(status="success").inc()
        logger.info(f"Order created: {order_id} (Customer: {order_req.customer_id})")
        return {
            "order_id": order_id,
            "customer_id": order_req.customer_id,
            "item": order_req.item,
            "quantity": order_req.quantity,
            "amount": order_req.amount,
            "status": "PENDING"
        }
    except Exception as exc:
        ORDERS_CREATED_TOTAL.labels(status="error").inc()
        logger.error(f"Failed to enqueue order {order_id}: {exc}")
        raise HTTPException(status_code=500, detail="Failed to enqueue order into Redis")


@app.get("/api/v1/orders", tags=["Orders"])
def list_orders():
    # Try PostgreSQL first
    try:
        with get_pg_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT order_id, customer_id, item, quantity, amount, status, worker_id, created_at, processed_at FROM orders ORDER BY created_at DESC LIMIT 20;")
                rows = cur.fetchall()
                return {"count": len(rows), "orders": rows}
    except Exception:
        # Fallback to Redis
        results = []
        if redis_client:
            order_ids = redis_client.lrange("recent_orders", 0, 19)
            for oid in order_ids:
                data = redis_client.hgetall(f"order:{oid}")
                if data:
                    results.append(data)
        return {"count": len(results), "orders": results}


@app.get("/api/v1/orders/{order_id}", tags=["Orders"])
def get_order(order_id: str):
    try:
        with get_pg_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM orders WHERE order_id = %s;", (order_id,))
                row = cur.fetchone()
                if row:
                    return row
    except Exception:
        pass

    if redis_client:
        data = redis_client.hgetall(f"order:{order_id}")
        if data:
            return data

    raise HTTPException(status_code=404, detail="Order not found")


@app.get("/api/v1/queue/stats", tags=["Monitoring"])
def get_queue_stats():
    queue_len = 0
    if redis_client:
        try:
            queue_len = redis_client.llen(settings.queue_name)
        except Exception:
            pass
    return {"queue_name": settings.queue_name, "pending_jobs": queue_len}


@app.get("/api/v1/cpu-burn", tags=["Testing"])
def cpu_burn(seconds: int = 3):
    end_time = time.time() + min(seconds, 10)
    count = 0
    while time.time() < end_time:
        count += 1
    return {"message": f"Burned CPU for ~{seconds} seconds", "iterations": count}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host=settings.host, port=settings.port, reload=True)
