import socket
from pydantic_settings import BaseSettings

class WorkerSettings(BaseSettings):
    service_name: str = "order-worker"
    worker_id: str = socket.gethostname()
    
    # Redis configuration (Queue)
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_password: str = ""
    redis_db: int = 0
    queue_name: str = "orders_queue"
    
    # PostgreSQL configuration (Persistent Database)
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_db: str = "orders_db"
    
    # Worker processing simulation
    processing_delay_seconds: float = 1.5
    
    # Observability & Health server
    metrics_port: int = 9102
    
    class Config:
        env_prefix = ""
        case_sensitive = False

settings = WorkerSettings()
