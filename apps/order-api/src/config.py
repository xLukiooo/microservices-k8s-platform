from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "order-api"
    app_version: str = "1.0.0"
    host: str = "0.0.0.0"
    port: int = 8000
    
    # Redis configuration (Queue & Cache)
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
    
    class Config:
        env_prefix = ""
        case_sensitive = False

settings = Settings()
