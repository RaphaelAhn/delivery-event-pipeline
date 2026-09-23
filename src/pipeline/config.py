"""환경변수 설정. .env 가 있으면 읽고, 없으면 docker-compose 기본값을 사용한다."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_db: str = os.getenv("CLICKHOUSE_DB", "delivery")
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "pipeline")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "pipeline")


settings = Settings()
