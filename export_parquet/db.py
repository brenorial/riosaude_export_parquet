from __future__ import annotations

import os
from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    name: str
    user: str
    password: str
    sslmode: str = "prefer"
    connect_timeout: int = 10

    @classmethod
    def from_env(cls) -> "DbConfig":
        return cls(
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", "5432")),
            name=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            sslmode=os.environ.get("DB_SSLMODE", "prefer"),
            connect_timeout=int(os.environ.get("DB_CONNECT_TIMEOUT", "10")),
        )

    def sqlalchemy_url(self) -> URL:
        return URL.create(
            drivername="postgresql+psycopg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.name,
            query={
                "sslmode": self.sslmode,
                "connect_timeout": str(self.connect_timeout),
            },
        )


def make_engine(cfg: DbConfig) -> Engine:
    return create_engine(cfg.sqlalchemy_url(), pool_pre_ping=True)
