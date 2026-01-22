from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv

from export_parquet.db import DbConfig, make_engine
from export_parquet.exporter import export_sql_to_parquet


SQL_PATH = Path("queries/query.sql")

CHUNKSIZE = 200_000
COMPRESSION = "snappy"
LOG_LEVEL = "INFO"

logger = logging.getLogger("export-parquet")


def main() -> None:
    load_dotenv()

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if not SQL_PATH.exists():
        raise SystemExit(f"SQL não encontrado: {SQL_PATH}")

    sql = SQL_PATH.read_text(encoding="utf-8")

    # ⬇️ INPUT DO USUÁRIO
    file_name = input(
        "Digite o nome do arquivo parquet (sem extensão): "
    ).strip()

    if not file_name:
        raise SystemExit("Nome do arquivo não pode ser vazio.")

    out_path = Path("output") / f"{file_name}.parquet"

    cfg = DbConfig.from_env()
    engine = make_engine(cfg)

    logger.info("Conectando em %s:%s/%s", cfg.host, cfg.port, cfg.name)
    logger.info("Executando query (%s) e exportando para %s", SQL_PATH, out_path)

    total = export_sql_to_parquet(
        engine=engine,
        sql=sql,
        out_path=out_path,
        chunksize=CHUNKSIZE,
        compression=COMPRESSION,
    )

    logger.info("Concluído. Linhas exportadas: %s", f"{total:,}")


if __name__ == "__main__":
    main()
