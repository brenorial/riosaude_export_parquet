from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def export_sql_to_parquet(
    *,
    engine: Engine,
    sql: str,
    out_path: Path,
    params: Dict[str, Any] | None = None,
    chunksize: int | None = 200_000,
    compression: str = "snappy",
) -> int:
    """Executa SQL e salva em Parquet. Retorna o total de linhas exportadas."""
    params = params or {}
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if chunksize is None:
        df = pd.read_sql_query(text(sql), con=engine, params=params)
        df.to_parquet(out_path, index=False, engine="pyarrow", compression=compression)
        return int(len(df))

    import pyarrow as pa
    import pyarrow.parquet as pq

    writer: pq.ParquetWriter | None = None
    total = 0

    try:
        for chunk in pd.read_sql_query(text(sql), con=engine, params=params, chunksize=chunksize):
            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(
                    str(out_path),
                    table.schema,
                    compression=compression,
                )

            writer.write_table(table)
            total += len(chunk)

        return int(total)

    finally:
        if writer is not None:
            writer.close()
