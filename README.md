# Exportar SQL em Parquet

Projeto Python (Poetry) para executar uma query no **PostgreSQL** e salvar o resultado em **Parquet** (`pyarrow`),
com suporte a **chunks** para evitar estouro de memória.

---

## Requisitos

- Python 3.10+
- Poetry

---

## Setup

```bash
poetry install
cp .env.example .env
# edite o .env com as credenciais
```

---

## Como rodar

A query fica em `queries/irs_fat_boletim.sql` (você pode trocar por outra).

```bash
poetry run python -m export_parquet.pipeline
```

O arquivo será gerado em:

- `output/irs_fat_boletim_2025.parquet`

---

## Onde você altera a query e parâmetros

Abra `export_parquet/main.py` e ajuste:

- `SQL_PATH` (arquivo .sql)
- `OUT_PATH` (arquivo de saída)
- `PARAMS` (parâmetros usados na query)
- `CHUNKSIZE` (tamanho do chunk; use `None` para carregar tudo)
- `COMPRESSION` (snappy/gzip/zstd...)

---

## Exemplo de SQL com parâmetro

No seu `.sql`:

```sql
SELECT *
FROM riosaude.irs_fat_boletim
WHERE data_entrada >= :data_min
```

No `main.py`:

```python
PARAMS = {"data_min": "2025-01-01"}
```

---

## Segurança

- `.env` está no `.gitignore`
- não commite credenciais
