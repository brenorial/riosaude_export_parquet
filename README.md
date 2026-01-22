# Exportar SQL em Parquet

Projeto Python (Poetry) para executar uma query no **PostgreSQL** e salvar o resultado em **Parquet** (`pyarrow`),
com suporte a **chunks** para evitar estouro de memória.

---

## Requisitos

- Python 3.10+
- Poetry

---

## Como rodar 

#### 1) Instale o poetry

```bash
poetry install
cp .env.example .env
```

#### 2) Crie um arquivo `.env` com as variáveis do banco. 

```bash
sed 's/=.*$/=/' .env.example > .env
```

#### 3) Digite a query

A query fica em `queries/query.sql`
Rode 

```bash
poetry run python -m pipeline.py
```
#### 4) Rode o código

Ao rodar o código, digite o nome do arquivo parquet no terminal. 

```bash
$ poetry run python pipeline.py
Digite o nome do arquivo parquet (sem extensão): 
```
---

#### 5) Arquivo gerado
O arquivo é então gerado e armazenado em `output/`
