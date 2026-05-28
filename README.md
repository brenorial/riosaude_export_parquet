# riosaude_export_parquet

Projeto Python para executar uma query em um banco PostgreSQL e exportar o resultado para um arquivo Parquet.

O fluxo principal le a SQL em `queries/query.sql`, usa as credenciais do arquivo `.env`, processa os dados em chunks para reduzir uso de memoria e grava o arquivo final em `output/`.

## Requisitos

- Python 3.10+
- Poetry
- Acesso a um banco PostgreSQL

## Instalacao

Instale as dependencias do projeto:

```bash
poetry install
```

Crie o arquivo de variaveis de ambiente a partir do exemplo:

```bash
cp .env.example .env
```

No Windows PowerShell, se preferir:

```powershell
Copy-Item .env.example .env
```

## Configuracao

Edite o arquivo `.env` com os dados de conexao do PostgreSQL:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=seu_banco
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_SSLMODE=prefer
DB_CONNECT_TIMEOUT=10
```

Variaveis obrigatorias:

- `DB_HOST`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

Variaveis opcionais:

- `DB_PORT`: padrao `5432`
- `DB_SSLMODE`: padrao `prefer`
- `DB_CONNECT_TIMEOUT`: padrao `10`

## Query

A query que sera exportada fica em:

```text
queries/query.sql
```

Atualmente ela consulta dados de `dtw.fat_historico_leito`, com join em `dtw.fat_boletim`, filtrando entradas entre `2025-01-01` e `2026-01-01`.

Para alterar a exportacao, edite esse arquivo SQL antes de rodar o pipeline.

## Como executar

Rode o pipeline com Poetry:

```bash
poetry run python pipeline.py
```

O programa vai pedir o nome do arquivo Parquet, sem extensao:

```text
Digite o nome do arquivo parquet (sem extensao):
```

Exemplo:

```text
historico_leito_2025
```

O arquivo sera gerado em:

```text
output/historico_leito_2025.parquet
```

## Como funciona

O arquivo `pipeline.py` concentra a execucao:

1. Carrega as variaveis do `.env`.
2. Le a query em `queries/query.sql`.
3. Solicita o nome do arquivo de saida.
4. Cria a conexao com PostgreSQL.
5. Exporta o resultado para Parquet em `output/`.

A exportacao e feita por `export_parquet/exporter.py` usando:

- `pandas.read_sql_query`
- `pyarrow.parquet.ParquetWriter`
- chunks de `200000` linhas
- compressao `snappy`

Durante a exportacao, o projeto normaliza algumas colunas para evitar divergencia de schema entre chunks:

- `esp_id_origem`
- `esp_id_secao`

## Estrutura do projeto

```text
.
|-- pipeline.py
|-- pyproject.toml
|-- queries/
|   `-- query.sql
|-- export_parquet/
|   |-- __init__.py
|   |-- db.py
|   `-- exporter.py
|-- output/
|-- .env.example
`-- README.md
```

## Arquivos principais

- `pipeline.py`: ponto de entrada da exportacao.
- `export_parquet/db.py`: configuracao e criacao da conexao SQLAlchemy.
- `export_parquet/exporter.py`: leitura da SQL em chunks e escrita do Parquet.
- `queries/query.sql`: SQL executada no banco.
- `.env.example`: modelo das variaveis de ambiente.
- `output/`: pasta onde os arquivos `.parquet` sao gravados.

## Desenvolvimento

O projeto inclui ferramentas de qualidade no grupo de desenvolvimento:

```bash
poetry run ruff check .
poetry run mypy .
```

## Observacoes

- O arquivo `.env` nao deve ser versionado, pois contem credenciais.
- Os arquivos gerados em `output/` e os arquivos `.parquet` sao ignorados pelo Git.
- Se a query retornar muitos dados, mantenha a exportacao em chunks para evitar alto consumo de memoria.
