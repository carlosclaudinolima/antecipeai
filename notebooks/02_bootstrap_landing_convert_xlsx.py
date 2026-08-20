# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bootstrap — Conversão do `.xlsx` para o formato de landing
# MAGIC
# MAGIC **Por que este notebook existe:** o Auto Loader (`cloudFiles`) não lê
# MAGIC `.xlsx` nativamente — os formatos suportados são `csv`, `json`, `parquet`,
# MAGIC `avro`, `orc`, `text` e `binaryFile`. Como a extração atual da Locaweb
# MAGIC vem em Excel, este notebook faz uma conversão pontual (roda com Pandas no
# MAGIC driver, não é um job distribuído) de `.xlsx` → `.parquet`, gravando no
# MAGIC subdiretório do Volume que o Auto Loader efetivamente monitora.
# MAGIC
# MAGIC **Por que Parquet, e não CSV**, para esse "arquivo ponte": no profiling
# MAGIC exploratório encontramos 11 quebras de linha e 1.241 aspas no campo
# MAGIC `Descrição resumida`, que quebram o parser CSV padrão do Spark se as
# MAGIC opções de `quote`/`escape`/`multiLine` não forem configuradas com
# MAGIC cuidado (chegamos a ler 122.554 linhas em vez de 122.543 até corrigir
# MAGIC isso). Parquet é *schema-safe* e elimina essa classe inteira de erro.
# MAGIC
# MAGIC **Estrutura de pastas dentro do Volume:**
# MAGIC ```
# MAGIC /Volumes/antecipeai/landing/raw/
# MAGIC   incoming_xlsx/     <- onde vocês fazem upload manual do .xlsx (nunca lido pelo Auto Loader)
# MAGIC   incidentes/        <- onde ESTE notebook grava o .parquet convertido (Auto Loader monitora aqui)
# MAGIC ```
# MAGIC
# MAGIC **Quando a Locaweb mandar extrações incrementais futuras:** se vierem em
# MAGIC CSV/Parquet, podem ser jogadas direto em `incidentes/`, pulando este
# MAGIC notebook — ele existe só por causa do formato Excel do arquivo atual.
# MAGIC
# MAGIC **Pré-requisito:** ter subido `LW-DATASET.xlsx` para
# MAGIC `/Volumes/antecipeai/landing/raw/incoming_xlsx/` via Catalog Explorer
# MAGIC ("Upload to this volume") antes de rodar este notebook.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Garantir dependências (openpyxl para ler `.xlsx` via pandas)

# COMMAND ----------

# MAGIC %pip install -q openpyxl
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler o `.xlsx` da área de upload manual

# COMMAND ----------

dbutils.widgets.text("source_filename", "LW-DATASET.xlsx", "Nome do arquivo .xlsx enviado")
source_filename = dbutils.widgets.get("source_filename")

incoming_dir = volume_path("incoming_xlsx")
incidentes_dir = volume_path("incidentes")

dbutils.fs.mkdirs(incoming_dir)
dbutils.fs.mkdirs(incidentes_dir)

source_path = f"{incoming_dir}/{source_filename}"
print("Lendo arquivo de origem:", source_path)

import pandas as pd

pdf = pd.read_excel(source_path, sheet_name="Dataset Geral")
print("Shape lido:", pdf.shape)
print(pdf.dtypes)

# COMMAND ----------
# Resultado esperado (validado previamente fora do Databricks, com o dataset
# real da Locaweb): shape = (122543, 19), batendo exatamente com o dicionário
# de dados v2 (19 colunas: Número, Prioridade, Produto, Categoria,
# Subcategoria, Grupo designado, Item de configuração, Aberto, Resolvido,
# Encerrado, Duração, Código de fechamento, Descrição resumida, Solução,
# Aberto por, Incidente Pai, Status, Entrou para KPI?, KPI Violado?).
# Se o shape aqui vier diferente, o arquivo enviado não é o esperado —
# conferir antes de continuar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converter tudo para string (schema-on-read mínimo)
# MAGIC
# MAGIC A Bronze deve receber os dados "na íntegra", sem qualquer tratamento de
# MAGIC tipo — isso inclui não deixar o Parquet "adivinhar" tipos numéricos ou de
# MAGIC data. Forçamos todas as colunas para string aqui, na ponte, para que o
# MAGIC Auto Loader herde exatamente essa mesma fidelidade ao ler o Parquet na
# MAGIC Bronze (notebook `03`).

# COMMAND ----------

pdf_str = pdf.astype(str)
# pandas converte NaN para a string literal "nan" no astype(str) — desfazemos
# isso para não confundir "nulo de verdade" com o texto "nan" na Bronze.
pdf_str = pdf_str.where(pdf.notna(), None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravar como Parquet no diretório monitorado pelo Auto Loader
# MAGIC
# MAGIC Forçamos um schema Parquet explícito com **todas as colunas como
# MAGIC string** (via `pyarrow.schema`), em vez de deixar o pandas/pyarrow
# MAGIC inferir o tipo. Isso evita um problema sutil de schema evolution no Auto
# MAGIC Loader: colunas com muitos nulos (ex.: `Resolvido`, presente em só 32,8%
# MAGIC das linhas) podem ser inferidas como tipo `null`/`double` num arquivo e
# MAGIC como `string` em outro, se cada extração futura for convertida
# MAGIC separadamente — o que quebraria a leitura incremental.

# COMMAND ----------

import uuid
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq

output_filename = f"incidentes_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
output_path = f"{incidentes_dir}/{output_filename}"

forced_string_schema = pa.schema([(col, pa.string()) for col in pdf_str.columns])
arrow_table = pa.Table.from_pandas(pdf_str, schema=forced_string_schema, preserve_index=False)
pq.write_table(arrow_table, output_path.replace("dbfs:", ""))

print(f"Arquivo convertido gravado em: {output_path}")
print(f"Linhas gravadas: {len(pdf_str)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

display(dbutils.fs.ls(incidentes_dir))
