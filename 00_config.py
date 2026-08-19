# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração do projeto AntecipeAI
# MAGIC
# MAGIC Este notebook **não é uma etapa do pipeline** — ele é chamado com `%run` no
# MAGIC início de todos os outros notebooks para carregar a configuração central
# MAGIC do projeto a partir do arquivo `.env`.
# MAGIC
# MAGIC A ideia por trás disso: migrar o projeto do Databricks Free (tudo managed,
# MAGIC storage do próprio metastore) para um ambiente de nuvem (S3/AWS,
# MAGIC ADLS/Azure, GCS/GCP, Object Storage/OCI) deve ser uma **troca de valores
# MAGIC no `.env`**, e não uma reescrita de notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Localizar e carregar o `.env`
# MAGIC
# MAGIC Assumimos a estrutura de pastas `antecipeai/notebooks/` e
# MAGIC `antecipeai/config/antecipeai.env` lado a lado no Repo/Workspace. Se a sua
# MAGIC estrutura for diferente, informe o caminho exato no widget
# MAGIC `env_file_path` antes de rodar este notebook.

# COMMAND ----------

dbutils.widgets.text("env_file_path", "", "Caminho do .env (opcional, sobrepõe os padrões)")

import os

_env_widget = dbutils.widgets.get("env_file_path").strip()

_candidate_paths = [p for p in [
    _env_widget,
    "../config/antecipeai.env",
    "./config/antecipeai.env",
    "/Workspace/Shared/antecipeai/config/antecipeai.env",
] if p]


def _parse_env_file(path: str) -> dict:
    """Parser manual de .env — evita depender de python-dotenv estar instalado no cluster."""
    cfg = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            cfg[key.strip()] = value.strip()
    return cfg


_env_path_used = None
_raw_config = {}
for p in _candidate_paths:
    if os.path.exists(p):
        _raw_config = _parse_env_file(p)
        _env_path_used = p
        break

if not _raw_config:
    print(f"[AVISO] Nenhum .env encontrado nos caminhos tentados: {_candidate_paths}")
    print("Usando valores DEFAULT hardcoded (iguais ao antecipeai.env de referência).")
    print("Para usar o arquivo de verdade, suba config/antecipeai.env no Repo/Workspace")
    print("ao lado da pasta notebooks/, ou informe o caminho no widget 'env_file_path'.")
else:
    print(f"Configuração carregada de: {_env_path_used}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defaults (usados apenas se o `.env` não for encontrado)

# COMMAND ----------

_defaults = {
    "ANTECIPAI_CATALOG": "antecipeai",
    "ANTECIPAI_SCHEMA_LANDING": "landing",
    "ANTECIPAI_SCHEMA_BRONZE": "bronze",
    "ANTECIPAI_SCHEMA_SILVER": "silver",
    "ANTECIPAI_SCHEMA_GOLD": "gold",
    "ANTECIPAI_VOLUME_RAW": "raw",
    "ANTECIPAI_TABLE_TYPE": "MANAGED",
    "ANTECIPAI_STORAGE_ROOT": "",
    "ANTECIPAI_CLOUD_PROVIDER": "NONE",
    "ANTECIPAI_COMPUTE_MODE": "SERVERLESS_FREE",
}

_config = {**_defaults, **_raw_config}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variáveis expostas para os notebooks que derem `%run` neste
# MAGIC
# MAGIC Por ser chamado via `%run`, tudo que é definido aqui fica disponível no
# MAGIC notebook que chamou — não precisa importar nada manualmente depois.

# COMMAND ----------

CATALOG = _config["ANTECIPAI_CATALOG"]
SCHEMA_LANDING = _config["ANTECIPAI_SCHEMA_LANDING"]
SCHEMA_BRONZE = _config["ANTECIPAI_SCHEMA_BRONZE"]
SCHEMA_SILVER = _config["ANTECIPAI_SCHEMA_SILVER"]
SCHEMA_GOLD = _config["ANTECIPAI_SCHEMA_GOLD"]
VOLUME_RAW = _config["ANTECIPAI_VOLUME_RAW"]

TABLE_TYPE = _config["ANTECIPAI_TABLE_TYPE"].strip().upper()
STORAGE_ROOT = _config["ANTECIPAI_STORAGE_ROOT"].strip()
CLOUD_PROVIDER = _config["ANTECIPAI_CLOUD_PROVIDER"].strip().upper()

if TABLE_TYPE not in ("MANAGED", "EXTERNAL"):
    raise ValueError(f"ANTECIPAI_TABLE_TYPE inválido: '{TABLE_TYPE}'. Use MANAGED ou EXTERNAL.")

if TABLE_TYPE == "EXTERNAL" and not STORAGE_ROOT:
    raise ValueError(
        "ANTECIPAI_TABLE_TYPE=EXTERNAL exige ANTECIPAI_STORAGE_ROOT preenchido "
        "(ex.: s3://bucket/prefixo/)."
    )


def qualified_table(schema: str, table: str) -> str:
    """Nome totalmente qualificado catalog.schema.tabela (three-level namespace do Unity Catalog)."""
    return f"{CATALOG}.{schema}.{table}"


def volume_path(subpath: str = "") -> str:
    """Path do volume de landing, no padrão /Volumes/catalog/schema/volume/subpath."""
    base = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/{VOLUME_RAW}"
    return f"{base}/{subpath}" if subpath else base


print("Configuração ativa:")
print(f"  CATALOG         = {CATALOG}")
print(f"  SCHEMA_LANDING  = {SCHEMA_LANDING}")
print(f"  SCHEMA_BRONZE   = {SCHEMA_BRONZE}")
print(f"  SCHEMA_SILVER   = {SCHEMA_SILVER}")
print(f"  SCHEMA_GOLD     = {SCHEMA_GOLD}")
print(f"  VOLUME_RAW      = {VOLUME_RAW}")
print(f"  TABLE_TYPE      = {TABLE_TYPE}")
print(f"  STORAGE_ROOT    = {STORAGE_ROOT or '(vazio - ok p/ MANAGED)'}")
print(f"  CLOUD_PROVIDER  = {CLOUD_PROVIDER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: DDL de criação de schema (MANAGED vs EXTERNAL)
# MAGIC
# MAGIC Centraliza a única parte do projeto que de fato muda entre "Databricks
# MAGIC Free" e "produção na nuvem": **onde** o schema grava fisicamente os
# MAGIC dados. O resto do código (leituras, transformações, escrita de tabelas)
# MAGIC não muda uma linha.

# COMMAND ----------

def create_schema_sql(schema_name: str) -> str:
    """
    Monta o DDL de CREATE SCHEMA. Se TABLE_TYPE=EXTERNAL, adiciona MANAGED
    LOCATION apontando para STORAGE_ROOT/schema_name/. Se MANAGED (default
    do MVP acadêmico), não define LOCATION nenhuma e deixa o metastore do
    Databricks Free escolher o storage default.
    """
    full_schema = f"{CATALOG}.{schema_name}"
    if TABLE_TYPE == "EXTERNAL":
        location = STORAGE_ROOT.rstrip("/") + f"/{schema_name}/"
        return f"CREATE SCHEMA IF NOT EXISTS {full_schema} MANAGED LOCATION '{location}'"
    return f"CREATE SCHEMA IF NOT EXISTS {full_schema}"


print("Helpers disponíveis: qualified_table(schema, table), volume_path(subpath), create_schema_sql(schema_name)")
