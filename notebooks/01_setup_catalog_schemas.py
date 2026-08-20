# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup do Catalog, Schemas e Volume de Staging
# MAGIC
# MAGIC Cria toda a estrutura do Unity Catalog usada pelo projeto **AntecipeAI**:
# MAGIC
# MAGIC - 1 catalog dedicado (`antecipeai`, configurável no `.env`)
# MAGIC - 4 schemas **irmãos** dentro dele: `landing`, `bronze`, `silver`, `gold`
# MAGIC - 1 Volume (`raw`) dentro do schema `landing`, usado como staging area
# MAGIC   para os arquivos que chegam (hoje: extração única em `.xlsx` da
# MAGIC   Locaweb; futuramente: extrações incrementais)
# MAGIC
# MAGIC **Decisão de arquitetura:** o schema `landing` guarda o Volume de arquivos
# MAGIC crus (staging), separado da camada `bronze` (que já é Delta). Isso não
# MAGIC estava 100% explícito na conversa anterior — se preferirem outro nome ou
# MAGIC organização para esse schema de staging, é só ajustar `SCHEMA_LANDING` no
# MAGIC `.env` e rodar este notebook de novo (é idempotente, usa `IF NOT EXISTS`
# MAGIC em tudo).
# MAGIC
# MAGIC Este notebook é seguro para rodar mais de uma vez (idempotente).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar o catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Catalog '{CATALOG}' pronto e selecionado como catalog ativo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar os schemas (camadas) — landing, bronze, silver, gold
# MAGIC
# MAGIC O DDL exato muda conforme `ANTECIPAI_TABLE_TYPE` no `.env`:
# MAGIC - `MANAGED` (default do MVP): sem `LOCATION`, o metastore decide onde gravar.
# MAGIC - `EXTERNAL`: adiciona `MANAGED LOCATION` apontando para o bucket/container
# MAGIC   configurado em `ANTECIPAI_STORAGE_ROOT` — é a troca que vamos fazer se o
# MAGIC   projeto for aprovado e migrar para S3.

# COMMAND ----------

for schema in [SCHEMA_LANDING, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]:
    ddl = create_schema_sql(schema)
    print(f"Executando: {ddl}")
    spark.sql(ddl)

print("\nSchemas criados/confirmados:", [SCHEMA_LANDING, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar o Volume de staging (`landing.raw`)
# MAGIC
# MAGIC É para dentro deste Volume que o arquivo `LW-DATASET.xlsx` (ou, no futuro,
# MAGIC as extrações incrementais em CSV) deve ser enviado manualmente via
# MAGIC **"Upload to this volume"** na UI do Catalog Explorer, antes de rodar o
# MAGIC notebook `02_bootstrap_landing_convert_xlsx`.

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_LANDING}.{VOLUME_RAW}
""")

print(f"Volume pronto em: {volume_path()}")
print("Envie o arquivo LW-DATASET.xlsx para esse volume (Catalog Explorer > Upload) antes do próximo notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência final da estrutura

# COMMAND ----------

print("=== Schemas em", CATALOG, "===")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

print("=== Volumes em", f"{CATALOG}.{SCHEMA_LANDING}", "===")
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA_LANDING}"))
