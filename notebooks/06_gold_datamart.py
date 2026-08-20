# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Camada Gold — Star Schema (Datamart)
# MAGIC
# MAGIC Constrói o datamart em modelo dimensional (Star Schema) a partir de
# MAGIC `silver.incidentes_tratados` — **sem** as features de ML (lags, médias
# MAGIC móveis), como combinado: essas ficam só nas tabelas de série temporal da
# MAGIC Silver, que alimentam o treino dos modelos, não o BI executivo.
# MAGIC
# MAGIC **Modelo dimensional:**
# MAGIC
# MAGIC | Tabela | Tipo | Grão / Chave |
# MAGIC |---|---|---|
# MAGIC | `gold.dim_data` | Dimensão | 1 linha por dia (`data_sk` = yyyyMMdd) |
# MAGIC | `gold.dim_produto` | Dimensão | 1 linha por produto (`produto_sk`) |
# MAGIC | `gold.dim_categoria` | Dimensão | 1 linha por categoria (`categoria_sk`) |
# MAGIC | `gold.dim_equipe` | Dimensão | 1 linha por grupo designado (`equipe_sk`) |
# MAGIC | `gold.dim_prioridade` | Dimensão | 1 linha por prioridade, já carrega o SLA (`prioridade_num`) |
# MAGIC | `gold.fato_incidentes` | Fato | 1 linha por incidente, FKs para as 5 dimensões |
# MAGIC | `gold.fato_incidentes_diario` | Fato agregada | 1 linha por (dia, produto, categoria, prioridade) — para Power BI via DirectQuery, sem precisar agregar 122k linhas em tempo real a cada refresh de dashboard |
# MAGIC
# MAGIC **Por que `Item de configuração` e `Subcategoria` não viraram dimensão
# MAGIC própria:** cardinalidade alta demais (9.171 e 447 valores distintos,
# MAGIC respectivamente — achados no notebook `04`), ficam como atributo
# MAGIC descritivo direto na fato (dimensão degenerada), evitando uma dimensão
# MAGIC quase do tamanho da própria fato.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

silver_incidentes = spark.table(qualified_table(SCHEMA_SILVER, "incidentes_tratados"))
print(f"Lendo silver.incidentes_tratados: {silver_incidentes.count()} linhas")

DATA_MIN = "2023-01-02"
DATA_MAX = "2025-12-31"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões

# COMMAND ----------

dim_data = (
    spark.range(1)
    .select(F.explode(F.sequence(F.lit(DATA_MIN).cast("date"), F.lit(DATA_MAX).cast("date"))).alias("data"))
    .withColumn("data_sk", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("dia_semana_num", F.dayofweek("data"))
    .withColumn("dia_semana_nome", F.date_format("data", "EEEE"))
    .withColumn("is_fim_de_semana", F.col("dia_semana_num").isin(1, 7))
)

dim_produto = (
    silver_incidentes.select("produto").distinct()
    .withColumn("produto_sk", F.row_number().over(Window.orderBy("produto")))
    .select("produto_sk", "produto")
)

dim_categoria = (
    silver_incidentes.select("categoria").distinct()
    .withColumn("categoria_sk", F.row_number().over(Window.orderBy("categoria")))
    .select("categoria_sk", "categoria")
)

dim_equipe = (
    silver_incidentes.select("grupo_designado").distinct()
    .withColumn("equipe_sk", F.row_number().over(Window.orderBy("grupo_designado")))
    .select("equipe_sk", "grupo_designado")
)

dim_prioridade = silver_incidentes.select("prioridade_num", "prioridade_desc", "sla_segundos").distinct()

for nome, df in [
    ("dim_data", dim_data), ("dim_produto", dim_produto), ("dim_categoria", dim_categoria),
    ("dim_equipe", dim_equipe), ("dim_prioridade", dim_prioridade),
]:
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(qualified_table(SCHEMA_GOLD, nome)))
    print(f"gold.{nome} gravada: {df.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato — `gold.fato_incidentes`
# MAGIC
# MAGIC Grão: 1 linha por incidente. Junta com as dimensões para trazer as
# MAGIC surrogate keys; mantém os atributos de cardinalidade alta
# MAGIC (`subcategoria`, `item_configuracao`) direto na fato.

# COMMAND ----------

fato_incidentes = (
    silver_incidentes
    .join(dim_produto, "produto", "left")
    .join(dim_categoria, "categoria", "left")
    .join(dim_equipe, "grupo_designado", "left")
    .withColumn("data_sk", F.date_format("data_abertura", "yyyyMMdd").cast("int"))
    .select(
        "numero_incidente", "data_sk", "produto_sk", "categoria_sk", "equipe_sk", "prioridade_num",
        "subcategoria", "item_configuracao", "status", "aberto_por",
        "duracao_segundos", "tem_incidente_pai",
        "entrou_kpi_flag_fonte", "entrou_kpi_flag_calculada", "kpi_regra_divergente",
        "kpi_violado_fonte", "duracao_suspeita",
        "dt_aberto", "dt_resolvido", "dt_encerrado",
    )
)

(
    fato_incidentes.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(qualified_table(SCHEMA_GOLD, "fato_incidentes"))
)

print(f"gold.fato_incidentes gravada: {fato_incidentes.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checagem de integridade referencial
# MAGIC
# MAGIC Antes de considerar a Gold pronta, validamos que toda linha da fato tem
# MAGIC correspondência nas dimensões — nenhuma chave estrangeira "solta"
# MAGIC (o que quebraria os relacionamentos no Power BI).

# COMMAND ----------

gold_fato = spark.table(qualified_table(SCHEMA_GOLD, "fato_incidentes"))
gold_dim_data = spark.table(qualified_table(SCHEMA_GOLD, "dim_data"))
gold_dim_produto = spark.table(qualified_table(SCHEMA_GOLD, "dim_produto"))

orfaos_data = gold_fato.join(gold_dim_data, "data_sk", "left_anti").count()
orfaos_produto = gold_fato.join(gold_dim_produto, "produto_sk", "left_anti").count()

print("Linhas da fato sem correspondência em dim_data:", orfaos_data)
print("Linhas da fato sem correspondência em dim_produto:", orfaos_produto)

assert orfaos_data == 0, "Integridade referencial quebrada em dim_data!"
assert orfaos_produto == 0, "Integridade referencial quebrada em dim_produto!"
print("Integridade referencial OK.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato agregada — `gold.fato_incidentes_diario`
# MAGIC
# MAGIC Pré-agrega por (dia, produto, categoria, prioridade) — grão pensado
# MAGIC para alimentar diretamente os painéis do Power BI (Dashboard Principal e
# MAGIC Gráfico de Previsão de Incidentes, do protótipo da Sprint 2) via
# MAGIC DirectQuery, sem precisar agregar a fato granular a cada refresh.

# COMMAND ----------

# COMMAND ----------

fato_incidentes_diario = (
    silver_incidentes.filter(F.col("entrou_kpi_flag_fonte"))
    .groupBy("data_abertura", "produto", "categoria", "prioridade_num")
    .agg(
        F.count("*").alias("qtd_incidentes"),
        F.sum(F.when(F.col("kpi_violado_fonte"), 1).otherwise(0)).alias("qtd_kpi_violado"),
        F.round(F.avg("duracao_segundos"), 1).alias("duracao_media_segundos"),
    )
    .withColumn("data_sk", F.date_format("data_abertura", "yyyyMMdd").cast("int"))
    .join(dim_produto, "produto", "left")
    .join(dim_categoria, "categoria", "left")
    .select(
        "data_sk", "produto_sk", "categoria_sk", "prioridade_num",
        "qtd_incidentes", "qtd_kpi_violado", "duracao_media_segundos",
    )
)

(
    fato_incidentes_diario.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(qualified_table(SCHEMA_GOLD, "fato_incidentes_diario"))
)

print(f"gold.fato_incidentes_diario gravada: {fato_incidentes_diario.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência final

# COMMAND ----------

for t in ["dim_data", "dim_produto", "dim_categoria", "dim_equipe", "dim_prioridade", "fato_incidentes", "fato_incidentes_diario"]:
    full_name = qualified_table(SCHEMA_GOLD, t)
    print(f"{full_name}: {spark.table(full_name).count()} linhas")

display(spark.table(qualified_table(SCHEMA_GOLD, "fato_incidentes")).limit(5))
