# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Camada Silver — Limpeza, Regras de Negócio e Features de ML
# MAGIC
# MAGIC Este notebook lê a Bronze (`bronze.incidentes`) e grava **quatro tabelas
# MAGIC Delta relacionadas** no schema `silver`, decisão tomada deliberadamente
# MAGIC para permitir features personalizadas por modelo, sem forçar um único
# MAGIC grão para tudo:
# MAGIC
# MAGIC | Tabela | Grão | Uso |
# MAGIC |---|---|---|
# MAGIC | `silver.incidentes_tratados` | 1 linha por incidente | Base limpa e tipada, **sem features de ML** — alimenta a Gold |
# MAGIC | `silver.features_calendario` | 1 linha por dia | Atributos de calendário (dia da semana, trimestre, fim de semana...), reutilizável por qualquer segmentação |
# MAGIC | `silver.features_series_produto` | 1 linha por (dia, produto) | Lags/médias móveis/target para modelos segmentados por Produto |
# MAGIC | `silver.features_series_categoria` | 1 linha por (dia, categoria) | Mesma lógica, segmentada por Categoria — outra opção de recorte para o modelo |
# MAGIC
# MAGIC As tabelas de série temporal se relacionam com `features_calendario` pela
# MAGIC chave `data_abertura`, e com `incidentes_tratados` pelas colunas de
# MAGIC segmentação (`produto`, `categoria`) — não duplicamos os atributos de
# MAGIC calendário dentro de cada tabela de série, evitando redundância.
# MAGIC
# MAGIC Todas as decisões de tratamento aqui (o que virou flag, o que ficou nulo
# MAGIC de propósito, por que o filtro de KPI) vêm diretamente dos achados do
# MAGIC notebook `04_exploratory_analysis` — não repetimos a análise aqui, só
# MAGIC aplicamos as decisões.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

bronze_table = qualified_table(SCHEMA_BRONZE, "incidentes")
bronze = spark.table(bronze_table)

DATA_MIN = "2023-01-02"
DATA_MAX = "2025-12-31"

print(f"Lendo {bronze_table}: {bronze.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 1 · `silver.incidentes_tratados`
# MAGIC
# MAGIC Tipagem completa + as flags de qualidade decididas na EDA:
# MAGIC - `entrou_kpi_flag_fonte` / `entrou_kpi_flag_calculada` / `kpi_regra_divergente`
# MAGIC   — mantemos as duas versões da flag de KPI, sem sobrescrever a origem
# MAGIC   (achado: 151 casos divergentes, concentrados em dez/2025).
# MAGIC - `duracao_suspeita` — incidentes com duração > 10x o SLA da prioridade
# MAGIC   mas não marcados como violação (achado: 2.499 casos). Fica como flag,
# MAGIC   não é excluído nem "corrigido".
# MAGIC - `produto`/`categoria` nulos viram `NAO_CLASSIFICADO` explícito (não
# MAGIC   apagamos a informação de que o incidente não tinha classificação —
# MAGIC   isso é estrutural de tickets abertos por monitoramento).

# COMMAND ----------

sla_expr = (
    F.when(F.col("prioridade_num").isin(1, 2), 4 * 3600)
    .when(F.col("prioridade_num") == 3, 12 * 3600)
    .when(F.col("prioridade_num") == 4, 24 * 3600)
    .when(F.col("prioridade_num") == 5, 96 * 3600)
)

_staged = (
    bronze
    .withColumn("prioridade_num", F.regexp_extract("Prioridade", r"^(\d)", 1).cast("int"))
    .withColumn("prioridade_desc", F.trim(F.regexp_extract("Prioridade", r"-\s*(.*)$", 1)))
    .withColumn("dt_aberto", F.to_timestamp("Aberto"))
    .withColumn("dt_resolvido", F.to_timestamp("Resolvido"))
    .withColumn("dt_encerrado", F.to_timestamp("Encerrado"))
    .withColumn("data_abertura", F.to_date("dt_aberto"))
    .withColumn("duracao_segundos", F.col("Duração").cast("long"))
    .withColumn("tem_incidente_pai", F.col("Incidente Pai").isNotNull())
    .withColumn("entrou_kpi_flag_fonte", F.col("Entrou para KPI?") == "SIM")
    .withColumn("kpi_violado_fonte", F.col("KPI Violado?") == "SIM")
    .withColumn("produto", F.coalesce(F.col("Produto"), F.lit("NAO_CLASSIFICADO")))
    .withColumn("categoria", F.coalesce(F.col("Categoria"), F.lit("NAO_CLASSIFICADO")))
)

_staged = (
    _staged
    .withColumn(
        "entrou_kpi_flag_calculada",
        _staged["prioridade_num"].isin(1, 2, 3)
        & ~_staged["tem_incidente_pai"]
        & (_staged["Status"] != "Sem Intervenção"),
    )
    .withColumn("kpi_regra_divergente", F.col("entrou_kpi_flag_fonte") != F.col("entrou_kpi_flag_calculada"))
    .withColumn("sla_segundos", sla_expr)
    .withColumn(
        "duracao_suspeita",
        F.col("entrou_kpi_flag_fonte") & (F.col("duracao_segundos") > F.col("sla_segundos") * 10),
    )
)

incidentes_tratados = _staged.select(
    F.col("Número").alias("numero_incidente"),
    "prioridade_num",
    "prioridade_desc",
    "produto",
    "categoria",
    F.col("Subcategoria").alias("subcategoria"),
    F.col("Grupo designado").alias("grupo_designado"),
    F.col("Item de configuração").alias("item_configuracao"),
    "dt_aberto",
    "dt_resolvido",
    "dt_encerrado",
    "data_abertura",
    "duracao_segundos",
    "sla_segundos",
    F.col("Código de fechamento").alias("codigo_fechamento"),
    F.col("Descrição resumida").alias("descricao_resumida"),
    F.col("Solução").alias("solucao"),
    F.col("Aberto por").alias("aberto_por"),
    "tem_incidente_pai",
    F.col("Status").alias("status"),
    "entrou_kpi_flag_fonte",
    "entrou_kpi_flag_calculada",
    "kpi_regra_divergente",
    "kpi_violado_fonte",
    "duracao_suspeita",
)

(
    incidentes_tratados.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(qualified_table(SCHEMA_SILVER, "incidentes_tratados"))
)

print(f"silver.incidentes_tratados gravada: {incidentes_tratados.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 2 · `silver.features_calendario`
# MAGIC
# MAGIC Grão diário, cobrindo o período completo do histórico
# MAGIC (`2023-01-02` a `2025-12-31`). Serve de base para o cross-join das
# MAGIC tabelas de série temporal (garante que todo dia exista, mesmo sem
# MAGIC incidentes — essencial para não quebrar cálculo de lag/médias móveis)
# MAGIC e pode ser usada por qualquer modelo que precise de atributos de
# MAGIC calendário sem duplicar essa lógica em cada tabela de série.

# COMMAND ----------

calendario_base = (
    spark.range(1)
    .select(F.explode(F.sequence(F.lit(DATA_MIN).cast("date"), F.lit(DATA_MAX).cast("date"))).alias("data_abertura"))
)

features_calendario = (
    calendario_base
    .withColumn("ano", F.year("data_abertura"))
    .withColumn("mes", F.month("data_abertura"))
    .withColumn("dia", F.dayofmonth("data_abertura"))
    .withColumn("trimestre", F.quarter("data_abertura"))
    .withColumn("dia_semana_num", F.dayofweek("data_abertura"))
    .withColumn("dia_semana_nome", F.date_format("data_abertura", "EEEE"))
    .withColumn("is_fim_de_semana", F.col("dia_semana_num").isin(1, 7))
    .withColumn("semana_do_ano", F.weekofyear("data_abertura"))
)

(
    features_calendario.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(qualified_table(SCHEMA_SILVER, "features_calendario"))
)

print(f"silver.features_calendario gravada: {features_calendario.count()} linhas (esperado: 1.095 dias)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas 3 e 4 · Séries temporais segmentadas (Produto e Categoria)
# MAGIC
# MAGIC **Decisão de arquitetura (achado da EDA item 10):** a variável-alvo é a
# MAGIC contagem diária de incidentes que **entram no KPI**
# MAGIC (`entrou_kpi_flag_fonte = true`) — não o volume bruto, que tem uma
# MAGIC mudança de regime em set/2025 causada por uma ferramenta de
# MAGIC monitoramento, não por variação operacional real.
# MAGIC
# MAGIC A função abaixo é parametrizada pela coluna de segmentação, para não
# MAGIC duplicar a lógica entre Produto e Categoria — e para deixar fácil
# MAGIC adicionar uma terceira segmentação (ex.: por `prioridade_num` ou
# MAGIC `grupo_designado`) no futuro, se algum modelo precisar.

# COMMAND ----------

def build_time_series_features(coluna_segmentacao: str) -> "DataFrame":
    """
    Constrói uma tabela de série temporal diária (grão: data + coluna_segmentacao)
    com lags, médias móveis e os targets D+1/D+7, a partir de incidentes_tratados.
    Preenche dias sem incidentes com zero (essencial para lag/rolling não quebrarem).
    """
    base_kpi = _staged.filter(F.col("entrou_kpi_flag_fonte"))

    agg = (
        base_kpi.groupBy("data_abertura", coluna_segmentacao)
        .agg(F.count("*").alias("qtd_incidentes"))
        .repartition(16)
    )

    valores_segmento = agg.select(coluna_segmentacao).distinct()
    grid = calendario_base.crossJoin(valores_segmento).repartition(16)
    full = grid.join(agg, ["data_abertura", coluna_segmentacao], "left").fillna(0, subset=["qtd_incidentes"])

    w = Window.partitionBy(coluna_segmentacao).orderBy("data_abertura")
    w7 = Window.partitionBy(coluna_segmentacao).orderBy("data_abertura").rowsBetween(-6, 0)
    w14 = Window.partitionBy(coluna_segmentacao).orderBy("data_abertura").rowsBetween(-13, 0)

    feat = (
        full
        .withColumn("lag_1d", F.lag("qtd_incidentes", 1).over(w))
        .withColumn("lag_7d", F.lag("qtd_incidentes", 7).over(w))
        .withColumn("lag_14d", F.lag("qtd_incidentes", 14).over(w))
        .withColumn("media_movel_7d", F.round(F.avg("qtd_incidentes").over(w7), 2))
        .withColumn("media_movel_14d", F.round(F.avg("qtd_incidentes").over(w14), 2))
        # targets: o que queremos prever para D+1 e D+7 a partir da linha atual
        .withColumn("target_d1", F.lead("qtd_incidentes", 1).over(w))
        .withColumn("target_d7", F.lead("qtd_incidentes", 7).over(w))
    )
    return feat


features_series_produto = build_time_series_features("produto")
(
    features_series_produto.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(qualified_table(SCHEMA_SILVER, "features_series_produto"))
)
print(f"silver.features_series_produto gravada: {features_series_produto.count()} linhas")

features_series_categoria = build_time_series_features("categoria")
(
    features_series_categoria.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(qualified_table(SCHEMA_SILVER, "features_series_categoria"))
)
print(f"silver.features_series_categoria gravada: {features_series_categoria.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência final

# COMMAND ----------

for t in ["incidentes_tratados", "features_calendario", "features_series_produto", "features_series_categoria"]:
    full_name = qualified_table(SCHEMA_SILVER, t)
    print(f"{full_name}: {spark.table(full_name).count()} linhas")

display(spark.table(qualified_table(SCHEMA_SILVER, "incidentes_tratados")).limit(5))
