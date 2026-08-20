# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ingestão Bronze via Auto Loader
# MAGIC
# MAGIC Lê os arquivos Parquet do diretório de landing
# MAGIC (`/Volumes/antecipeai/landing/raw/incidentes/`) via **Auto Loader**
# MAGIC (`cloudFiles`), que faz leitura incremental — só processa arquivos novos
# MAGIC desde a última execução, controlado por checkpoint.
# MAGIC
# MAGIC **Bronze = "na íntegra"**: nenhum tratamento de tipo, nenhuma limpeza.
# MAGIC Os dados já chegam como string (decisão tomada no notebook `02`), e aqui
# MAGIC só adicionamos metadata técnica de ingestão:
# MAGIC - `_ingested_at`: timestamp de quando a linha entrou na Bronze
# MAGIC - `_source_file`: arquivo de origem de cada linha (rastreabilidade)
# MAGIC
# MAGIC Usamos `.trigger(availableNow=True)`: processa tudo que estiver disponível
# MAGIC agora e para — sem manter um cluster rodando 24/7 (importante pro custo
# MAGIC zero do Databricks Free). Quando novos arquivos chegarem na landing,
# MAGIC basta rodar este notebook de novo.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths de checkpoint, schema location e origem/destino

# COMMAND ----------

from pyspark.sql import functions as F

source_path = volume_path("incidentes")
checkpoint_path = volume_path("_checkpoints/bronze_incidentes")
schema_location = volume_path("_schema/bronze_incidentes")
bronze_table = qualified_table(SCHEMA_BRONZE, "incidentes")

print("Origem (landing):     ", source_path)
print("Checkpoint:            ", checkpoint_path)
print("Schema location:       ", schema_location)
print("Tabela destino (Bronze):", bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura incremental com Auto Loader
# MAGIC
# MAGIC `cloudFiles.inferColumnTypes` fica `false` de propósito — mesmo o
# MAGIC Parquet de origem já vindo 100% string (garantido no notebook `02`),
# MAGIC deixamos explícito aqui que a Bronze nunca deve inferir tipos. Isso é o
# MAGIC que torna essa camada resiliente a mudanças de schema na origem: uma
# MAGIC coluna nova na próxima extração da Locaweb é automaticamente capturada
# MAGIC (schema evolution do Auto Loader), sem quebrar o job.

# COMMAND ----------

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
)

bronze_stream = (
    raw_stream
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita incremental na tabela Delta da Bronze

# COMMAND ----------

query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

query.awaitTermination()

print(f"Ingestão concluída. Linhas na Bronze agora: {spark.table(bronze_table).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

df_bronze = spark.table(bronze_table)
df_bronze.printSchema()
display(df_bronze.limit(10))

# COMMAND ----------
# Resultado esperado (validado com o dataset real fora do Databricks antes de
# escrever este notebook): 122.543 linhas, 19 colunas originais + as 2 colunas
# de metadata (_ingested_at, _source_file) = 21 colunas. Todas as 19 colunas
# originais devem aparecer como StringType — se alguma vier tipada diferente,
# a etapa de conversão do notebook 02 não rodou como esperado.
