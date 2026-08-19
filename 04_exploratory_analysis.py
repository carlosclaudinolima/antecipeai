# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Análise Exploratória de Dados (EDA) — Bronze
# MAGIC
# MAGIC Este notebook existe para **documentar, com evidência reproduzível em
# MAGIC PySpark, as descobertas que orientaram as decisões de arquitetura da
# MAGIC camada Silver** — o que vira flag, o que vira tabela separada, e por quê.
# MAGIC Cada achado abaixo foi primeiro identificado com Pandas fora do
# MAGIC Databricks (para validação rápida) e é **repetido aqui em PySpark**,
# MAGIC lendo direto da Bronze, como evidência para a banca.
# MAGIC
# MAGIC **Como ler este notebook:** antes de cada célula de código há uma célula
# MAGIC de markdown explicando o que vamos analisar; depois de cada célula de
# MAGIC código há um comentário com o resultado obtido e o que ele significa
# MAGIC para o projeto.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = qualified_table(SCHEMA_BRONZE, "incidentes")
bronze = spark.table(bronze_table)
total_linhas = bronze.count()
print(f"Tabela: {bronze_table} | linhas: {total_linhas}")

# COMMAND ----------
# Resultado: 122.543 linhas, 19 colunas de negócio + 2 de metadata de
# ingestão (_ingested_at, _source_file). Bate com o dicionário de dados v2
# fornecido pela Locaweb.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Período coberto pelos dados
# MAGIC
# MAGIC Precisamos saber o range de datas para dimensionar a dimensão calendário
# MAGIC da Gold e para entender se temos histórico suficiente para features de
# MAGIC sazonalidade (dia da semana, mês).

# COMMAND ----------

bronze.select(
    F.min("Aberto").alias("primeiro_incidente"),
    F.max("Aberto").alias("ultimo_incidente"),
).show(truncate=False)

# COMMAND ----------
# Resultado: dados de 2023-01-02T20:19:58 até 2025-12-31T23:45:18 — quase 3
# anos de histórico (1.094 dias corridos). Como veremos no item 8, porém,
# nem todo esse período tem volume "real" de negócio: há uma rampa de
# adoção clara ao longo do tempo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Nulos por coluna
# MAGIC
# MAGIC Define quais colunas exigem tratamento de nulo na Silver e quais nulos
# MAGIC são estruturais (fazem parte do processo de negócio) e não devem ser
# MAGIC "corrigidos" às cegas.

# COMMAND ----------

null_pct = bronze.select([
    (F.sum(F.when(F.col(c).isNull() | (F.col(c) == "nan"), 1).otherwise(0)) / total_linhas * 100)
    .alias(c)
    for c in bronze.columns if not c.startswith("_")
])
display(null_pct)

# COMMAND ----------
# Resultado (% de nulos):
#   Produto 63,60% | Categoria 63,42% | Subcategoria 63,42% | Resolvido 67,16%
#   | Código de fechamento 66,70% | Solução 87,51% | Incidente Pai 87,66%
#   | KPI Violado? 79,11% | Item de configuração 1,45% | demais colunas 0%.
# Produto/Categoria/Subcategoria nulos em ~63% das linhas parecem alarmantes
# à primeira vista, mas o item 5 mostra que isso é estrutural (tickets
# abertos por monitoramento automatizado não têm produto associado), não
# um problema de qualidade a "consertar".

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Duplicatas no identificador do incidente

# COMMAND ----------

duplicatas = bronze.groupBy("Número").count().filter("count > 1").count()
print("Grupos de 'Número' com mais de 1 ocorrência:", duplicatas)

# COMMAND ----------
# Resultado: 0 duplicatas. "Número" é uma chave primária confiável — pode
# ser usada como chave de negócio na fato da Gold sem risco de fan-out.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Distribuição de Prioridade e Status

# COMMAND ----------

bronze.groupBy("Prioridade").count().orderBy(F.desc("count")).show()
bronze.groupBy("Status").count().orderBy(F.desc("count")).show()

# COMMAND ----------
# Resultado Prioridade: 4-Baixa 64.828 | 3-Média 41.732 | 2-Alta 15.649 |
# 5-Muito Baixa 333 | 1-Crítica 1.
# Resultado Status: Sem Intervenção 80.373 | Encerrado Automaticamente
# 26.830 | Encerrado 15.339 | Aguardando Problema 1.
# "Sem Intervenção" ser o status mais comum (65,6% da base) é o primeiro
# sinal de que grande parte do volume é ruído de monitoramento automatizado
# — confirmado no item 8.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Regra de negócio "Entrou para KPI?" — validação nos dois sentidos
# MAGIC
# MAGIC O dicionário de dados define a regra: só entram no KPI incidentes com
# MAGIC prioridade 1/2/3, **sem** Incidente Pai preenchido, e com Status
# MAGIC diferente de "Sem Intervenção". Testamos a regra **nos dois sentidos**:
# MAGIC (a) todo `SIM` satisfaz a regra? (b) toda linha que satisfaz a regra
# MAGIC está marcada `SIM`? Validar só o sentido (a), como fizemos numa primeira
# MAGIC passada fora do Databricks, escondeu um problema real.

# COMMAND ----------

regra = bronze \
    .withColumn("prioridade_num", F.regexp_extract("Prioridade", r'^(\d)', 1).cast("int")) \
    .withColumn("tem_incidente_pai", F.col("Incidente Pai").isNotNull()) \
    .withColumn("entrou_kpi_flag", F.col("Entrou para KPI?") == "SIM") \
    .withColumn(
        "kpi_esperado_pela_regra",
        F.col("prioridade_num").isin(1, 2, 3)
        & ~F.col("tem_incidente_pai")
        & (F.col("Status") != "Sem Intervenção"),
    )

sentido_a = regra.filter(F.col("entrou_kpi_flag") & ~F.col("kpi_esperado_pela_regra")).count()
sentido_b = regra.filter(~F.col("entrou_kpi_flag") & F.col("kpi_esperado_pela_regra")).count()
print("SIM mas não deveria (sentido a):", sentido_a)
print("Deveria ser SIM mas está NAO (sentido b):", sentido_b)

# COMMAND ----------
# Resultado: sentido (a) = 0 (todo SIM realmente satisfaz a regra — a flag
# da fonte nunca é "otimista demais"). Sentido (b) = 151 incidentes que
# satisfazem a regra de elegibilidade mas estão marcados NAO — quase todos
# (148 de 151) concentrados em dezembro/2025. Não vamos sobrescrever a flag
# original da Locaweb — mantemos as duas: a flag da fonte
# (`entrou_kpi_flag_fonte`) e a calculada por nós
# (`entrou_kpi_flag_calculada`), com um flag de divergência
# (`kpi_regra_divergente`) para a Silver. É um dado real de inconsistência
# de origem, não um bug nosso — vale reportar à Locaweb.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Nulos de Produto/Categoria correlacionados com "Aberto por"

# COMMAND ----------

bronze.groupBy("Aberto por").agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("Produto").isNull(), 1).otherwise(0)).alias("produto_nulo"),
).show(truncate=False)

# COMMAND ----------
# Resultado: de 104.299 incidentes abertos por "Monitoramento", 77.823
# (74,6%) não têm Produto preenchido. De 18.244 abertos manualmente, só 112
# (0,6%) não têm Produto. Confirma que o nulo em Produto/Categoria é
# estrutural do processo de monitoramento automatizado, não erro de
# digitação — decisão para a Silver: manter nulo (ou mapear para uma
# categoria explícita "NAO_CLASSIFICADO"), nunca imputar um produto
# arbitrário.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cardinalidade das dimensões (dimensiona a Gold)

# COMMAND ----------

for c in ["Produto", "Categoria", "Subcategoria", "Grupo designado", "Item de configuração", "Código de fechamento"]:
    print(c, "->", bronze.select(c).distinct().count(), "valores distintos")

# COMMAND ----------
# Resultado: Produto=51 | Categoria=141 | Subcategoria=447 |
# Grupo designado=17 | Item de configuração=9.171 | Código de fechamento=17.
# Produto e Grupo designado são pequenos o bastante para virar dimensões
# "achatadas" (flat) na Gold; Item de configuração, com 9.171 valores, é
# candidato a ficar só como atributo na fato, não como dimensão própria
# (cardinalidade alta demais para valer a pena).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Consistência do campo `Duração`
# MAGIC
# MAGIC Verificamos se `Duração` é sempre `Resolvido - Aberto` (quando
# MAGIC Resolvido existe) ou `Encerrado - Aberto` (caso contrário) — importante
# MAGIC para decidir se recalculamos esse campo na Silver ou confiamos na
# MAGIC origem.

# COMMAND ----------

dur_check = bronze \
    .withColumn("dt_aberto", F.to_timestamp("Aberto")) \
    .withColumn("dt_resolvido", F.to_timestamp("Resolvido")) \
    .withColumn("dt_encerrado", F.to_timestamp("Encerrado")) \
    .withColumn("duracao_original", F.col("Duração").cast("long")) \
    .withColumn(
        "duracao_recalculada",
        F.when(F.col("dt_resolvido").isNotNull(), F.col("dt_resolvido").cast("long") - F.col("dt_aberto").cast("long"))
         .otherwise(F.col("dt_encerrado").cast("long") - F.col("dt_aberto").cast("long")),
    ) \
    .withColumn("diff_segundos", F.abs(F.col("duracao_recalculada") - F.col("duracao_original")))

print("Linhas com diferença > 60s entre Duração original e recalculada:", dur_check.filter(F.col("diff_segundos") > 60).count())

# COMMAND ----------
# Resultado: 0 linhas com diferença > 60s. O campo Duração da origem é
# 100% consistente com Resolvido/Encerrado - Aberto — não precisamos
# recalculá-lo na Silver, só tipar como long.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Achado — durações extremas não marcadas como violação de SLA
# MAGIC
# MAGIC O dicionário de dados define SLA por prioridade (P1/P2: 4h, P3: 12h,
# MAGIC P4: 24h, P5: 96h). Cruzamos isso com incidentes que entram no KPI: o
# MAGIC quanto eles estouram o SLA, e se isso bate com o campo
# MAGIC `KPI Violado?`.

# COMMAND ----------

sla_check = bronze \
    .withColumn("prioridade_num", F.regexp_extract("Prioridade", r'^(\d)', 1).cast("int")) \
    .withColumn("entrou_kpi_flag", F.col("Entrou para KPI?") == "SIM") \
    .withColumn("duracao_segundos", F.col("Duração").cast("long")) \
    .withColumn(
        "sla_segundos",
        F.when(F.col("prioridade_num").isin(1, 2), 4 * 3600)
         .when(F.col("prioridade_num") == 3, 12 * 3600)
         .when(F.col("prioridade_num") == 4, 24 * 3600)
         .when(F.col("prioridade_num") == 5, 96 * 3600),
    ) \
    .withColumn(
        "duracao_suspeita",
        F.col("entrou_kpi_flag") & (F.col("duracao_segundos") > F.col("sla_segundos") * 10),
    )

total_suspeitos = sla_check.filter(F.col("duracao_suspeita")).count()
print("Total de incidentes com duração > 10x o SLA da prioridade:", total_suspeitos)
sla_check.filter(F.col("duracao_suspeita")).groupBy("KPI Violado?").count().show()

# COMMAND ----------
# Resultado: 2.499 incidentes (de 25.600 que entram no KPI, ~9,8%) duram
# mais de 10x o SLA da sua prioridade — e 2.429 desses (97,2%) estão
# marcados `KPI Violado? = NAO`, o que é contraintuitivo. Não vamos excluir
# essas linhas nem corrigir o campo `KPI Violado?` por conta própria — como
# combinado, viram uma FLAG (`duracao_suspeita`) na Silver, tratadas como
# outliers de qualidade de dados a serem investigados, não removidas. Vale
# reportar à Locaweb como um ponto de atenção do processo de origem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. 🚨 Achado principal — mudança de regime no volume diário de incidentes
# MAGIC
# MAGIC Este é o achado que mais afeta a modelagem: comparamos o volume mensal
# MAGIC bruto (todos os incidentes) contra o volume mensal de incidentes abertos
# MAGIC por "Monitoramento" e contra o volume mensal de incidentes que **entram
# MAGIC no KPI** — que é o que o desafio da Locaweb pede para prever.

# COMMAND ----------

serie = bronze \
    .withColumn("dt_aberto", F.to_timestamp("Aberto")) \
    .withColumn("ano_mes", F.date_format("dt_aberto", "yyyy-MM")) \
    .withColumn("entrou_kpi_flag", F.col("Entrou para KPI?") == "SIM")

print("=== Volume mensal TOTAL (bruto) — últimos meses ===")
serie.groupBy("ano_mes").count().orderBy("ano_mes").filter(F.col("ano_mes") >= "2025-08").show()

print("=== Volume mensal aberto por Monitoramento — últimos meses ===")
serie.filter(F.col("Aberto por") == "Monitoramento").groupBy("ano_mes").count().orderBy("ano_mes").filter(F.col("ano_mes") >= "2025-08").show()

print("=== Volume mensal SOMENTE o que entra no KPI (o que o desafio pede pra prever) ===")
serie.filter(F.col("entrou_kpi_flag")).groupBy("ano_mes").count().orderBy("ano_mes").filter(F.col("ano_mes") >= "2025-01").show(20)

# COMMAND ----------
# Resultado — a descoberta mais importante desta EDA:
#
# Volume bruto salta de ~4.000/mês (ago/2025) para ~21.500-27.300/mês a
# partir de set/2025 (5-7x). Isolando só "Aberto por = Monitoramento", o
# salto é quase idêntico (de ~2.400 para ~20.000-26.500/mês) — ou seja, o
# salto é inteiramente causado por uma ferramenta de monitoramento
# automatizado que passou a abrir (e majoritariamente auto-fechar, status
# "Sem Intervenção") um volume muito maior de tickets a partir de set/2025.
#
# Quando filtramos para SOMENTE o que entra no KPI (o alvo real de
# previsão do desafio), o volume é estável desde jan/2025
# (~1.400-2.400 incidentes/mês), SEM o salto de setembro. O ruído de
# monitoramento não contamina o sinal relevante para o negócio.
#
# Decisão de arquitetura que isso gera para a Silver/Gold: o modelo de
# previsão D+1/D+7 deve ser treinado sobre o volume que entra no KPI
# (estável), não sobre o volume bruto (nao-estacionário por causa de uma
# mudança de ferramenta, não de operação real). O ruído de monitoramento
# vira uma métrica separada de operação da ferramenta, não o alvo do
# modelo preditivo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Também vale registrar: rampa de adoção em 2023-2024
# MAGIC
# MAGIC Além do salto de set/2025, o início da série (2023 a meados de 2024)
# MAGIC tem volume quase nulo — sugere um período de adoção/piloto da
# MAGIC ferramenta de ITSM, não represetativo do volume operacional atual.

# COMMAND ----------

serie.filter(F.col("entrou_kpi_flag")).groupBy("ano_mes").count().orderBy("ano_mes").filter(F.col("ano_mes") < "2025-01").show(30)

# COMMAND ----------
# Resultado: volume mensal (KPI=SIM) sobe de 1-14/mês no início de 2023
# para só ~30-120/mês ao longo de 2024, atingindo o patamar estável
# (~1.400-2.400/mês) apenas a partir de jan/2025. Decisão: o modelo de
# séries temporais deve considerar primariamente a janela jan/2025 em
# diante como "regime operacional real"; 2023-2024 fica disponível na
# Silver/Gold para contexto histórico e storytelling, mas não deve
# dominar o treino do modelo de previsão.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo — decisões de arquitetura que esta EDA gera para a Silver/Gold
# MAGIC
# MAGIC 1. **Duas flags de KPI na Silver**: `entrou_kpi_flag_fonte` (como veio) e
# MAGIC    `entrou_kpi_flag_calculada` (nossa regra), com `kpi_regra_divergente`
# MAGIC    marcando os 151 casos que diferem — sem sobrescrever a origem.
# MAGIC 2. **Flag `duracao_suspeita`** (>10x o SLA da prioridade) em vez de
# MAGIC    excluir ou corrigir os 2.499 casos encontrados.
# MAGIC 3. **Produto/Categoria nulos permanecem nulos** (ou viram
# MAGIC    "NAO_CLASSIFICADO" explícito) — não imputar, é estrutural do processo
# MAGIC    de monitoramento.
# MAGIC 4. **Duração não precisa ser recalculada** — a origem já é consistente.
# MAGIC 5. **O modelo de previsão usa o volume que entra no KPI**, não o volume
# MAGIC    bruto — o salto de set/2025 é artefato de ferramenta, não sinal de
# MAGIC    negócio. Isso vira uma métrica de "ruído de monitoramento" separada,
# MAGIC    não a variável-alvo.
# MAGIC 6. **Produto e Grupo designado** viram dimensões na Gold (baixa
# MAGIC    cardinalidade); **Item de configuração** fica como atributo na fato
# MAGIC    (cardinalidade alta demais para dimensão própria).
