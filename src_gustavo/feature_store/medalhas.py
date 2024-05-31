# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT t1.idPlayer, count(DISTINCT t1.idMedal) as qtdDistinctMedalhas,
# MAGIC count(t1.idMedal) as qtMedalha, 
# MAGIC count( CASE WHEN t2.descMedal IN ('Tribo Gaules','Missão da Tribo','#YADINHO - Eu Fui!') then t1.idMedal end ) as qtMedalhaTribo, 
# MAGIC count( CASE WHEN t2.descMedal = 'Experiência de Batalha' then t1.idMedal end) as qtExperienciaBatalha, 
# MAGIC count( CASE WHEN t2.descMedal IN ('Membro Premium','Membro Plus') then t1.idMedal end) as qtdAssinatura,
# MAGIC count( CASE WHEN t2.descMedal = 'Membro Premium' then t1.idMedal end) as qtdPremium,
# MAGIC count( CASE WHEN t2.descMedal = 'Membro Plus' then t1.idMedal end) as qtdPlus,
# MAGIC max( CASE WHEN t2.descMedal IN ('Membro Premium','Membro Plus') and coalesce(t1.dtRemove,now()) > '2022-01-01' then 1 else 0 end) as flAssinante
# MAGIC FROM bronze.gamersclub.tb_players_medalha as t1
# MAGIC LEFT JOIN bronze.gamersclub.tb_medalha as t2
# MAGIC ON t1.idMedal = t2.idMedal
# MAGIC WHERE t1.dtCreatedAt < t1.dtExpiration and 
# MAGIC t1.dtCreatedAt < coalesce(t1.dtRemove,now())
# MAGIC and t1.dtCreatedAt < '2022-01-01'ddd
# MAGIC GROUP BY t1.idPlayer

# COMMAND ----------

# MAGIC %md
# MAGIC
