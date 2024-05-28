# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze.gamersclub.tb_players
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze.gamersclub.tb_lobby_stats_player
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze.gamersclub.tb_players_medalha
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze.gamersclub.tb_medalha
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze.gamersclub.tb_players_medalha
# MAGIC WHERE idMedal IN (1,3) AND dtCreatedAt < dtExpiration AND dtCreatedAt < dtRemove
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM bronze.gamersclub.tb_players_medalha
# MAGIC WHERE dtCreatedAt < '2022-02-20' AND 
# MAGIC dtRemove > '2022-02-20' AND 
# MAGIC dtCreatedAt < dtRemove AND 
# MAGIC dtCreatedAt < dtExpiration AND
# MAGIC idMedal IN (1,3)

# COMMAND ----------


