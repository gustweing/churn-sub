# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT idPlayer, count(*)
# MAGIC FROM bronze.gamersclub.tb_lobby_stats_player
# MAGIC WHERE month(dtCreatedAt) = 1
# MAGIC GROUP BY idPlayer
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tb_level as (
# MAGIC SELECT idPlayer, vlLevel, row_number() OVER (partition by idPlayer order by dtCreatedAt DESC) as rnPlayer
# MAGIC FROM bronze.gamersclub.tb_lobby_stats_player
# MAGIC WHERE dtCreatedAt < '2022-01-01' and 
# MAGIC dtCreatedAt >= date_add('2022-01-01',-30)
# MAGIC ORDER BY idPlayer, dtCreatedAt
# MAGIC ), 
# MAGIC tb_level_final as (
# MAGIC     SELECT *
# MAGIC     FROM tb_level 
# MAGIC     WHERE rnPlayer = 1
# MAGIC ),
# MAGIC tb_gameplaystats as (
# MAGIC     SELECT idPlayer,
# MAGIC     COUNT(DISTINCT idLobbyGame) as qtdPartidas, 
# MAGIC     COUNT(DISTINCT date(dtCreatedAt)) as qtdDias,
# MAGIC     COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 1 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia01,
# MAGIC     COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 2 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia02,
# MAGIC     COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 3 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia03,
# MAGIC     COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 4 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia04,
# MAGIC     COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 5 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia05,
# MAGIC     COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 6 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia06,
# MAGIC     COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 7 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia07,
# MAGIC     ROUND(COUNT(DISTINCT idLobbyGame)/COUNT(DISTINCT date(dtCreatedAt))) as mediaJogosDia, 
# MAGIC     min(datediff('2022-01-01',dtCreatedAt)) as qtdRecencia, 
# MAGIC     AVG(flWinner) as winRate,
# MAGIC     avg(qtHs / qtKill) as AvghsRate, 
# MAGIC     sum(qtHs) / sum(qtKill) as hsRate, 
# MAGIC     avg((qtKill + qtAssist)/coalesce(qtDeath,1)) as avgKDA, 
# MAGIC     coalesce(sum(qtKill + qtAssist)/sum(coalesce(qtDeath,1)),0) as KDAgeral, 
# MAGIC     avg(coalesce(qtKill,0)/coalesce(qtDeath,1)) as avgKDR, 
# MAGIC     sum(coalesce(qtKill,0))/sum(coalesce(qtDeath,1)) as KDRgeral,
# MAGIC     sum(coalesce(qtHits,0))/sum(coalesce(qtShots,1)) as propAcertoTiro,
# MAGIC     avg(coalesce(qtHits,0)/coalesce(qtShots,1)) as avgpropAcertoTiro, 
# MAGIC     avg(coalesce(vlDamage,0)/coalesce(qtHits,1)) as avgDanoPorTiroAcerto,
# MAGIC     avg(coalesce(vlDamage,0)/coalesce(qtShots,1)) as avgDanoPorTiro,
# MAGIC     AVG(qtKill) as avgqtKill,
# MAGIC     AVG(qtAssist) as avgqtAssist ,
# MAGIC     AVG(qtDeath) as avgqtDeath,
# MAGIC     AVG(qtHs) as avgqtHs,
# MAGIC     AVG(qtBombeDefuse) as avgqtBombeDefuse,
# MAGIC     AVG(qtBombePlant) as avgqtBombePlant,
# MAGIC     AVG(qtTk) as avgqtTk,
# MAGIC     AVG(qtTkAssist) as avgqtTkAssist,
# MAGIC     AVG(qt1Kill) as avgqt1Kill,
# MAGIC     AVG(qt2Kill) as avgqt2Kill,
# MAGIC     AVG(qt3Kill) as avgqt3Kill,
# MAGIC     AVG(qt4Kill) as avgqt4Kill,
# MAGIC     AVG(qt5Kill) as avgqt5Kill,
# MAGIC     AVG(qtPlusKill) as avgqtPlusKill,
# MAGIC     AVG(qtFirstKill) as avgqtFirstKill,
# MAGIC     AVG(vlDamage) as avgvlDamage,
# MAGIC     AVG(qtHits) as avgqtHits,
# MAGIC     AVG(qtShots) as avgqtShots,
# MAGIC     AVG(qtLastAlive) as avgqtLastAlive,
# MAGIC     AVG(qtClutchWon) as avgqtClutchWon,
# MAGIC     AVG(qtRoundsPlayed) as avgqtRoundsPlayed,
# MAGIC     AVG(qtSurvived) as avgqtSurvived,
# MAGIC     AVG(qtTrade) as avgqtTrade,
# MAGIC     AVG(qtFlashAssist) as avgqtFlashAssist,
# MAGIC     COUNT(DISTINCT CASE WHEN descMapName = 'de_ancient' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propAncient,
# MAGIC     COUNT(DISTINCT CASE WHEN descMapName = 'de_overpass' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propOverpass,
# MAGIC     COUNT(DISTINCT CASE WHEN descMapName = 'de_vertigo' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propVertigo,
# MAGIC     COUNT(DISTINCT CASE WHEN descMapName = 'de_nuke' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propNuke,
# MAGIC     COUNT(DISTINCT CASE WHEN descMapName = 'de_train' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propTrain,
# MAGIC     COUNT(DISTINCT CASE WHEN descMapName = 'de_mirage' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propMirage,
# MAGIC     COUNT(DISTINCT CASE WHEN descMapName = 'de_dust2' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propDust2,
# MAGIC     COUNT(DISTINCT CASE WHEN descMapName = 'de_inferno' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propInferno 
# MAGIC     FROM bronze.gamersclub.tb_lobby_stats_player
# MAGIC     WHERE dtCreatedAt < '2022-01-01' and 
# MAGIC     dtCreatedAt >= date_add('2022-01-01',-30)
# MAGIC     GROUP BY idPlayer
# MAGIC )
# MAGIC SELECT '2022-01-01' as dtRef, t2.vlLevel, t1.*
# MAGIC FROM tb_gameplaystats as t1
# MAGIC LEFT JOIN tb_level_final as t2
# MAGIC ON t1.idPlayer = t2.idPlayer
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tb_level as (
# MAGIC SELECT idPlayer, vlLevel, row_number() OVER (partition by idPlayer order by dtCreatedAt DESC) as rnPlayer
# MAGIC FROM bronze.gamersclub.tb_lobby_stats_player
# MAGIC WHERE dtCreatedAt < '2022-01-01' and 
# MAGIC dtCreatedAt >= date_add('2022-01-01',-30)
# MAGIC ORDER BY idPlayer, dtCreatedAt
# MAGIC ), 
# MAGIC tb_level_final as (
# MAGIC     SELECT *
# MAGIC     FROM tb_level 
# MAGIC     WHERE rnPlayer = 1
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM tb_level_final

# COMMAND ----------




# COMMAND ----------



# COMMAND ----------


