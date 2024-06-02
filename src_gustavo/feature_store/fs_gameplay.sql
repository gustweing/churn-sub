%sql
WITH tb_level as (
SELECT idPlayer, vlLevel, row_number() OVER (partition by idPlayer order by dtCreatedAt DESC) as rnPlayer
FROM bronze.gamersclub.tb_lobby_stats_player
WHERE dtCreatedAt < '{date}' and 
dtCreatedAt >= date_add('{date}',-30)
ORDER BY idPlayer, dtCreatedAt
), 
tb_level_final as (
    SELECT *
    FROM tb_level 
    WHERE rnPlayer = 1
),
tb_gameplaystats as (
    SELECT idPlayer,
    COUNT(DISTINCT idLobbyGame) as qtdPartidas, 
    COUNT(DISTINCT date(dtCreatedAt)) as qtdDias,
    COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 1 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia01,
    COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 2 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia02,
    COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 3 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia03,
    COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 4 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia04,
    COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 5 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia05,
    COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 6 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia06,
    COUNT(DISTINCT CASE WHEN dayofweek(dtCreatedAt) = 7 THEN date(dtCreatedAt) END)/COUNT(DISTINCT date(dtCreatedAt)) as propDia07,
    ROUND(COUNT(DISTINCT idLobbyGame)/COUNT(DISTINCT date(dtCreatedAt))) as mediaJogosDia, 
    min(datediff('{date}',dtCreatedAt)) as qtdRecencia, 
    AVG(flWinner) as winRate,
    avg(qtHs / qtKill) as AvghsRate, 
    sum(qtHs) / sum(qtKill) as hsRate, 
    avg((qtKill + qtAssist)/coalesce(qtDeath,1)) as avgKDA, 
    coalesce(sum(qtKill + qtAssist)/sum(coalesce(qtDeath,1)),0) as KDAgeral, 
    avg(coalesce(qtKill,0)/coalesce(qtDeath,1)) as avgKDR, 
    sum(coalesce(qtKill,0))/sum(coalesce(qtDeath,1)) as KDRgeral,
    sum(coalesce(qtHits,0))/sum(coalesce(qtShots,1)) as propAcertoTiro,
    avg(coalesce(qtHits,0)/coalesce(qtShots,1)) as avgpropAcertoTiro, 
    avg(coalesce(vlDamage,0)/coalesce(qtHits,1)) as avgDanoPorTiroAcerto,
    avg(coalesce(vlDamage,0)/coalesce(qtShots,1)) as avgDanoPorTiro,
    AVG(qtKill) as avgqtKill,
    AVG(qtAssist) as avgqtAssist ,
    AVG(qtDeath) as avgqtDeath,
    AVG(qtHs) as avgqtHs,
    AVG(qtBombeDefuse) as avgqtBombeDefuse,
    AVG(qtBombePlant) as avgqtBombePlant,
    AVG(qtTk) as avgqtTk,
    AVG(qtTkAssist) as avgqtTkAssist,
    AVG(qt1Kill) as avgqt1Kill,
    AVG(qt2Kill) as avgqt2Kill,
    AVG(qt3Kill) as avgqt3Kill,
    AVG(qt4Kill) as avgqt4Kill,
    AVG(qt5Kill) as avgqt5Kill,
    AVG(qtPlusKill) as avgqtPlusKill,
    AVG(qtFirstKill) as avgqtFirstKill,
    AVG(vlDamage) as avgvlDamage,
    AVG(qtHits) as avgqtHits,
    AVG(qtShots) as avgqtShots,
    AVG(qtLastAlive) as avgqtLastAlive,
    AVG(qtClutchWon) as avgqtClutchWon,
    AVG(qtRoundsPlayed) as avgqtRoundsPlayed,
    AVG(qtSurvived) as avgqtSurvived,
    AVG(qtTrade) as avgqtTrade,
    AVG(qtFlashAssist) as avgqtFlashAssist,
    COUNT(DISTINCT CASE WHEN descMapName = 'de_ancient' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propAncient,
    COUNT(DISTINCT CASE WHEN descMapName = 'de_overpass' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propOverpass,
    COUNT(DISTINCT CASE WHEN descMapName = 'de_vertigo' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propVertigo,
    COUNT(DISTINCT CASE WHEN descMapName = 'de_nuke' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propNuke,
    COUNT(DISTINCT CASE WHEN descMapName = 'de_train' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propTrain,
    COUNT(DISTINCT CASE WHEN descMapName = 'de_mirage' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propMirage,
    COUNT(DISTINCT CASE WHEN descMapName = 'de_dust2' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propDust2,
    COUNT(DISTINCT CASE WHEN descMapName = 'de_inferno' THEN idLobbyGame end)/COUNT(DISTINCT idLobbyGame) as propInferno 
    FROM bronze.gamersclub.tb_lobby_stats_player
    WHERE dtCreatedAt < '{date}' and 
    dtCreatedAt >= date_add('{date}',-30)
    GROUP BY idPlayer
)
SELECT '{date}' as dtRef, t2.vlLevel, t1.*
FROM tb_gameplaystats as t1
LEFT JOIN tb_level_final as t2
ON t1.idPlayer = t2.idPlayer
LIMIT 5