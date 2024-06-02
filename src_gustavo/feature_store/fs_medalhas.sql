%sql
SELECT '{date}' as dtRef,
t1.idPlayer, count(DISTINCT t1.idMedal) as qtdDistinctMedalhas,
count(t1.idMedal) as qtMedalha, 
count( CASE WHEN t2.descMedal IN ('Tribo Gaules','Missão da Tribo','#YADINHO - Eu Fui!') then t1.idMedal end ) as qtMedalhaTribo, 
count( CASE WHEN t2.descMedal = 'Experiência de Batalha' then t1.idMedal end) as qtExperienciaBatalha,
count( CASE WHEN t2.descMedal = 'Terminou o tutorial da Gamers Club' then t1.idMedal end) as qtTerminouTutorial,
count( CASE WHEN t2.descMedal = 'Já conquistou 10 Vitórias' then t1.idMedal end) as qt10Vitorias,
count( CASE WHEN t2.descMedal = 'Já conquistou 25 Vitórias' then t1.idMedal end) as qt25Vitorias
FROM bronze.gamersclub.tb_players_medalha as t1
LEFT JOIN bronze.gamersclub.tb_medalha as t2
ON t1.idMedal = t2.idMedal
WHERE t1.dtCreatedAt < t1.dtExpiration and 
t1.dtCreatedAt < coalesce(t1.dtRemove,now())
and t1.dtCreatedAt < '{date}'
GROUP BY t1.idPlayer
