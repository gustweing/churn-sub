%sql
with tb_assinaturas as 
(
SELECT t1.*, t2.descMedal
FROM bronze.gamersclub.tb_players_medalha as t1
left join bronze.gamersclub.tb_medalha t2
on t1.idMedal = t2.idMedal
WHERE t1.dtCreatedAt < t1.dtExpiration and 
t1.dtCreatedAt < coalesce(t1.dtRemove,t1.dtExpiration, now())
and t1.dtCreatedAt < {date}
and coalesce(t1.dtRemove,t1.dtExpiration,now() ) > {date}
), 
tb_assinaturas_rn as 
(
  SELECT *, row_number() over (partition by idPlayer order by dtExpiration DESC) as rn_assinatura
  FROM tb_assinaturas
),
tb_assinatura_sumario as (
  SELECT *,
  datediff({date},dtCreatedAt) as qtdDiasAssinatura,
  datediff(dtExpiration,{date} ) as qtdDiasExpiracao
  from tb_assinaturas_rn
  WHERE rn_assinatura = 1
), 
tb_assinatura_historica as (
SELECT 
t1.idPlayer,
count(t1.idMedal) as qtdAssinatura,
count(CASE WHEN t2.descMedal = 'Membro Premium' then t1.idMedal end) as qtdPremium,
count(CASE WHEN t2.descMedal = 'Membro Plus' then t1.idMedal end) as qtdPlus
FROM bronze.gamersclub.tb_players_medalha as t1
LEFT JOIN bronze.gamersclub.tb_medalha as t2
ON t1.idMedal = t2.idMedal
WHERE t1.dtCreatedAt < t1.dtExpiration and 
t1.dtCreatedAt < coalesce(t1.dtRemove,now())
and t1.dtCreatedAt < {date}
and coalesce(t1.dtRemove,now() ) > {date}
and t2.descMedal in ('Membro Premium','Membro Plus')
GROUP BY t1.idPlayer
)
SELECT {date} as dtRef,
t1.idPlayer, 
t1.descMedal, 
1 as flAssinatura,
t1.qtdDiasExpiracao, 
t1.qtdDiasAssinatura, 
t2.qtdAssinatura, 
t2.qtdPremium, 
t2.qtdPlus
FROM tb_assinatura_sumario t1
left join tb_assinatura_historica t2
on t1.idPlayer = t2.idPlayer