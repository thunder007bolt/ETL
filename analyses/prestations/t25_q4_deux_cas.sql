-- =============================================================================
-- T25 — Question Q4 (pension de la veuve) : une requête par réponse possible
-- Rédigé le 30/10/2026, pas encore exécuté. Le lot 3 B (P51, P52, P58) a donné
-- raison au cas B : t25_requete_proposee.sql compte désormais par demande.
-- But : montrer comment chaque réponse de la CNSS se traduit en SQL.
--   Cas A : la pension de la veuve est ouverte dans un NOUVEAU dossier.
--           On compte des dossiers : c'est la requête T25 proposée.
--   Cas B : la pension de la veuve s'ajoute au dossier DU DÉFUNT.
--           On compte des demandes : une demande = un dossier × une ligne.
-- Les deux requêtes rendent les mêmes colonnes. Seuls les blocs 1 à 3
-- changent ; le bloc 4 (comptages) est le même.
-- Simplifiées pour ne montrer que la différence : sans coordination (règle 1)
-- ni rejets (règle 7), un seul exercice.
-- :1 = CLICHE (MMYYYY) ; :2 = année de l'exercice (ex. 2025).
-- Rang des lignes, comme dans t25_requete_proposee.sql : 1 vieillesse,
-- 2 anticipée, 3 invalidité, 4 survivant, 5 allocation vieillesse,
-- 6 allocation survivant, 7 autres, 8 pas encore de ligne.
-- =============================================================================


-- =============================================================================
-- CAS A — NOUVEAU DOSSIER : une ligne de T25 compte des dossiers
-- =============================================================================
WITH

-- 1. Un dossier → une ligne (sa pension principale) et une date de
--    liquidation (la première notification du dossier).
ligne_dossier AS (
    SELECT d.DOS_CODE, d.RD_ID,
           MIN(CASE
                   WHEN pe.TPE_CODE IS NULL                             THEN 8
                   WHEN pe.TPE_CODE = 'RPV'                             THEN 1
                   WHEN pe.TPE_CODE = 'RPRA'                            THEN 2
                   WHEN pe.TPE_CODE = 'RPI'                             THEN 3
                   WHEN pe.TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA')  THEN 4
                   WHEN pe.TPE_CODE = 'RAV'                             THEN 5
                   WHEN pe.TPE_CODE IN ('RAS', 'RASC', 'RASO', 'RASA')  THEN 6
                   ELSE 7
               END)                                 AS rang,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))      AS date_liquidation
    FROM      DWH.FAIT_DOSSIER        d
    LEFT JOIN DWH.FAIT_PRESTATION_ESP pe ON pe.DOS_CODE = d.DOS_CODE
                                        AND pe.CLICHE   = :1
    WHERE d.CLICHE    = :1
      AND d.TDOS_CODE = 'V'
    GROUP BY d.DOS_CODE, d.RD_ID
),

-- 2. Une réception = un dépôt, sur la ligne du dossier qu'elle a ouvert.
--    Une réception qui n'a ouvert aucun dossier reste sans ligne (rang 8).
depot AS (
    SELECT r.RD_ID,
           TRUNC(r.RD_DATE_RECEPTION)   AS date_depot,
           NVL(ld.rang, 8)              AS rang,
           ld.date_liquidation
    FROM      DWH.FAIT_RECEPTION_DOSSIER r
    LEFT JOIN ligne_dossier              ld ON ld.RD_ID = r.RD_ID
    WHERE r.CLICHE    = :1
      AND r.TDOS_CODE = 'V'
      AND r.RD_DATE_RECEPTION IS NOT NULL
),

-- 3. Un dossier liquidé ; durée depuis la réception qui l'a ouvert.
liq AS (
    SELECT ld.rang,
           ld.date_liquidation,
           GREATEST(ld.date_liquidation - TRUNC(r.RD_DATE_RECEPTION), 0) AS duree
    FROM      ligne_dossier              ld
    LEFT JOIN DWH.FAIT_RECEPTION_DOSSIER r ON r.RD_ID  = ld.RD_ID
                                          AND r.CLICHE = :1
    WHERE ld.date_liquidation IS NOT NULL
),

-- 4. Comptages de l'exercice (identiques dans les deux cas).
dep_n AS (
    SELECT rang,
           SUM(CASE WHEN EXTRACT(YEAR FROM date_depot) = :2 THEN 1 ELSE 0 END) AS deposes,
           SUM(CASE WHEN date_depot <= TO_DATE(:2 || '1231', 'YYYYMMDD')
                     AND (date_liquidation IS NULL
                          OR date_liquidation > TO_DATE(:2 || '1231', 'YYYYMMDD'))
                    THEN 1 ELSE 0 END)                                       AS en_instance
    FROM depot
    GROUP BY rang
),
liq_n AS (
    SELECT rang,
           COUNT(*)                                                 AS liquides,
           SUM(CASE WHEN duree <= 14 THEN 1 ELSE 0 END)             AS nb_moins_15,
           SUM(CASE WHEN duree BETWEEN 15 AND 45 THEN 1 ELSE 0 END) AS nb_15_45,
           SUM(CASE WHEN duree > 45 THEN 1 ELSE 0 END)              AS nb_plus_45,
           ROUND(AVG(duree), 1)                                     AS duree_moyenne
    FROM liq
    WHERE EXTRACT(YEAR FROM date_liquidation) = :2
    GROUP BY rang
)
SELECT l.rang,
       NVL(d.deposes, 0)      AS deposes,
       NVL(q.liquides, 0)     AS liquides,
       q.nb_moins_15, q.nb_15_45, q.nb_plus_45,
       NVL(d.en_instance, 0)  AS en_instance,
       q.duree_moyenne
FROM (SELECT LEVEL AS rang FROM DUAL CONNECT BY LEVEL <= 8) l
LEFT JOIN dep_n d ON d.rang = l.rang
LEFT JOIN liq_n q ON q.rang = l.rang
ORDER BY l.rang;


-- =============================================================================
-- CAS B — MÊME DOSSIER : une ligne de T25 compte des demandes
-- Une demande = un dossier × une ligne. Le dossier d'un pensionné décédé
-- porte deux demandes : sa pension de vieillesse (ligne 1) et la pension de
-- sa veuve (ligne 4), chacune avec sa propre date de liquidation.
-- Si chaque veuve a son propre dossier, dossier × ligne = dossier : le cas B
-- rend alors les mêmes chiffres que le cas A.
-- =============================================================================
WITH

-- 1a. Chaque prestation et sa ligne.
prest AS (
    SELECT pe.DOS_CODE,
           pe.PE_DATE_DEMANDE,
           pe.PE_DATE_NOTIFICATION,
           CASE
               WHEN pe.TPE_CODE = 'RPV'                             THEN 1
               WHEN pe.TPE_CODE = 'RPRA'                            THEN 2
               WHEN pe.TPE_CODE = 'RPI'                             THEN 3
               WHEN pe.TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA')  THEN 4
               WHEN pe.TPE_CODE = 'RAV'                             THEN 5
               WHEN pe.TPE_CODE IN ('RAS', 'RASC', 'RASO', 'RASA')  THEN 6
               ELSE 7
           END                                      AS rang
    FROM DWH.FAIT_PRESTATION_ESP pe
    JOIN DWH.FAIT_DOSSIER        d ON d.DOS_CODE = pe.DOS_CODE
                                  AND d.CLICHE   = :1
    WHERE pe.CLICHE   = :1
      AND d.TDOS_CODE = 'V'
),

-- 1b. Une demande = un dossier × une ligne, liquidée à la première
--     notification de CETTE ligne. Un complément (rang 7 : bonification,
--     majoration) suit la pension du dossier : il ne fait une demande que si
--     le dossier n'a rien d'autre. Veuves et orphelins d'un même défunt sont
--     tous en ligne 4 : une seule demande.
demande AS (
    SELECT DOS_CODE, rang,
           TRUNC(MIN(PE_DATE_DEMANDE))       AS date_demande,
           TRUNC(MIN(PE_DATE_NOTIFICATION))  AS date_liquidation
    FROM (SELECT p.*, MIN(p.rang) OVER (PARTITION BY p.DOS_CODE) AS rang_principal
          FROM prest p)
    WHERE rang < 7 OR rang = rang_principal
    GROUP BY DOS_CODE, rang
),

-- 2. Une réception = un dépôt. Elle a ouvert un dossier, ou elle vise un
--    dossier existant (DOS_CODE de la réception : la veuve qui dépose sur le
--    dossier du défunt). Elle va à la demande qu'elle a fait naître : la
--    première du dossier qu'elle ouvre, ou la première demande du dossier
--    visé liquidée après elle. Sans demande, elle reste sans ligne (rang 8),
--    en instance.
depot AS (
    SELECT RD_ID, DOS_CODE, date_depot, NVL(rang, 8) AS rang, date_liquidation
    FROM (
        SELECT r.RD_ID,
               dm.DOS_CODE,
               TRUNC(r.RD_DATE_RECEPTION)   AS date_depot,
               dm.rang,
               dm.date_liquidation,
               ROW_NUMBER() OVER (PARTITION BY r.RD_ID
                                  ORDER BY dm.date_liquidation NULLS LAST, dm.rang) AS n
        FROM      DWH.FAIT_RECEPTION_DOSSIER r
        LEFT JOIN DWH.FAIT_DOSSIER           o  ON o.RD_ID  = r.RD_ID
                                               AND o.CLICHE = :1
        LEFT JOIN demande                    dm ON dm.DOS_CODE = NVL(o.DOS_CODE, r.DOS_CODE)
                                               AND (o.DOS_CODE IS NOT NULL
                                                    OR dm.date_liquidation IS NULL
                                                    OR dm.date_liquidation >= TRUNC(r.RD_DATE_RECEPTION))
        WHERE r.CLICHE    = :1
          AND r.TDOS_CODE = 'V'
          AND r.RD_DATE_RECEPTION IS NOT NULL
    )
    WHERE n = 1
),

-- 3. Une demande liquidée ; durée depuis sa réception, sinon depuis la date
--    de demande portée sur la prestation.
liq AS (
    SELECT dm.rang,
           dm.date_liquidation,
           GREATEST(dm.date_liquidation - NVL(dp.date_depot, dm.date_demande), 0) AS duree
    FROM demande dm
    LEFT JOIN (SELECT DOS_CODE, rang, MIN(date_depot) AS date_depot
               FROM   depot
               WHERE  DOS_CODE IS NOT NULL
               GROUP BY DOS_CODE, rang) dp
           ON dp.DOS_CODE = dm.DOS_CODE
          AND dp.rang     = dm.rang
    WHERE dm.date_liquidation IS NOT NULL
),

-- 4. Comptages de l'exercice (identiques au cas A).
dep_n AS (
    SELECT rang,
           SUM(CASE WHEN EXTRACT(YEAR FROM date_depot) = :2 THEN 1 ELSE 0 END) AS deposes,
           SUM(CASE WHEN date_depot <= TO_DATE(:2 || '1231', 'YYYYMMDD')
                     AND (date_liquidation IS NULL
                          OR date_liquidation > TO_DATE(:2 || '1231', 'YYYYMMDD'))
                    THEN 1 ELSE 0 END)                                       AS en_instance
    FROM depot
    GROUP BY rang
),
liq_n AS (
    SELECT rang,
           COUNT(*)                                                 AS liquides,
           SUM(CASE WHEN duree <= 14 THEN 1 ELSE 0 END)             AS nb_moins_15,
           SUM(CASE WHEN duree BETWEEN 15 AND 45 THEN 1 ELSE 0 END) AS nb_15_45,
           SUM(CASE WHEN duree > 45 THEN 1 ELSE 0 END)              AS nb_plus_45,
           ROUND(AVG(duree), 1)                                     AS duree_moyenne
    FROM liq
    WHERE EXTRACT(YEAR FROM date_liquidation) = :2
    GROUP BY rang
)
SELECT l.rang,
       NVL(d.deposes, 0)      AS deposes,
       NVL(q.liquides, 0)     AS liquides,
       q.nb_moins_15, q.nb_15_45, q.nb_plus_45,
       NVL(d.en_instance, 0)  AS en_instance,
       q.duree_moyenne
FROM (SELECT LEVEL AS rang FROM DUAL CONNECT BY LEVEL <= 8) l
LEFT JOIN dep_n d ON d.rang = l.rang
LEFT JOIN liq_n q ON q.rang = l.rang
ORDER BY l.rang;
