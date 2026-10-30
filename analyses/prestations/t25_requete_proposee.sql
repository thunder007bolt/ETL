-- =============================================================================
-- T25 — Dossiers de prestations PVID de l'exercice selon la durée de liquidation
-- PROPOSITION du 27/10/2026, revue le 30/10/2026 après le lot 3 B
-- (lot3b_controles_t25.sql), à valider : traduit les règles 1 à 10 de la fiche
-- Excalidraw « T25 dossiers de pension ».
-- Sources : DWH.FAIT_RECEPTION_DOSSIER, FAIT_DOSSIER, FAIT_PRESTATION_ESP,
--           FAIT_DOSSIER_REJETE (copies des tables de USER_DWH).
-- Grain   : ANNEE × ligne CIPRES. Rangs 1 à 7 = lignes de T25 ; rang 8 = pas
--           encore de ligne (demande sans prestation), compté dans le Total
--           mais pas affiché ; rang 9 = Total, recalculé sur toutes les demandes
--           (jamais une moyenne de moyennes).
-- On compte des DEMANDES : une demande = un dossier × une ligne. Le dossier
-- d'un pensionné décédé porte sa pension (ligne 1) et la réversion de sa veuve
-- et de ses orphelins (ligne 4), chacune avec son dépôt et sa liquidation
-- (P51, P52, P54, P58). Un dossier qui n'a qu'une ligne est une seule demande.
-- :1      = CLICHE (MMYYYY). Exercices calculés : les 7 années closes avant
--           l'année du cliché (cliché 092026 → 2019 à 2025).
-- Choix encore à valider (fiche T25) : COOR_AC gardés (Q1), début du stock en
-- instance (Q3, paramètre debut_stock), réversion sans réception datée par sa
-- demande (Q4).
-- =============================================================================

WITH

-- ── 1. Exercices : les 7 années closes, plus la précédente ──────────────────
-- L'année la plus ancienne ne sert qu'au stock en instance au 1er janvier.
annees AS (
    SELECT TO_NUMBER(SUBSTR(:1, 3, 4)) - LEVEL AS annee
    FROM   DUAL
    CONNECT BY LEVEL <= 8
),

-- Règle 6 : les réceptions reçues avant cette date et jamais reliées à un
-- dossier ne sont pas des demandes en attente : le lien entre les anciens
-- dossiers et leur réception n'a pas été saisi (P48, P50). Date à fixer avec
-- la direction des prestations (Q3).
parametres AS (
    SELECT DATE '2016-01-01' AS debut_stock FROM DUAL
),

-- ── 2. Prestations et leur ligne CIPRES ─────────────────────────────────────
-- Règle 1 : dossiers de pension (V), sans les dossiers transférés à une autre
--           caisse (CNSS_AC). Les dossiers de coordination payés par une autre
--           caisse (COOR_AC) restent : la CNSS les reçoit et les notifie (P48).
prest AS (
    SELECT pe.DOS_CODE,
           d.DOS_STATUT,
           CASE
               WHEN pe.TPE_CODE = 'RPV'                            THEN 1
               WHEN pe.TPE_CODE = 'RPRA'                           THEN 2
               WHEN pe.TPE_CODE = 'RPI'                            THEN 3
               WHEN pe.TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA') THEN 4
               WHEN pe.TPE_CODE = 'RAV'                            THEN 5
               WHEN pe.TPE_CODE IN ('RAS', 'RASC', 'RASO', 'RASA') THEN 6
               ELSE 7
           END                                  AS rang,
           pe.PE_DATE_DEMANDE,
           pe.PE_DATE_NOTIFICATION
    FROM DWH.FAIT_PRESTATION_ESP pe
    JOIN DWH.FAIT_DOSSIER        d ON d.DOS_CODE = pe.DOS_CODE
                                  AND d.CLICHE   = :1
    WHERE pe.CLICHE   = :1
      AND d.TDOS_CODE = 'V'
      AND NVL(d.DOS_CATEGORIE, 'ORDINAIRE') <> 'CNSS_AC'
),

-- ── 3. Demandes : un dossier × une ligne ────────────────────────────────────
-- La demande principale d'un dossier est celle du plus petit rang. Un
-- complément (rang 7 : bonification, majoration…) suit la pension du dossier :
-- il ne fait une demande que si le dossier n'a rien d'autre. Veuves et
-- orphelins d'un même défunt sont tous en ligne 4 : une seule demande.
-- Règle 3 : date de liquidation = première notification des droits de la
--           demande (pas du dossier : sinon la réversion passe pour une
--           révision, P54).
demande AS (
    SELECT DOS_CODE,
           DOS_STATUT,
           rang,
           CASE WHEN rang = rang_principal THEN 1 ELSE 0 END  AS principale,
           TRUNC(MIN(PE_DATE_DEMANDE))                        AS date_demande,
           TRUNC(MIN(PE_DATE_NOTIFICATION))                   AS date_liquidation
    FROM (SELECT p.*, MIN(p.rang) OVER (PARTITION BY p.DOS_CODE) AS rang_principal
          FROM   prest p)
    WHERE rang < 7 OR rang = rang_principal
    GROUP BY DOS_CODE, DOS_STATUT, rang,
             CASE WHEN rang = rang_principal THEN 1 ELSE 0 END
),

-- ── 4. Réceptions rattachées à leur demande ─────────────────────────────────
-- Règle 2 : une réception au guichet = un dépôt.
--   · Une réception qui a ouvert un dossier (type NOUVEAU) va à la demande
--     principale de ce dossier.
--   · Une réception déposée sur un dossier existant (type ANCIEN, DOS_CODE de
--     la réception) va à une autre demande de ce dossier : la réversion (94 %
--     des cas, P58). De préférence la première liquidée après la réception,
--     sinon une demande pas encore liquidée.
--   · Sans demande, la réception reste sans ligne (rang 8).
recep_cand AS (
    SELECT r.RD_ID,
           TRUNC(r.RD_DATE_RECEPTION)                         AS date_reception,
           CASE WHEN o.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END AS ouvre_dossier,
           dd.DOS_CODE,
           dd.DOS_STATUT,
           dm.rang,
           dm.date_liquidation,
           ROW_NUMBER() OVER (
               PARTITION BY r.RD_ID
               ORDER BY CASE WHEN dm.date_liquidation IS NULL
                               OR dm.date_liquidation >= TRUNC(r.RD_DATE_RECEPTION)
                             THEN 0 ELSE 1 END,
                        dm.date_liquidation, dm.rang, dd.DOS_CODE)  AS n
    FROM      DWH.FAIT_RECEPTION_DOSSIER r
    LEFT JOIN DWH.FAIT_DOSSIER           o  ON o.RD_ID     = r.RD_ID
                                           AND o.CLICHE    = :1
    LEFT JOIN DWH.FAIT_DOSSIER           dd ON dd.DOS_CODE = NVL(o.DOS_CODE, r.DOS_CODE)
                                           AND dd.CLICHE   = :1
    LEFT JOIN demande                    dm ON dm.DOS_CODE   = dd.DOS_CODE
                                           AND dm.principale = CASE WHEN o.DOS_CODE IS NOT NULL
                                                                    THEN 1 ELSE 0 END
    WHERE r.CLICHE    = :1
      AND r.TDOS_CODE = 'V'
      AND r.RD_DATE_RECEPTION IS NOT NULL
      AND NVL(dd.DOS_CATEGORIE, 'ORDINAIRE') <> 'CNSS_AC'
),

-- Une réception ne va qu'à une demande, même si elle a ouvert deux dossiers
-- (11 cas, P52). Une demande ne compte qu'un dépôt, le premier : la veuve et
-- les orphelins d'un même défunt font un seul dépôt.
recep AS (
    SELECT RD_ID, date_reception, ouvre_dossier, DOS_CODE, DOS_STATUT,
           NVL(rang, 8) AS rang, date_liquidation,
           CASE WHEN rang IS NULL THEN 1
                ELSE ROW_NUMBER() OVER (PARTITION BY DOS_CODE, rang
                                        ORDER BY date_reception, RD_ID)
           END          AS k
    FROM   recep_cand
    WHERE  n = 1
),

-- ── 5. Dépôts ───────────────────────────────────────────────────────────────
-- Les réceptions, plus les réversions liquidées dont la demande n'a pas été
-- enregistrée au guichet : elles prennent la date de demande de la pension
-- comme date de dépôt (Q4).
depot AS (
    SELECT RD_ID, date_reception AS date_depot, ouvre_dossier, DOS_CODE, DOS_STATUT,
           rang, date_liquidation
    FROM   recep
    WHERE  k = 1
    UNION ALL
    SELECT NULL, dm.date_demande, 0, dm.DOS_CODE, dm.DOS_STATUT,
           dm.rang, dm.date_liquidation
    FROM   demande dm
    WHERE  dm.principale = 0
      AND  dm.rang IN (4, 6)
      AND  dm.date_demande     IS NOT NULL
      AND  dm.date_liquidation IS NOT NULL
      AND  NOT EXISTS (SELECT 1
                       FROM   recep x
                       WHERE  x.k        = 1
                         AND  x.DOS_CODE = dm.DOS_CODE
                         AND  x.rang     = dm.rang)
),

-- ── 6. Demandes liquidées et durée de liquidation ───────────────────────────
-- Règle 4 : durée = jours entre le dépôt (sinon la date de demande) et la
--           première notification ; une durée négative (anomalie de saisie)
--           est ramenée à 0.
liq AS (
    SELECT dm.rang,
           EXTRACT(YEAR FROM dm.date_liquidation)                            AS annee,
           GREATEST(dm.date_liquidation - NVL(dp.date_depot, dm.date_demande), 0) AS duree
    FROM      demande dm
    LEFT JOIN depot   dp ON dp.DOS_CODE = dm.DOS_CODE
                        AND dp.rang     = dm.rang
    WHERE dm.date_liquidation IS NOT NULL
),

-- ── 7. Rejets enregistrés ───────────────────────────────────────────────────
-- Règle 7 : année de la date de rejet ; ligne de la demande principale du
--           dossier s'il existe. Seul le module des rejets note les refus
--           (P55 à P57).
rej AS (
    SELECT NVL(dm.rang, 8)              AS rang,
           TRUNC(dr.DOSRE_DATE_REJET)   AS date_rejet,
           dr.RD_ID
    FROM      DWH.FAIT_DOSSIER_REJETE dr
    LEFT JOIN demande                 dm ON dm.DOS_CODE   = dr.DOS_CODE
                                        AND dm.principale = 1
    WHERE dr.CLICHE    = :1
      AND dr.TDOS_CODE = 'V'
      AND dr.DOSRE_DATE_REJET IS NOT NULL
),

-- ── 8. Stock en instance au 31 décembre de chaque année ─────────────────────
-- Règle 6 : déposé au plus tard le 31/12, pas encore liquidé à cette date, et
--           sans rejet enregistré à cette date. Ne sont pas en attente :
--           · un dossier fermé jamais notifié (pension radiée dont la
--             notification n'a pas été saisie, P57) ;
--           · une réception d'avant debut_stock jamais reliée à un dossier.
instance_fin AS (
    SELECT a.annee, dp.rang, COUNT(*) AS nb
    FROM   annees     a
    CROSS JOIN parametres pr
    JOIN   depot      dp ON dp.date_depot <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
    WHERE (dp.date_liquidation IS NULL
           OR dp.date_liquidation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
      AND NOT (dp.ouvre_dossier = 1
               AND dp.DOS_STATUT = 'F'
               AND dp.date_liquidation IS NULL)
      AND NOT (dp.DOS_CODE IS NULL
               AND dp.date_depot < pr.debut_stock)
      AND NOT EXISTS (SELECT 1
                      FROM   rej x
                      WHERE  x.RD_ID      = dp.RD_ID
                        AND  x.date_rejet <= TO_DATE(a.annee || '1231', 'YYYYMMDD'))
    GROUP BY a.annee, dp.rang
),

-- ── 9. Comptages par année et par ligne, plus le Total (rang 9) ─────────────
depose_agg AS (
    SELECT EXTRACT(YEAR FROM date_depot) AS annee, rang, COUNT(*) AS nb
    FROM   depot
    GROUP BY EXTRACT(YEAR FROM date_depot), rang
    UNION ALL
    SELECT EXTRACT(YEAR FROM date_depot), 9, COUNT(*)
    FROM   depot
    GROUP BY EXTRACT(YEAR FROM date_depot)
),

-- Règles 5 et 8 : tranches 0–14 / 15–45 / 46 j et plus ; moyenne et médiane.
liq_agg AS (
    SELECT annee, rang,
           COUNT(*)                                                 AS nb,
           SUM(CASE WHEN duree <= 14 THEN 1 ELSE 0 END)             AS nb_moins_15,
           SUM(CASE WHEN duree BETWEEN 15 AND 45 THEN 1 ELSE 0 END) AS nb_15_45,
           SUM(CASE WHEN duree > 45 THEN 1 ELSE 0 END)              AS nb_plus_45,
           ROUND(AVG(duree), 1)                                     AS duree_moy,
           MEDIAN(duree)                                            AS duree_med
    FROM   liq
    GROUP BY annee, rang
    UNION ALL
    SELECT annee, 9,
           COUNT(*),
           SUM(CASE WHEN duree <= 14 THEN 1 ELSE 0 END),
           SUM(CASE WHEN duree BETWEEN 15 AND 45 THEN 1 ELSE 0 END),
           SUM(CASE WHEN duree > 45 THEN 1 ELSE 0 END),
           ROUND(AVG(duree), 1),
           MEDIAN(duree)
    FROM   liq
    GROUP BY annee
),

rejet_agg AS (
    SELECT EXTRACT(YEAR FROM date_rejet) AS annee, rang, COUNT(*) AS nb
    FROM   rej
    GROUP BY EXTRACT(YEAR FROM date_rejet), rang
    UNION ALL
    SELECT EXTRACT(YEAR FROM date_rejet), 9, COUNT(*)
    FROM   rej
    GROUP BY EXTRACT(YEAR FROM date_rejet)
),

instance_agg AS (
    SELECT annee, rang, nb FROM instance_fin
    UNION ALL
    SELECT annee, 9, SUM(nb) FROM instance_fin GROUP BY annee
),

-- ── 10. Grille : chaque exercice × chaque ligne, même vide ──────────────────
grille AS (
    SELECT a.annee, l.rang
    FROM   annees a
    CROSS JOIN (SELECT LEVEL AS rang FROM DUAL CONNECT BY LEVEL <= 9) l
    WHERE  a.annee >= TO_NUMBER(SUBSTR(:1, 3, 4)) - 7
)

SELECT
    g.annee                                                    AS ANNEE,
    g.rang                                                     AS RANG_LIGNE,
    CASE g.rang
        WHEN 1 THEN 'Pension de vieillesse'
        WHEN 2 THEN 'Pension de retraite anticipée'
        WHEN 3 THEN 'Pension d''invalidité'
        WHEN 4 THEN 'Pension de survivant'
        WHEN 5 THEN 'Allocations de vieillesse'
        WHEN 6 THEN 'Allocation de survivant'
        WHEN 7 THEN 'Autres prestations de la branche'
        WHEN 8 THEN 'Pas encore de ligne'
        ELSE        'Total'
    END                                                        AS LIGNE_CIPRES,
    NVL(dp.nb, 0)                                              AS NB_DEPOSES,
    NVL(lq.nb_moins_15, 0)                                     AS NB_LIQ_MOINS_15J,
    NVL(lq.nb_15_45, 0)                                        AS NB_LIQ_15_45J,
    NVL(lq.nb_plus_45, 0)                                      AS NB_LIQ_PLUS_45J,
    NVL(lq.nb, 0)                                              AS NB_LIQUIDES,
    lq.duree_moy                                               AS DUREE_MOYENNE_J,
    lq.duree_med                                               AS DUREE_MEDIANE_J,
    NVL(it.nb, 0)                                              AS NB_EN_INSTANCE,
    NVL(rj.nb, 0)                                              AS NB_REJETES,
    -- Règle 10 : en instance au 31/12 / (en instance au 1er janvier + déposés)
    ROUND(NVL(it.nb, 0) / NULLIF(NVL(i0.nb, 0) + NVL(dp.nb, 0), 0), 4)
                                                               AS TAUX_RESTANT_A_TRAITER,
    :1                                                         AS CLICHE
FROM      grille       g
LEFT JOIN depose_agg   dp ON dp.annee = g.annee     AND dp.rang = g.rang
LEFT JOIN liq_agg      lq ON lq.annee = g.annee     AND lq.rang = g.rang
LEFT JOIN rejet_agg    rj ON rj.annee = g.annee     AND rj.rang = g.rang
LEFT JOIN instance_agg it ON it.annee = g.annee     AND it.rang = g.rang
LEFT JOIN instance_agg i0 ON i0.annee = g.annee - 1 AND i0.rang = g.rang
ORDER BY g.annee, g.rang
