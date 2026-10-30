-- =============================================================================
-- T27 — Dossiers de prestations PF de l'exercice selon la durée de liquidation
-- PROPOSITION du 30/10/2026, à valider : traduit les règles 1 à 10 de la fiche
-- Excalidraw « T27 dossiers PF », sur le modèle de t25_requete_proposee.sql
-- et d'après le lot 4 (lot4_controles_t26_t27.sql, P64 à P68).
-- Sources : DWH.FAIT_RECEPTION_DOSSIER, FAIT_DOSSIER, FAIT_PRESTATION_ESP,
--           FAIT_DOSSIER_REJETE (copies des tables de USER_DWH).
-- Grain   : ANNEE × ligne CIPRES. Rangs 1 à 4 = lignes de T27 ; rang 5 = pas
--           encore de ligne (réception qui n'a encore donné aucune prestation),
--           compté dans le Total mais pas affiché ; rang 6 = Total, recalculé
--           sur toutes les demandes (jamais une moyenne de moyennes).
-- Branches : F (prestations familiales) et M (maternité, rattachée aux PF).
-- On compte des DEMANDES, comme en T25 : une demande = un dossier × une ligne.
--   · Un dossier F est celui d'un allocataire : sa demande d'allocations
--     familiales. Chaque enfant ajouté plus tard, sans passer par le guichet,
--     est une demande de plus si enfant_ajoute = 1 (Q1, P64, P65).
--   · Un dossier M est celui d'une grossesse : il porte l'IJ de maternité et
--     l'allocation prénatale, deux demandes déposées et liquidées ensemble
--     (Q2, P66, P67).
-- :1      = CLICHE (MMYYYY). Exercices calculés : les 7 années closes avant
--           l'année du cliché (cliché 092026 → 2019 à 2025).
-- Choix encore à valider (fiche T27) : enfant ajouté compté (Q1) ; grossesse
-- sur deux lignes (Q2) ; réceptions sans suite sorties du stock après
-- sans_suite_mois (Q3).
-- =============================================================================

WITH

-- ── 1. Exercices : les 7 années closes, plus la précédente ──────────────────
-- L'année la plus ancienne ne sert qu'au stock en instance au 1er janvier.
annees AS (
    SELECT TO_NUMBER(SUBSTR(:1, 3, 4)) - LEVEL AS annee
    FROM   DUAL
    CONNECT BY LEVEL <= 8
),

-- Règle 6 : comme en T25, une réception d'avant debut_stock jamais reliée à
-- un dossier n'est pas en attente ; une demande toujours sans suite
-- sans_suite_mois après son dépôt sort du stock (Q3 ; 483 réceptions PF de
-- 2025 sans dossier, P68). enfant_ajoute : 1 = un enfant ajouté à un dossier
-- existant est une demande (Q1), 0 = non. Valeurs à fixer avec la direction
-- des prestations.
parametres AS (
    SELECT DATE '2016-01-01' AS debut_stock,
           24                AS sans_suite_mois,
           1                 AS enfant_ajoute
    FROM   DUAL
),

-- ── 2. Dossiers PF et maternité, et la réception qui les a ouverts ──────────
-- Règle 1 : dossiers F et M.
dossier AS (
    SELECT d.DOS_CODE,
           d.DOS_STATUT,
           r.RD_ID                                            AS rd_ouverture,
           TRUNC(r.RD_DATE_RECEPTION)                         AS date_reception
    FROM      DWH.FAIT_DOSSIER           d
    LEFT JOIN DWH.FAIT_RECEPTION_DOSSIER r ON r.RD_ID  = d.RD_ID
                                          AND r.CLICHE = :1
    WHERE d.CLICHE    = :1
      AND d.TDOS_CODE IN ('F', 'M')
),

-- ── 3. Prestations et leur ligne CIPRES ─────────────────────────────────────
-- FAF → allocations familiales ; MIJ → IJ de maternité ; MAP (maternité) et
-- FAP (ancien code PF, 33 en 2025) → allocations prénatales.
prest AS (
    SELECT pe.DOS_CODE,
           pe.TPE_CODE,
           CASE
               WHEN pe.TPE_CODE = 'FAF'            THEN 1
               WHEN pe.TPE_CODE = 'MIJ'            THEN 2
               WHEN pe.TPE_CODE IN ('MAP', 'FAP')  THEN 3
               ELSE                                     4
           END                                                AS rang,
           TRUNC(pe.PE_DATE_DEMANDE)                          AS date_demande,
           TRUNC(pe.PE_DATE_NOTIFICATION)                     AS date_notif
    FROM DWH.FAIT_PRESTATION_ESP pe
    JOIN dossier                 d ON d.DOS_CODE = pe.DOS_CODE
    WHERE pe.CLICHE = :1
),

-- ── 4. Demandes ─────────────────────────────────────────────────────────────
-- Règle 3 : une demande est liquidée à la première notification de sa ligne.
--   · demande initiale : une par dossier et par ligne (ajout = 0) ;
--   · enfant ajouté : chaque date de notification d'allocations familiales
--     postérieure à la première du dossier (ajout = 1), déposée à la date de
--     demande de l'allocation, puisqu'elle ne passe pas par le guichet
--     (6 réceptions sur un dossier existant en 2025, P65).
initiale AS (
    SELECT DOS_CODE,
           rang,
           0                                                  AS ajout,
           MIN(date_demande)                                  AS date_demande,
           MIN(date_notif)                                    AS date_liquidation
    FROM   prest
    GROUP BY DOS_CODE, rang
),

demande AS (
    SELECT * FROM initiale
    UNION ALL
    SELECT p.DOS_CODE, 1, 1, MIN(p.date_demande), p.date_notif
    FROM   prest      p
    JOIN   initiale   i  ON i.DOS_CODE = p.DOS_CODE
                        AND i.rang     = 1
    CROSS JOIN parametres pr
    WHERE  p.rang = 1
      AND  p.date_notif > i.date_liquidation
      AND  pr.enfant_ajoute = 1
    GROUP BY p.DOS_CODE, p.date_notif
),

-- ── 5. Dépôts ───────────────────────────────────────────────────────────────
-- Règle 2 : la réception qui ouvre le dossier (nouvel allocataire, nouvelle
--           grossesse) est le dépôt de chacune de ses demandes initiales : une
--           grossesse compte sur la ligne des IJ et sur celle des allocations
--           prénatales (Q2). Un enfant ajouté est déposé à la date de demande
--           de son allocation. Les réceptions sur un dossier existant portent
--           des certificats de scolarité ou de vie : pas des dépôts (P65).
--           En rang 5 : les réceptions qui ont ouvert un dossier sans
--           prestation, et celles jamais reliées à un dossier.
depot AS (
    SELECT CASE WHEN dm.ajout = 0 THEN d.rd_ouverture END     AS RD_ID,
           CASE WHEN dm.ajout = 0 THEN d.date_reception
                ELSE dm.date_demande END                      AS date_depot,
           dm.DOS_CODE, d.DOS_STATUT, dm.rang, dm.ajout, dm.date_liquidation
    FROM   demande dm
    JOIN   dossier d ON d.DOS_CODE = dm.DOS_CODE
    UNION ALL
    SELECT d.rd_ouverture, d.date_reception, d.DOS_CODE, d.DOS_STATUT, 5, 0,
           CAST(NULL AS DATE)
    FROM   dossier d
    WHERE  d.rd_ouverture IS NOT NULL
      AND  NOT EXISTS (SELECT 1 FROM prest x WHERE x.DOS_CODE = d.DOS_CODE)
    UNION ALL
    SELECT r.RD_ID, TRUNC(r.RD_DATE_RECEPTION), NULL, NULL, 5, 0, CAST(NULL AS DATE)
    FROM   DWH.FAIT_RECEPTION_DOSSIER r
    WHERE  r.CLICHE    = :1
      AND  r.TDOS_CODE IN ('F', 'M')
      AND  r.RD_DATE_RECEPTION IS NOT NULL
      AND  r.DOS_CODE IS NULL
      AND  NOT EXISTS (SELECT 1 FROM dossier o WHERE o.rd_ouverture = r.RD_ID)
),

-- ── 6. Demandes liquidées et durée de liquidation ───────────────────────────
-- Règle 4 : jours entre le dépôt (sinon la date de demande de la prestation)
--           et la liquidation ; une durée négative (anomalie de saisie) est
--           ramenée à 0. Une grossesse est déposée environ 3 semaines avant
--           l'accouchement : la durée ne compte pas l'attente de la naissance
--           (P67).
liq AS (
    SELECT dp.rang,
           EXTRACT(YEAR FROM dp.date_liquidation)             AS annee,
           GREATEST(dp.date_liquidation
                    - NVL(dp.date_depot, dm.date_demande), 0) AS duree
    FROM   depot   dp
    JOIN   demande dm ON dm.DOS_CODE         = dp.DOS_CODE
                     AND dm.rang             = dp.rang
                     AND dm.ajout            = dp.ajout
                     AND dm.date_liquidation = dp.date_liquidation
    WHERE  dp.date_liquidation IS NOT NULL
),

-- ── 7. Rejets enregistrés ───────────────────────────────────────────────────
-- Règle 7 : année de la date de rejet ; ligne de la demande initiale du
--           dossier (allocations familiales, ou IJ de maternité), sinon rang 5.
--           Le module des rejets sert peu : 2 à 62 rejets par an en PF, 0 à
--           17 en maternité (P68).
rej AS (
    SELECT NVL((SELECT MIN(i.rang)
                FROM   initiale i
                WHERE  i.DOS_CODE = dr.DOS_CODE), 5)          AS rang,
           TRUNC(dr.DOSRE_DATE_REJET)                         AS date_rejet,
           dr.RD_ID,
           dr.DOS_CODE
    FROM   DWH.FAIT_DOSSIER_REJETE dr
    WHERE  dr.CLICHE    = :1
      AND  dr.TDOS_CODE IN ('F', 'M')
      AND  dr.DOSRE_DATE_REJET IS NOT NULL
),

-- ── 8. Stock en instance au 31 décembre de chaque année ─────────────────────
-- Règle 6 : déposée au plus tard le 31/12, pas encore liquidée à cette date,
--           sans rejet enregistré à cette date. Ne sont pas en attente :
--           · un dossier fermé dont la demande n'a jamais été liquidée ;
--           · une réception d'avant debut_stock jamais reliée à un dossier ;
--           · une demande toujours sans suite sans_suite_mois après son
--             dépôt : réceptions jamais reliées à un dossier, prestations
--             anciennes jamais notifiées (FAF : 68 % notifiées, P66) (Q3).
instance_fin AS (
    SELECT a.annee, dp.rang, COUNT(*) AS nb
    FROM   annees     a
    CROSS JOIN parametres pr
    JOIN   depot      dp ON dp.date_depot <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
    WHERE (dp.date_liquidation IS NULL
           OR dp.date_liquidation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
      AND NOT (NVL(dp.DOS_STATUT, '-') = 'F' AND dp.date_liquidation IS NULL)
      AND NOT (dp.DOS_CODE IS NULL AND dp.date_depot < pr.debut_stock)
      AND dp.date_depot >= ADD_MONTHS(TO_DATE(a.annee || '1231', 'YYYYMMDD'),
                                       -pr.sans_suite_mois)
      AND NOT EXISTS (SELECT 1
                      FROM   rej x
                      WHERE  (x.RD_ID = dp.RD_ID OR x.DOS_CODE = dp.DOS_CODE)
                        AND  x.date_rejet <= TO_DATE(a.annee || '1231', 'YYYYMMDD'))
    GROUP BY a.annee, dp.rang
),

-- ── 9. Comptages par année et par ligne, plus le Total (rang 6) ─────────────
depose_agg AS (
    SELECT EXTRACT(YEAR FROM date_depot) AS annee, rang, COUNT(*) AS nb
    FROM   depot
    WHERE  date_depot IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM date_depot), rang
    UNION ALL
    SELECT EXTRACT(YEAR FROM date_depot), 6, COUNT(*)
    FROM   depot
    WHERE  date_depot IS NOT NULL
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
    SELECT annee, 6,
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
    SELECT EXTRACT(YEAR FROM date_rejet), 6, COUNT(*)
    FROM   rej
    GROUP BY EXTRACT(YEAR FROM date_rejet)
),

instance_agg AS (
    SELECT annee, rang, nb FROM instance_fin
    UNION ALL
    SELECT annee, 6, SUM(nb) FROM instance_fin GROUP BY annee
),

-- ── 10. Grille : chaque exercice × chaque ligne, même vide ──────────────────
grille AS (
    SELECT a.annee, l.rang
    FROM   annees a
    CROSS JOIN (SELECT LEVEL AS rang FROM DUAL CONNECT BY LEVEL <= 6) l
    WHERE  a.annee >= TO_NUMBER(SUBSTR(:1, 3, 4)) - 7
)

SELECT
    g.annee                                                    AS ANNEE,
    g.rang                                                     AS RANG_LIGNE,
    CASE g.rang
        WHEN 1 THEN 'Allocations familiales'
        WHEN 2 THEN 'Indemnités journalières de maternité'
        WHEN 3 THEN 'Allocations prénatales'
        WHEN 4 THEN 'Autres prestations de la branche'
        WHEN 5 THEN 'Pas encore de ligne'
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
