-- =============================================================================
-- T26 — Dossiers de prestations AT/MP de l'exercice selon la durée de
--       liquidation
-- PROPOSITION du 30/10/2026, à valider : traduit les règles 1 à 10 de la fiche
-- Excalidraw « T26 dossiers AT/MP », sur le modèle de t25_requete_proposee.sql
-- et d'après le lot 4 (lot4_controles_t26_t27.sql, P59 à P73).
-- Sources : DWH.FAIT_RECEPTION_DOSSIER, FAIT_DOSSIER, FAIT_PRESTATION_ESP,
--           FAIT_DEBOURS (remboursements de soins), FAIT_DOSSIER_REJETE
--           (copies des tables de USER_DWH).
-- Grain   : ANNEE × ligne CIPRES. Rangs 1 à 5 = lignes de T26 ; rang 6 = pas
--           encore de ligne (réception qui n'a encore donné aucune prestation),
--           compté dans le Total mais pas affiché ; rang 7 = Total, recalculé
--           sur toutes les demandes (jamais une moyenne de moyennes).
-- On compte des DEMANDES, comme en T25 : une demande = un dossier × une ligne.
-- Un dossier AT/MP en porte souvent plusieurs, liquidées à des dates
-- différentes : les IJ et les soins après la déclaration, la rente ou
-- l'allocation des années après la consolidation, les rentes de survivants
-- après un décès (P59, P61, P71).
-- :1      = CLICHE (MMYYYY). Exercices calculés : les 7 années closes avant
--           l'année du cliché (cliché 092026 → 2019 à 2025).
-- Choix encore à valider (fiche T26) : déclarations sans suite sorties du
-- stock après sans_suite_mois (Q1) ; soins comptés en « autres prestations »
-- (Q2) ; durée d'une rente depuis sa demande au guichet (Q3) ; durée des IJ
-- depuis la déclaration (Q4).
-- =============================================================================

WITH

-- ── 1. Exercices : les 7 années closes, plus la précédente ──────────────────
-- L'année la plus ancienne ne sert qu'au stock en instance au 1er janvier.
annees AS (
    SELECT TO_NUMBER(SUBSTR(:1, 3, 4)) - LEVEL AS annee
    FROM   DUAL
    CONNECT BY LEVEL <= 8
),

-- Règle 6 : une réception d'avant debut_stock jamais reliée à un dossier
-- n'est pas une demande en attente (comme en T25). Une demande toujours sans
-- suite sans_suite_mois après son dépôt sort du stock : 46 % des
-- déclarations ne donnent jamais rien et ne sont jamais fermées (P59, P68),
-- et des prestations d'avant 2019 n'ont pas de date de notification (P61,
-- P72). Valeurs à fixer avec la direction des prestations (Q1).
parametres AS (
    SELECT DATE '2016-01-01' AS debut_stock,
           24                AS sans_suite_mois
    FROM   DUAL
),

-- ── 2. Dossiers AT/MP et leur déclaration ───────────────────────────────────
-- Règle 1 : tous les dossiers de la branche AT/MP (type A).
-- La déclaration est la réception qui a ouvert le dossier (type NOUVEAU).
dossier AS (
    SELECT d.DOS_CODE,
           d.DOS_STATUT,
           TRUNC(d.DOS_DATE_ACCIDENT)                         AS date_accident,
           TRUNC(d.DOS_DATE_REC_CM_RENTE)                     AS date_cm_rente,
           r.RD_ID                                            AS rd_declaration,
           TRUNC(r.RD_DATE_RECEPTION)                         AS date_declaration
    FROM      DWH.FAIT_DOSSIER           d
    LEFT JOIN DWH.FAIT_RECEPTION_DOSSIER r ON r.RD_ID  = d.RD_ID
                                          AND r.CLICHE = :1
    WHERE d.CLICHE    = :1
      AND d.TDOS_CODE = 'A'
),

-- ── 3. Demandes : un dossier × une ligne ────────────────────────────────────
-- Règle 3 : une prestation en espèces est liquidée à la première notification
--           de sa ligne : une IJ prolongée donne plusieurs IJ, liquidées à la
--           première (1,9 IJ par dossier, P61). L'allocation d'incapacité a sa
--           notification depuis 2019 (P72).
--           Les soins (remboursements en nature) font une demande par dossier,
--           liquidée à la saisie du premier remboursement : les dates
--           d'approbation des vieux dossiers ont été remplies en bloc en 2024
--           (P73). Saisie et approbation sont à moins de 10 j l'une de l'autre
--           dans le flux normal (P63).
demande AS (
    SELECT pe.DOS_CODE,
           CASE
               WHEN pe.TPE_CODE = 'ARI'                             THEN 1
               WHEN pe.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA') THEN 2
               WHEN pe.TPE_CODE = 'AIJ'                             THEN 3
               WHEN pe.TPE_CODE = 'AAI'                             THEN 4
               ELSE                                                      5
           END                                                AS rang,
           TRUNC(MIN(pe.PE_DATE_DEMANDE))                     AS date_demande,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))                AS date_liquidation
    FROM DWH.FAIT_PRESTATION_ESP pe
    JOIN dossier                 d ON d.DOS_CODE = pe.DOS_CODE
    WHERE pe.CLICHE = :1
    GROUP BY pe.DOS_CODE,
             CASE
                 WHEN pe.TPE_CODE = 'ARI'                             THEN 1
                 WHEN pe.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA') THEN 2
                 WHEN pe.TPE_CODE = 'AIJ'                             THEN 3
                 WHEN pe.TPE_CODE = 'AAI'                             THEN 4
                 ELSE                                                      5
             END
    UNION ALL
    SELECT deb.DOS_CODE,
           5,
           MIN(d.date_accident),
           TRUNC(MIN(deb.DEB_DATE_INSERT))
    FROM DWH.FAIT_DEBOURS deb
    JOIN dossier          d ON d.DOS_CODE = deb.DOS_CODE
    WHERE deb.CLICHE   = :1
      AND deb.TPN_CODE IS NOT NULL
    GROUP BY deb.DOS_CODE
),

-- La première demande d'un dossier (la plus tôt liquidée) porte ses rejets.
demande_rg AS (
    SELECT dm.*,
           ROW_NUMBER() OVER (PARTITION BY dm.DOS_CODE
                              ORDER BY dm.date_liquidation NULLS LAST, dm.rang) AS ordre
    FROM   demande dm
),

-- ── 4. Demandes déposées sur un dossier existant ────────────────────────────
-- Règle 2 : une réception sur un dossier existant (type ANCIEN) est le dépôt
--           d'une rente, d'une allocation ou d'une rente de survivant : 70 %
--           sont suivies d'une rente ou d'une allocation dans l'année, 9 %
--           d'une rente de survivant (P71). Elle va à la première de ces
--           demandes liquidée après elle, sinon à une demande pas encore
--           liquidée. Sans demande de ce genre :
--             · si le dossier a déjà une rente liquidée avant, c'est une pièce
--               de maintien des droits (certificat de vie…) : pas un dépôt ;
--             · sinon, la demande attend sa prestation : rang 6.
ancien_cand AS (
    SELECT r.RD_ID,
           TRUNC(r.RD_DATE_RECEPTION)                         AS date_reception,
           d.DOS_CODE,
           d.DOS_STATUT,
           dm.rang,
           dm.date_liquidation,
           (SELECT COUNT(*)
            FROM   demande x
            WHERE  x.DOS_CODE = d.DOS_CODE
              AND  x.rang IN (1, 2, 4)
              AND  x.date_liquidation < TRUNC(r.RD_DATE_RECEPTION)) AS rentes_avant,
           ROW_NUMBER() OVER (
               PARTITION BY r.RD_ID
               ORDER BY CASE WHEN dm.date_liquidation IS NULL THEN 1 ELSE 0 END,
                        dm.date_liquidation, dm.rang)         AS n
    FROM      DWH.FAIT_RECEPTION_DOSSIER r
    JOIN      dossier                    d  ON d.DOS_CODE = r.DOS_CODE
    LEFT JOIN demande                    dm ON dm.DOS_CODE = d.DOS_CODE
                                           AND dm.rang IN (1, 2, 4)
                                           AND (dm.date_liquidation IS NULL
                                                OR dm.date_liquidation >= TRUNC(r.RD_DATE_RECEPTION))
    WHERE r.CLICHE    = :1
      AND r.TDOS_CODE = 'A'
      AND r.RD_DATE_RECEPTION IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM dossier o WHERE o.rd_declaration = r.RD_ID)
),

-- Une demande ne compte qu'un dépôt, le premier.
ancien AS (
    SELECT RD_ID, date_reception, DOS_CODE, DOS_STATUT,
           NVL(rang, 6)                                       AS rang,
           date_liquidation,
           CASE WHEN rang IS NULL THEN 1
                ELSE ROW_NUMBER() OVER (PARTITION BY DOS_CODE, rang
                                        ORDER BY date_reception, RD_ID)
           END                                                AS k
    FROM   ancien_cand
    WHERE  n = 1
      AND  NOT (rang IS NULL AND rentes_avant > 0)
),

-- ── 5. Dépôts ───────────────────────────────────────────────────────────────
-- Règle 2 : chaque demande est déposée une fois :
--   · IJ et soins : par la déclaration, qui les demande ;
--   · rente, allocation, rente de survivant : par sa demande au guichet
--     (ancien), sinon à la réception du certificat pour la rente, sinon
--     avec la déclaration.
-- Une déclaration qui donne des IJ et des soins compte donc sur les deux
-- lignes. S'y ajoutent, en rang 6 : les déclarations qui n'ont encore donné
-- aucune prestation, les demandes ANCIEN qui attendent la leur, les
-- réceptions jamais reliées à un dossier.
depot AS (
    SELECT CASE WHEN dm.rang IN (3, 5) THEN d.rd_declaration
                ELSE NVL(a.RD_ID, d.rd_declaration) END       AS RD_ID,
           CASE WHEN dm.rang IN (3, 5) THEN d.date_declaration
                ELSE COALESCE(a.date_reception, d.date_cm_rente,
                              d.date_declaration) END         AS date_depot,
           dm.DOS_CODE, d.DOS_STATUT, dm.rang, dm.date_liquidation
    FROM      demande  dm
    JOIN      dossier  d ON d.DOS_CODE = dm.DOS_CODE
    LEFT JOIN ancien   a ON a.DOS_CODE = dm.DOS_CODE
                        AND a.rang     = dm.rang
                        AND a.k        = 1
    UNION ALL
    SELECT d.rd_declaration, d.date_declaration, d.DOS_CODE, d.DOS_STATUT, 6,
           CAST(NULL AS DATE)
    FROM   dossier d
    WHERE  d.rd_declaration IS NOT NULL
      AND  NOT EXISTS (SELECT 1 FROM demande x WHERE x.DOS_CODE = d.DOS_CODE)
    UNION ALL
    SELECT RD_ID, date_reception, DOS_CODE, DOS_STATUT, 6, CAST(NULL AS DATE)
    FROM   ancien
    WHERE  rang = 6
    UNION ALL
    SELECT r.RD_ID, TRUNC(r.RD_DATE_RECEPTION), NULL, NULL, 6, CAST(NULL AS DATE)
    FROM   DWH.FAIT_RECEPTION_DOSSIER r
    WHERE  r.CLICHE    = :1
      AND  r.TDOS_CODE = 'A'
      AND  r.RD_DATE_RECEPTION IS NOT NULL
      AND  r.DOS_CODE IS NULL
      AND  NOT EXISTS (SELECT 1 FROM dossier o WHERE o.rd_declaration = r.RD_ID)
),

-- ── 6. Demandes liquidées et durée de liquidation ───────────────────────────
-- Règle 4 : jours entre le dépôt (sinon la date de demande de la prestation,
--           ou l'accident pour les soins) et la liquidation ; une durée
--           négative (anomalie de saisie) est ramenée à 0. Pour une IJ, la
--           durée part de la déclaration : un mois d'arrêt en médiane, puis
--           environ 7 mois d'attente que la base n'explique pas (P69, Q4).
liq AS (
    SELECT dm.rang,
           EXTRACT(YEAR FROM dm.date_liquidation)             AS annee,
           GREATEST(dm.date_liquidation
                    - NVL(dp.date_depot, dm.date_demande), 0) AS duree
    FROM      demande dm
    LEFT JOIN depot   dp ON dp.DOS_CODE = dm.DOS_CODE
                        AND dp.rang     = dm.rang
    WHERE dm.date_liquidation IS NOT NULL
),

-- ── 7. Rejets enregistrés ───────────────────────────────────────────────────
-- Règle 7 : année de la date de rejet ; ligne de la première demande du
--           dossier, sinon rang 6. Le module des rejets sert peu : 0 à 7
--           rejets AT/MP par an (P68).
rej AS (
    SELECT NVL(dm.rang, 6)                                    AS rang,
           TRUNC(dr.DOSRE_DATE_REJET)                         AS date_rejet,
           dr.RD_ID,
           dr.DOS_CODE
    FROM      DWH.FAIT_DOSSIER_REJETE dr
    LEFT JOIN demande_rg              dm ON dm.DOS_CODE = dr.DOS_CODE
                                        AND dm.ordre    = 1
    WHERE dr.CLICHE    = :1
      AND dr.TDOS_CODE = 'A'
      AND dr.DOSRE_DATE_REJET IS NOT NULL
),

-- ── 8. Stock en instance au 31 décembre de chaque année ─────────────────────
-- Règle 6 : déposée au plus tard le 31/12, pas encore liquidée à cette date,
--           sans rejet enregistré à cette date. Ne sont pas en attente :
--           · un dossier fermé dont la demande n'a jamais été liquidée ;
--           · une réception d'avant debut_stock jamais reliée à un dossier ;
--           · une demande toujours sans suite sans_suite_mois après son
--             dépôt : surtout les déclarations qui n'ont jamais rien donné
--             (Q1), et des prestations anciennes jamais notifiées (P61).
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

-- ── 9. Comptages par année et par ligne, plus le Total (rang 7) ─────────────
depose_agg AS (
    SELECT EXTRACT(YEAR FROM date_depot) AS annee, rang, COUNT(*) AS nb
    FROM   depot
    WHERE  date_depot IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM date_depot), rang
    UNION ALL
    SELECT EXTRACT(YEAR FROM date_depot), 7, COUNT(*)
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
    SELECT annee, 7,
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
    SELECT EXTRACT(YEAR FROM date_rejet), 7, COUNT(*)
    FROM   rej
    GROUP BY EXTRACT(YEAR FROM date_rejet)
),

instance_agg AS (
    SELECT annee, rang, nb FROM instance_fin
    UNION ALL
    SELECT annee, 7, SUM(nb) FROM instance_fin GROUP BY annee
),

-- ── 10. Grille : chaque exercice × chaque ligne, même vide ──────────────────
grille AS (
    SELECT a.annee, l.rang
    FROM   annees a
    CROSS JOIN (SELECT LEVEL AS rang FROM DUAL CONNECT BY LEVEL <= 7) l
    WHERE  a.annee >= TO_NUMBER(SUBSTR(:1, 3, 4)) - 7
)

SELECT
    g.annee                                                    AS ANNEE,
    g.rang                                                     AS RANG_LIGNE,
    CASE g.rang
        WHEN 1 THEN 'Rentes viagères'
        WHEN 2 THEN 'Rentes de survivants'
        WHEN 3 THEN 'Indemnités journalières'
        WHEN 4 THEN 'Allocation AT/MP'
        WHEN 5 THEN 'Autres prestations de la branche'
        WHEN 6 THEN 'Pas encore de ligne'
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
