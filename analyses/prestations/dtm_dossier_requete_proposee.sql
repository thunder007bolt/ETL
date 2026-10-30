-- =============================================================================
-- DTM_DOSSIER — Dossiers de prestations selon la durée de liquidation (T25–T27)
-- PROPOSITION du 29/10/2026, à valider. Remplacerait
-- domaines/prestation_recouvrement/dtm/sql/dossier.sql (V5) ; le pipeline
-- n'est pas modifié tant que les règles ne sont pas validées.
--
-- Ce qui change par rapport à la V5 :
--   - Mêmes colonnes, plus SOMME_DELAIS_JOURS (DDL : dtm_dossier_ddl_propose.sql).
--   - ID_TEMPS n'est plus le mois d'ouverture du dossier : chaque événement est
--     compté au mois où il se produit (dépôt, liquidation, rejet).
--   - NB_INSTANCE devient le stock en fin de mois : il ne s'additionne pas sur
--     les mois ; pour un exercice, on lit le mois de décembre.
--   - Liquidé = dossier notifié, et non plus dossier au statut F (A = actif,
--     F = fermé : Données P02, P10).
--   - Tous les dossiers sont pris, et non plus seulement ceux qui ont un
--     paiement (Données P09).
-- Le sens de NB_DOSSIERS, NB_INSTANCE et ID_TEMPS change : accord de Jules
-- (Oracle Analytics) requis, et clichés déjà chargés à recharger ou purger.
--
-- Sources : DWH.FAIT_RECEPTION_DOSSIER, FAIT_DOSSIER, FAIT_PRESTATION_ESP,
--           FAIT_DEBOURS (remboursements RB), FAIT_DOSSIER_REJETE, FAIT_INDIVIDU,
--           DTM.DIM_TRANCHE_AGE, DTM.DIM_TYPE_PRESTATION.
-- Grain   : ID_TEMPS (mois de l'événement) × TDOS_CODE × CODE_PRESTATION
--           × DR_NO × SP_NO × LP_NO × SEXE × TAG_CODE.
-- Période : du 1er janvier de (année du cliché − 8) à la fin du mois du
--           cliché (092026 → janvier 2018 à septembre 2026).
-- :1      = CLICHE (MMYYYY). Toutes les tables de faits sont filtrées sur :1.
--
-- Lecture dans Oracle Analytics, pour un exercice :
--   Déposés              = somme de NB_DOSSIERS sur les 12 mois
--   Liquidés, tranches   = somme de NB_LIQUIDES, NB_INF_15J, NB_15_45J, NB_SUP_45J
--   Durée moyenne        = somme de SOMME_DELAIS_JOURS
--                          / somme de (NB_INF_15J + NB_15_45J + NB_SUP_45J)
--                          (DELAI_MOYEN_JOURS, moyenne d'une seule ligne, ne
--                          s'agrège pas)
--   En instance          = NB_INSTANCE du mois de décembre
--   Rejetés              = somme de NB_REJETES
--   Taux restant à traiter = en instance de décembre
--                          / (en instance de décembre N−1 + déposés de N)
--   Ligne CIPRES         = d'après CODE_PRESTATION (correspondance du README)
--
-- Règles : README (R1, R2, R9, R10, R11, R12) et document « Règles de calcul
-- des tableaux des prestations » (règles T25 1 à 7, étendues à T26 et T27).
-- Points encore à vérifier : voir la fin du fichier.
-- =============================================================================

WITH

-- ── 1. Période et mois couverts ─────────────────────────────────────────────
periode AS (
    SELECT TO_DATE('0101' || TO_CHAR(TO_NUMBER(SUBSTR(:1, 3, 4)) - 8), 'DDMMYYYY')
                                              AS date_debut,
           LAST_DAY(TO_DATE(:1, 'MMYYYY'))    AS date_fin
    FROM   DUAL
),

mois AS (
    SELECT ADD_MONTHS(p.date_debut, LEVEL - 1)           AS debut_mois,
           LAST_DAY(ADD_MONTHS(p.date_debut, LEVEL - 1)) AS fin_mois
    FROM   periode p
    CONNECT BY LEVEL <= MONTHS_BETWEEN(TRUNC(p.date_fin, 'MM'), p.date_debut) + 1
),

-- ── 2. Prestations de chaque dossier, avec leur rang ────────────────────────
-- Rang = ordre des lignes de la maquette (Annuaire p. 34), retraite anticipée
-- à part (Kaya 2017, tableau 1, T25). Ordre retenu quand un dossier a
-- plusieurs prestations : Proposition BI.
-- Rangs 7 et 8 = « Autres prestations de la branche » (8 = en nature).
prest_dossier AS (
    SELECT pe.DOS_CODE,
           pe.TPE_CODE                                    AS code_prestation,
           CASE
               -- T25 (PVID)
               WHEN pe.TPE_CODE = 'RPV'                            THEN 1
               WHEN pe.TPE_CODE = 'RPRA'                           THEN 2
               WHEN pe.TPE_CODE = 'RPI'                            THEN 3
               WHEN pe.TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA') THEN 4
               WHEN pe.TPE_CODE = 'RAV'                            THEN 5
               WHEN pe.TPE_CODE IN ('RAS', 'RASC', 'RASO', 'RASA') THEN 6
               -- T26 (AT/MP)
               WHEN pe.TPE_CODE = 'ARI'                            THEN 1
               WHEN pe.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA') THEN 2
               WHEN pe.TPE_CODE = 'AIJ'                            THEN 3
               WHEN pe.TPE_CODE = 'AAI'                            THEN 4
               -- T27 (PF, maternité comprise)
               WHEN pe.TPE_CODE = 'FAF'                            THEN 1
               WHEN pe.TPE_CODE = 'MIJ'                            THEN 2
               WHEN pe.TPE_CODE IN ('FAP', 'MAP')                  THEN 3
               ELSE                                                     7
           END                                            AS rang,
           pe.PE_DATE_NOTIFICATION                        AS date_notification,
           -- R9 : mois cotisés au-dessus de 600 (jusqu'à 999) écartés (Données P29, P30)
           CASE WHEN pe.PE_MOIS_COTISATION <= 600
                THEN pe.PE_MOIS_COTISATION END            AS mois_cotises,
           CAST(NULL AS DATE)                             AS date_approbation_rb
    FROM   DWH.FAIT_PRESTATION_ESP pe
    WHERE  pe.CLICHE   = :1
      AND  pe.TPE_CODE IS NOT NULL

    UNION ALL

    -- Remboursements (prestations en nature, surtout frais médicaux AT/MP)
    SELECT deb.DOS_CODE,
           deb.TPN_CODE,
           8,
           CAST(NULL AS DATE),
           CAST(NULL AS NUMBER),
           MIN(deb.DEB_DATE_APPROBATION)
    FROM   DWH.FAIT_DEBOURS deb
    WHERE  deb.CLICHE   = :1
      AND  deb.DEB_TYPE = 'RB'
      AND  deb.TPN_CODE IS NOT NULL
    GROUP BY deb.DOS_CODE, deb.TPN_CODE
),

-- ── 3. Code principal, date de liquidation et mois cotisés du dossier ───────
-- Code principal = prestation de plus petit rang (puis ordre alphabétique).
code_principal AS (
    SELECT DOS_CODE, code_prestation
    FROM (
        SELECT DOS_CODE,
               code_prestation,
               ROW_NUMBER() OVER (PARTITION BY DOS_CODE
                                  ORDER BY rang, code_prestation) AS rn
        FROM   prest_dossier
    )
    WHERE rn = 1
),

-- R11 / règle T25 3 : date de liquidation = première notification des droits
-- (Annuaire p. 64 ; « première » : Proposition BI).
-- Dossier sans aucune notification mais avec un remboursement approuvé
-- (frais médicaux seuls) : date de la première approbation (Proposition BI).
liquidation AS (
    SELECT DOS_CODE,
           TRUNC(NVL(MIN(date_notification), MIN(date_approbation_rb))) AS date_liquidation,
           MAX(mois_cotises)                                            AS mois_cotises
    FROM   prest_dossier
    GROUP BY DOS_CODE
),

-- ── 4. Dossiers, avec leur réception ────────────────────────────────────────
-- Région, service et lieu de paiement : ceux du dossier, sinon ceux de la
-- réception.
dossier AS (
    SELECT d.DOS_CODE,
           d.RD_ID,
           d.TDOS_CODE,
           d.IND_ID,
           NVL(d.DOS_CATEGORIE, 'ORDINAIRE')  AS categorie,
           NVL(d.DR_NO, r.DR_NO)              AS DR_NO,
           NVL(d.SP_NO, r.SP_NO)              AS SP_NO,
           NVL(d.LP_NO, r.LP_NO)              AS LP_NO,
           TRUNC(r.RD_DATE_RECEPTION)         AS date_reception,
           cp.code_prestation,
           lq.date_liquidation,
           lq.mois_cotises
    FROM      DWH.FAIT_DOSSIER           d
    LEFT JOIN DWH.FAIT_RECEPTION_DOSSIER r  ON r.RD_ID    = d.RD_ID
                                           AND r.CLICHE   = :1
    LEFT JOIN code_principal             cp ON cp.DOS_CODE = d.DOS_CODE
    LEFT JOIN liquidation                lq ON lq.DOS_CODE = d.DOS_CODE
    WHERE d.CLICHE = :1
),

-- ── 5. Réceptions, rattachées à leur dossier ────────────────────────────────
-- R10 / règle T25 2 : une réception au guichet = un dossier déposé
-- (Annuaire p. 64 ; Kaya 2017, tableau 2). Réception sans dossier : code
-- prestation vide (« type non encore connu »), comptée dans le total.
-- Si une réception a plusieurs dossiers, on garde le premier liquidé
-- (à vérifier : une réception = un dossier ?).
reception AS (
    SELECT r.RD_ID,
           TRUNC(r.RD_DATE_RECEPTION)                     AS date_reception,
           r.TDOS_CODE,
           NVL(d.DR_NO, r.DR_NO)                          AS DR_NO,
           NVL(d.SP_NO, r.SP_NO)                          AS SP_NO,
           NVL(d.LP_NO, r.LP_NO)                          AS LP_NO,
           d.IND_ID,
           d.code_prestation,
           d.date_liquidation,
           ROW_NUMBER() OVER (PARTITION BY r.RD_ID
                              ORDER BY d.date_liquidation NULLS LAST,
                                       d.DOS_CODE)        AS rn
    FROM      DWH.FAIT_RECEPTION_DOSSIER r
    LEFT JOIN dossier                    d ON d.RD_ID = r.RD_ID
    WHERE r.CLICHE            = :1
      AND r.RD_DATE_RECEPTION IS NOT NULL
      -- Règle T25 1 : hors coordination payée par une autre caisse et dossiers
      -- transférés (Proposition BI, Kaya 2017 n'en parle que pour T32 et T42)
      AND NVL(d.categorie, 'ORDINAIRE') NOT IN ('COOR_AC', 'CNSS_AC')
),

-- ── 6. Rejets ───────────────────────────────────────────────────────────────
-- Règle T25 7 : compté au mois de la date de rejet. Les rejets sont presque
-- pas saisis (486 en tout : Données P09 ; Kaya 2017, tableau 1, T27).
rejet AS (
    SELECT dr.DOSRE_ID,
           TRUNC(dr.DOSRE_DATE_REJET)                     AS date_rejet,
           NVL(dr.RD_ID, d.RD_ID)                         AS RD_ID,
           NVL(dr.TDOS_CODE, d.TDOS_CODE)                 AS TDOS_CODE,
           NVL(d.DR_NO, dr.DR_NO)                         AS DR_NO,
           d.SP_NO,
           NVL(d.LP_NO, dr.LP_NO)                         AS LP_NO,
           d.IND_ID,
           d.code_prestation
    FROM      DWH.FAIT_DOSSIER_REJETE dr
    LEFT JOIN dossier                 d ON d.DOS_CODE = dr.DOS_CODE
    WHERE dr.CLICHE           = :1
      AND dr.DOSRE_DATE_REJET IS NOT NULL
      AND NVL(d.categorie, 'ORDINAIRE') NOT IN ('COOR_AC', 'CNSS_AC')
),

-- ── 7. Sortie du stock de chaque réception : liquidation ou premier rejet ──
-- R12 / règle T25 6 : en instance = reçu, ni liquidé ni rejeté à la date.
reception_stock AS (
    SELECT rc.RD_ID,
           rc.date_reception,
           rc.TDOS_CODE,
           rc.DR_NO,
           rc.SP_NO,
           rc.LP_NO,
           rc.IND_ID,
           rc.code_prestation,
           CASE
               WHEN rr.date_rejet        IS NULL THEN rc.date_liquidation
               WHEN rc.date_liquidation  IS NULL THEN rr.date_rejet
               ELSE LEAST(rc.date_liquidation, rr.date_rejet)
           END                                            AS date_sortie
    FROM      reception rc
    LEFT JOIN (SELECT RD_ID, MIN(date_rejet) AS date_rejet
               FROM   rejet
               WHERE  RD_ID IS NOT NULL
               GROUP BY RD_ID)  rr ON rr.RD_ID = rc.RD_ID
    WHERE rc.rn = 1
),

-- ── 8. Événements : une ligne par événement, au mois où il se produit ──────
evenement AS (
    -- Dépôts
    SELECT TRUNC(rs.date_reception, 'MM')                 AS mois,
           rs.TDOS_CODE, rs.code_prestation,
           rs.DR_NO, rs.SP_NO, rs.LP_NO, rs.IND_ID,
           1 AS nb_depose, 0 AS nb_liquide, 0 AS nb_rejete, 0 AS nb_instance,
           CAST(NULL AS NUMBER)                           AS delai,
           CAST(NULL AS NUMBER)                           AS mois_cotises
    FROM       reception_stock rs
    CROSS JOIN periode         p
    WHERE rs.date_reception BETWEEN p.date_debut AND p.date_fin

    UNION ALL

    -- Liquidations. Règle T25 4 : durée = notification − réception, une durée
    -- négative (erreur de saisie) ramenée à 0 ; vide si le dossier n'a pas de
    -- réception (compté dans NB_LIQUIDES, hors tranches).
    SELECT TRUNC(d.date_liquidation, 'MM'),
           d.TDOS_CODE, d.code_prestation,
           d.DR_NO, d.SP_NO, d.LP_NO, d.IND_ID,
           0, 1, 0, 0,
           GREATEST(d.date_liquidation - d.date_reception, 0),
           d.mois_cotises
    FROM       dossier d
    CROSS JOIN periode p
    WHERE d.date_liquidation BETWEEN p.date_debut AND p.date_fin
      AND d.categorie NOT IN ('COOR_AC', 'CNSS_AC')

    UNION ALL

    -- Rejets
    SELECT TRUNC(rj.date_rejet, 'MM'),
           rj.TDOS_CODE, rj.code_prestation,
           rj.DR_NO, rj.SP_NO, rj.LP_NO, rj.IND_ID,
           0, 0, 1, 0,
           CAST(NULL AS NUMBER),
           CAST(NULL AS NUMBER)
    FROM       rejet   rj
    CROSS JOIN periode p
    WHERE rj.date_rejet BETWEEN p.date_debut AND p.date_fin

    UNION ALL

    -- Stock en instance à la fin de chaque mois
    SELECT m.debut_mois,
           rs.TDOS_CODE, rs.code_prestation,
           rs.DR_NO, rs.SP_NO, rs.LP_NO, rs.IND_ID,
           0, 0, 0, 1,
           CAST(NULL AS NUMBER),
           CAST(NULL AS NUMBER)
    FROM   mois            m
    JOIN   reception_stock rs ON rs.date_reception <= m.fin_mois
                             AND (rs.date_sortie IS NULL OR rs.date_sortie > m.fin_mois)
)

-- ── 9. Agrégation au grain du DTM ───────────────────────────────────────────
SELECT
    TO_NUMBER(TO_CHAR(e.mois, 'YYYYMMDD'))                AS ID_TEMPS,
    e.TDOS_CODE,
    e.code_prestation                                     AS CODE_PRESTATION,
    tp.DESCRIPTION                                        AS DESCRIPTION_PRESTATION,
    NVL(e.DR_NO, 99)                                      AS DR_NO,
    NVL(e.SP_NO, 9999)                                    AS SP_NO,
    NVL(e.LP_NO, 9998)                                    AS LP_NO,
    ind.IND_SEXE                                          AS SEXE,
    CASE ind.IND_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        'NON RENSEIGNE'
    END                                                   AS LIBELLE_SEXE,
    NVL(tag.TAG_CODE, 1000)                               AS TAG_CODE,

    SUM(e.nb_depose)                                      AS NB_DOSSIERS,
    SUM(e.mois_cotises)                                   AS NB_MOIS_COTISES,
    SUM(e.nb_liquide)                                     AS NB_LIQUIDES,
    SUM(e.nb_instance)                                    AS NB_INSTANCE,
    SUM(e.nb_rejete)                                      AS NB_REJETES,

    -- Règle T25 5 : moins de 15 j / 15 à 45 j / plus de 45 j (Annuaire p. 34)
    SUM(CASE WHEN e.delai <= 14              THEN 1 ELSE 0 END) AS NB_INF_15J,
    SUM(CASE WHEN e.delai BETWEEN 15 AND 45  THEN 1 ELSE 0 END) AS NB_15_45J,
    SUM(CASE WHEN e.delai > 45               THEN 1 ELSE 0 END) AS NB_SUP_45J,

    AVG(e.delai)                                          AS DELAI_MOYEN_JOURS,
    MAX(e.delai)                                          AS DELAI_MAX_JOURS,
    MIN(e.delai)                                          AS DELAI_MIN_JOURS,
    -- Nouvelle colonne : permet une durée moyenne juste sur l'année
    SUM(e.delai)                                          AS SOMME_DELAIS_JOURS,

    SYSDATE                                               AS DATE_CHARGEMENT,
    :1                                                    AS CLICHE

FROM      evenement                e
LEFT JOIN DWH.FAIT_INDIVIDU        ind ON ind.IND_ID = e.IND_ID
                                      AND ind.CLICHE = :1
-- R8 : âge = année de l'événement − année de naissance (Annuaire p. 62)
LEFT JOIN DTM.DIM_TRANCHE_AGE      tag ON EXTRACT(YEAR FROM e.mois)
                                          - EXTRACT(YEAR FROM ind.IND_DATE_NAISSANCE)
                                          BETWEEN tag.INF AND tag.SUP
LEFT JOIN DTM.DIM_TYPE_PRESTATION  tp  ON tp.CODE_PRESTATION = e.code_prestation
-- R1 : les quatre branches ; M (maternité) est rattachée aux PF dans T27
-- (Données P02, P05)
WHERE e.TDOS_CODE IN ('V', 'A', 'F', 'M')

GROUP BY
    TO_NUMBER(TO_CHAR(e.mois, 'YYYYMMDD')),
    e.TDOS_CODE,
    e.code_prestation,
    tp.DESCRIPTION,
    NVL(e.DR_NO, 99),
    NVL(e.SP_NO, 9999),
    NVL(e.LP_NO, 9998),
    ind.IND_SEXE,
    NVL(tag.TAG_CODE, 1000),
    :1

-- =============================================================================
-- Points encore à vérifier (lot 3) avant mise en production
--   1. Une réception = un seul dossier ? (sinon le premier liquidé est gardé)
--   2. Réceptions anciennes jamais closes : elles restent en instance pour
--      toujours. Compter le stock par année de réception.
--   3. Plusieurs notifications sur un dossier (révision, réversion) : écart
--      entre première et dernière ; comparer aussi DOS_DATE_NOTIFICATION.
--   4. Dossiers de frais médicaux seuls : la date d'approbation du premier
--      remboursement vaut-elle liquidation ?
--   5. Sens de RD_STATUT (E, I, L, R, A) : une réception annulée doit-elle
--      sortir du stock ? (question DSI)
--   6. La base source est arrêtée au 02/02/2026 (Données P01) : de février à
--      septembre 2026, aucun événement et un stock figé.
--   7. Temps d'exécution : le stock croise ~105 mois avec toutes les
--      réceptions ; passer l'entrée « dossier » en heavy si besoin.
-- =============================================================================
