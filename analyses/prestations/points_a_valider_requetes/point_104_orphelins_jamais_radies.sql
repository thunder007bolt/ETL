-- =============================================================================
-- Point 104. Les orphelins jamais radiés (T35)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « PRESTATION_ESP, code RPSE, sans date de radiation
--   (PE_DATE_RADIATION), orphelin de 25 ans ou plus. » ; « fin 2024, 5 774
--   pensions d'orphelin de 25 ans et plus encore en cours, presque jamais
--   payées. Fin 2025, 8 447 pensions d'orphelin actives sans aucun paiement
--   au dernier trimestre. »
-- Requêtes :
--   1. Les pensions actives et en vie au 31/12, de 2020 à 2024, par type, dont
--      les orphelins de tous âges et de moins de 25 ans.
--   2. Au 31/12/2025 : prestations actives par les dates et payées au 4e
--      trimestre.
-- Reprise de : comparaison_annuaire_cnss_2024/requetes_comparaison.sql, A01
--   (exécutée le 30/10/2026) ; lot2_statuts_stocks_nouveaux.sql, P25
--   (exécutée le 26/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc en_cours (T35 :
--   moins de 25 ans). Fiche des règles : règle 50.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Pensions actives et en vie au 31/12, de 2020 à 2024, par type et
-- selon plusieurs définitions.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 5
),
pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
paie AS (
    SELECT d.PE_ID,
           EXTRACT(YEAR FROM d.DEB_DATE_EFFET)                    AS annee,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P') THEN 1 ELSE 0 END) AS paye,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                     AND EXTRACT(MONTH FROM d.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS paye_t4,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                     AND EXTRACT(MONTH FROM d.DEB_DATE_EFFET) = 12
                    THEN 1 ELSE 0 END)                            AS paye_dec,
           SUM(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                    THEN d.DEB_MONTANT ELSE 0 END)                AS montant
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P', 'V')
      AND d.DEB_DATE_EFFET >= DATE '2020-01-01'
      AND d.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID, EXTRACT(YEAR FROM d.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.TPE_CODE,
           pe.IND_ID_PREST,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                THEN 1 ELSE 0 END                                 AS actif,
           CASE WHEN pe.date_deces IS NULL
                  OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                THEN 1 ELSE 0 END                                 AS vivant,
           a.annee - pe.annee_naissance                           AS age,
           pa.PE_ID                                               AS pe_paye,
           NVL(pa.paye, 0)                                        AS paye,
           NVL(pa.paye_t4, 0)                                     AS paye_t4,
           NVL(pa.paye_dec, 0)                                    AS paye_dec,
           pa.montant
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       CASE TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       SUM(actif)                                                 AS actives_31_12,
       SUM(actif * vivant)                                        AS actives_en_vie,
       SUM(CASE WHEN actif = 1 AND vivant = 1 AND age < 25
                THEN 1 ELSE 0 END)                                AS actives_en_vie_moins_25,
       SUM(paye)                                                  AS payees_annee,
       SUM(CASE WHEN pe_paye IS NOT NULL THEN 1 ELSE 0 END)       AS payees_ou_validees,
       SUM(paye_t4)                                               AS payees_t4,
       SUM(paye_dec)                                              AS payees_decembre,
       SUM(CASE WHEN actif = 1 AND paye = 1 THEN 1 ELSE 0 END)    AS actives_et_payees_annee,
       COUNT(DISTINCT CASE WHEN paye = 1 THEN IND_ID_PREST END)   AS personnes_payees_annee,
       ROUND(SUM(montant))                                        AS montant_paye_annee
FROM base
GROUP BY annee,
         CASE TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END
ORDER BY annee, type_pension;

-- Résultat obtenu (A01, exécutée le 30/10/2026 ; détail dans
-- comparaison_annuaire_cnss_2024/resultats/A01_tableau18.tsv), orphelins au
-- 31/12/2024 : 13 769 tous âges, dont 7 995 de moins de 25 ans ; 6 227 payés
-- dans l'année.
-- Lecture (calculé ici) : 13 769 − 7 995 = 5 774 pensions d'orphelin de 25
--   ans et plus encore en cours, presque jamais payées.


-- -----------------------------------------------------------------------------
-- Requête 2. Au 31/12/2025, par code : prestations actives par les dates,
-- payées au 4e trimestre, actives non payées, payées non actives.
-- -----------------------------------------------------------------------------
WITH paie AS (
    SELECT PE_ID,
           MAX(CASE WHEN DEB_DATE_EFFET >= DATE '2025-12-01' THEN 1 ELSE 0 END) AS paye_dec
    FROM USER_DWH.DEBOURS
    WHERE DEB_TYPE = 'PM' AND PE_ID IS NOT NULL
      AND DEB_DATE_EFFET >= DATE '2025-10-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
    GROUP BY PE_ID
),
paie_dos AS (
    SELECT DISTINCT DOS_CODE, TPE_CODE
    FROM USER_DWH.DEBOURS
    WHERE DEB_TYPE = 'PM' AND TPE_CODE IS NOT NULL
      AND DEB_DATE_EFFET >= DATE '2025-10-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
),
pe AS (
    SELECT p.PE_ID, p.DOS_CODE, p.TPE_CODE, p.PE_STATUT, p.IND_ID_PREST,
           CASE WHEN p.PE_DATE_EFFET <= DATE '2025-12-31'
                 AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
                 AND (   p.PE_DATE_SUSPENSION IS NULL
                      OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
                      OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
                          AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
                THEN 1 ELSE 0 END AS actif_dates
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                         'ARI', 'ARSV', 'ARSF', 'ARSE', 'ARSA', 'FAF', 'RBEC')
)
SELECT pe.TPE_CODE,
       COUNT(*)                                                             AS nb_prestations,
       SUM(CASE WHEN pe.PE_STATUT = 'A' THEN 1 ELSE 0 END)                  AS statut_a,
       SUM(pe.actif_dates)                                                  AS actives_par_dates,
       SUM(CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END)                AS payees_au_t4,
       SUM(NVL(pa.paye_dec, 0))                                             AS payees_en_decembre,
       SUM(CASE WHEN pe.actif_dates = 1 AND pa.PE_ID IS NOT NULL
                THEN 1 ELSE 0 END)                                          AS actives_et_payees_t4,
       SUM(CASE WHEN pe.actif_dates = 1 AND pa.PE_ID IS NULL
                THEN 1 ELSE 0 END)                                          AS actives_non_payees_t4,
       SUM(CASE WHEN pe.actif_dates = 0 AND pa.PE_ID IS NOT NULL
                THEN 1 ELSE 0 END)                                          AS payees_non_actives,
       SUM(CASE WHEN pd.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END)             AS dossier_paye_t4,
       SUM(CASE WHEN pe.actif_dates = 1 AND i.IND_DATE_DECES <= DATE '2025-12-31'
                THEN 1 ELSE 0 END)                                          AS actives_ayant_droit_decede,
       COUNT(DISTINCT CASE WHEN pa.PE_ID IS NOT NULL
                           THEN pe.IND_ID_PREST END)                        AS personnes_payees_t4
FROM pe
LEFT JOIN paie                pa ON pa.PE_ID    = pe.PE_ID
LEFT JOIN paie_dos            pd ON pd.DOS_CODE = pe.DOS_CODE AND pd.TPE_CODE = pe.TPE_CODE
LEFT JOIN USER_DWH.INDIVIDU   i  ON i.IND_ID    = pe.IND_ID_PREST
GROUP BY pe.TPE_CODE
ORDER BY nb_prestations DESC;

-- Résultat obtenu (P25, exécutée le 26/10/2026) : RPSE 13 383 actives par les
-- dates, 4 974 payées au 4e trimestre, 8 447 actives non payées, 38 payées non
-- actives.
-- Lecture : la limite de 25 ans est appliquée par le paiement, pas par la
--   radiation.
