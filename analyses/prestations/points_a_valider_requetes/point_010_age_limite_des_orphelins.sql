-- =============================================================================
-- Point 10. L'âge limite des orphelins
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « les pensions d'orphelin (PRESTATION_ESP, code RPSE)
--   et l'âge (IND_DATE_NAISSANCE). » ; « l'annuaire CNSS 2024 fait pareil au
--   31/12 : ses 9 478 orphelins ont tous moins de 25 ans. En 2024, 2 nouveaux
--   orphelins pensionnés (T44) ont de 25 à 29 ans. »
-- Requêtes :
--   1. Les pensions de 2020 à 2024 au 31/12, dont les orphelins de moins de
--      25 ans.
--   2. Les nouveaux bénéficiaires de 2024 par tableau et tranche d'âge.
-- Reprise de : comparaison_annuaire_cnss_2024/requetes_comparaison.sql, A01
--   (exécutée le 30/10/2026) ; lot7_nouveaux_beneficiaires.sql, P84
--   (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 1 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc en_cours
--   (orphelins de moins de 25 ans au 31/12) ; t42_t49_requete_proposee.sql,
--   bloc tranche (pas de limite pour les nouveaux). Fiche des règles :
--   règle 50.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Pensions actives et en vie au 31/12, de 2020 à 2024, par type,
-- selon plusieurs définitions, dont les orphelins de moins de 25 ans.
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
-- comparaison_annuaire_cnss_2024/resultats/A01_tableau18.tsv), orphelins
-- au 31/12, de 2020 à 2024 :
--   tous âges        15 130 / 15 313 / 15 752 / 13 741 / 13 769
--   moins de 25 ans   9 705 /  9 558 /  9 296 /  8 123 /  7 995
-- Face au tableau 20 de l'annuaire CNSS 2024 (p. 17) : 9 478 orphelins fin
-- 2024, tous dans les tranches de 0 à 24 ans.


-- -----------------------------------------------------------------------------
-- Requête 2. Nouveaux bénéficiaires de 2024 (première prestation du tableau),
-- par tableau, tranche d'âge de 5 ans et sexe.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T42' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T42', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T44', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T45', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T46', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T47', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T48', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T49', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T49', 'ARSF' FROM DUAL
),
nouveaux AS (
    SELECT DISTINCT
           pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_INSERT >= DATE '2024-01-01'
      AND p.PE_DATE_INSERT <  DATE '2025-01-01'
)
SELECT tableau,
       CASE WHEN age IS NULL THEN 'âge inconnu'
            WHEN age < 0     THEN 'âge négatif'
            ELSE LPAD(TO_CHAR(FLOOR(age / 5) * 5), 3, '0') || '-'
                 || LPAD(TO_CHAR(FLOOR(age / 5) * 5 + 4), 3, '0')
       END                                                        AS tranche_5_ans,
       SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END)                AS sexe_1,
       SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END)                AS sexe_2,
       SUM(CASE WHEN sexe NOT IN ('1', '2') THEN 1 ELSE 0 END)    AS sexe_inconnu,
       COUNT(*)                                                   AS personnes
FROM nouveaux
GROUP BY tableau,
         CASE WHEN age IS NULL THEN 'âge inconnu'
              WHEN age < 0     THEN 'âge négatif'
              ELSE LPAD(TO_CHAR(FLOOR(age / 5) * 5), 3, '0') || '-'
                   || LPAD(TO_CHAR(FLOOR(age / 5) * 5 + 4), 3, '0')
         END
ORDER BY tableau, tranche_5_ans;

-- Résultat obtenu (P84, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P84_ages_nouveaux_2024.tsv), T44 (nouveaux orphelins de
-- pension) : 10-14 ans 196 ; 15-19 ans 286 ; 20-24 ans 139 ; 25-29 ans 2.
-- Lecture : au 31/12, l'annuaire ne compte que les orphelins de moins de
--   25 ans ; parmi les nouveaux de 2024, 2 ont de 25 à 29 ans, et ils vont en
--   « 20 ans et plus ».
