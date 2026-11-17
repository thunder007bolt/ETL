-- =============================================================================
-- Point 37. Les veuves qui manquent (T34)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « pensions de veuve et de veuf (PRESTATION_ESP, codes
--   RPSV et RPSF) en cours au 31/12 ; décès (IND_DATE_DECES), arrêt
--   (PE_DATE_RADIATION). Pas de date de déclaration du décès. » ; « fin 2024,
--   20 359 veuves dans la base pour 21 438 dans l'annuaire CNSS 2024 (5,0 % de
--   moins), surtout à Bobo-Dioulasso et à Ouagadougou. »
-- Requêtes :
--   1. Par tableau et par sexe, les bénéficiaires comptés au 31/12/2024.
--   2. Les pensions de 2024 par direction régionale.
-- Reprise de : lot5_beneficiaires_31_12.sql, P75 (exécutée le 31/10/2026) ;
--   comparaison_annuaire_cnss_2024/requetes_comparaison.sql, A02 (exécutée le
--   30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc en_cours, sur la
--   photo de la base donnée au lancement (paramètre :1). Fiche des règles :
--   règles 2, 4 et 49.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par tableau et par sexe (IND_SEXE : 1 homme, 2 femme) : personnes
-- comptées au 31/12, montants.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2024 AS annee FROM DUAL UNION ALL SELECT 2025 FROM DUAL
),
perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T41', 'FAF'  FROM DUAL
),
pe AS (
    SELECT p.PE_ID,
           pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
),
paie AS (
    SELECT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID, EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.tableau,
           pe.sexe,
           pe.IND_ID_PREST,
           pa.montant,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (pe.tableau NOT IN ('T35', 'T39') OR a.annee - pe.annee_naissance < 25)
                 AND (pe.tableau <> 'T41' OR a.annee - pe.annee_naissance <= 21)
                THEN 1 ELSE 0 END                                 AS compte
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       tableau,
       sexe,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN IND_ID_PREST END) AS personnes_comptees,
       ROUND(SUM(CASE WHEN compte = 1 THEN montant END))          AS montant_des_comptes,
       ROUND(SUM(montant))                                        AS montant_tout_le_paye,
       COUNT(DISTINCT CASE WHEN compte = 0 AND montant > 0
                           THEN IND_ID_PREST END)                 AS payes_non_comptes
FROM base
GROUP BY annee, tableau, sexe
ORDER BY annee, tableau, sexe;

-- Résultat obtenu (P75, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P75_montants_2024_2025.tsv), T34 au 31/12/2024 :
--   20 359 femmes (veuves) et 356 hommes (veufs).
-- Face au tableau 18 de l'annuaire CNSS 2024 (p. 16) : 21 438 veuves et 329
-- veufs.
-- Lecture (calculé ici) : 1 079 veuves de moins que l'annuaire (−5,0 %) ;
--   les veufs sont 27 de plus.


-- -----------------------------------------------------------------------------
-- Requête 2. Pensions de 2024 par direction régionale du dossier : actives et
-- en vie au 31/12, payées en 2024, payées au 4e trimestre.
-- -----------------------------------------------------------------------------
WITH pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           NVL(dr.DR_SIGLE, '(sans direction)')                   AS direction,
           CASE WHEN p.PE_DATE_EFFET <= DATE '2024-12-31'
                 AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
                 AND (   p.PE_DATE_SUSPENSION IS NULL
                      OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
                      OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
                          AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
                 AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
                THEN 1 ELSE 0 END                                 AS actif_en_vie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN      USER_DWH.DOSSIER             d  ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.DIRECTION_REGIONALE dr ON dr.DR_NO   = d.DR_NO
    LEFT JOIN USER_DWH.INDIVIDU            i  ON i.IND_ID   = p.IND_ID_PREST
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
paie AS (
    SELECT d.PE_ID,
           MAX(CASE WHEN EXTRACT(MONTH FROM d.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS paye_t4
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P')
      AND d.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND d.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID
)
SELECT CASE pe.TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       pe.direction,
       SUM(pe.actif_en_vie)                                       AS actives_en_vie,
       SUM(CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END)      AS payees_2024,
       SUM(NVL(pa.paye_t4, 0))                                    AS payees_t4_2024
FROM pe
LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
GROUP BY CASE pe.TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END,
         pe.direction
ORDER BY type_pension, pe.direction;

-- Résultat obtenu (A02, exécutée le 30/10/2026 ; détail dans
-- comparaison_annuaire_cnss_2024/resultats/A02_tableau22.tsv), veuves
-- actives au 31/12/2024 : DRO 10 480, DRB 6 974, DRN 1 457, DRF 909,
-- DRD 552. Face au tableau 22 de l'annuaire CNSS 2024 (p. 18), il manque des
-- veuves à la DRO (−403) et à la DRB (−617).
-- Lecture : l'écart des veuves est surtout à Bobo-Dioulasso (DRB) et à
--   Ouagadougou (DRO), là où des radiations ont été saisies après la
--   publication.
