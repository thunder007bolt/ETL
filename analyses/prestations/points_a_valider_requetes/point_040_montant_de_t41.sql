-- =============================================================================
-- Point 40. Le montant de T41
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « les paiements des enfants comptés : DEBOURS,
--   DEB_TYPE = PM, DEB_STATUT D ou P, montant DEB_MONTANT. » ; « en 2024,
--   7,69 Md pour les enfants comptés, au plus 8,00 Md en prenant la date de
--   saisie. L'annuaire CNSS 2024 publie 8,24 Md, alors que l'effectif
--   retombe à 0,7 % près. Son montant est un multiple de 500 FCFA. »
-- Requêtes :
--   1. Par tableau, le montant payé aux comptés et tout le payé.
--   2. Les paiements de 2024 par code, au mois payé et à la saisie.
-- Reprise de : lot5_beneficiaires_31_12.sql, P75 (exécutée le 31/10/2026) ;
--   lot8_evolution_5_ans.sql, P98 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Les deux
--   requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, blocs paie et
--   beneficiaire (en T41, le montant des allocations des enfants comptés).
--   Fiche des règles : règles 51 et 55.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par tableau et par sexe : montant payé aux bénéficiaires comptés
-- et tout le payé (débours PM au statut D ou P).
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
-- resultats/lot5_P75_montants_2024_2025.tsv), T41 en 2024 : 7 688 913 000
-- payés aux enfants comptés ; 7 954 458 000 en tout.


-- -----------------------------------------------------------------------------
-- Requête 2. Paiements (PM) au statut D ou P de 2024 par code de prestation :
-- au mois payé, à la saisie, à l'approbation, au décaissement.
-- -----------------------------------------------------------------------------
SELECT p.TPE_CODE                                                 AS code,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_EFFET >= DATE '2024-01-01'
                       AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS mois_paye_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_INSERT >= DATE '2024-01-01'
                       AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS saisi_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_APPROBATION >= DATE '2024-01-01'
                       AND deb.DEB_DATE_APPROBATION <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS approuve_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_DECAISSEMENT >= DATE '2024-01-01'
                       AND deb.DEB_DATE_DECAISSEMENT <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS decaisse_2024,
       SUM(CASE WHEN deb.DEB_DATE_INSERT >= DATE '2024-01-01'
                 AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
                 AND deb.DEB_DATE_DECAISSEMENT IS NULL
                THEN 1 ELSE 0 END)                                AS saisis_sans_decaissement
FROM USER_DWH.DEBOURS deb
JOIN USER_DWH.PRESTATION_ESP p ON p.PE_ID = deb.PE_ID
WHERE deb.DEB_TYPE = 'PM'
  AND deb.DEB_STATUT IN ('D', 'P')
  AND (   (deb.DEB_DATE_EFFET        >= DATE '2024-01-01' AND deb.DEB_DATE_EFFET        < DATE '2025-01-01')
       OR (deb.DEB_DATE_INSERT       >= DATE '2024-01-01' AND deb.DEB_DATE_INSERT       < DATE '2025-01-01')
       OR (deb.DEB_DATE_APPROBATION  >= DATE '2024-01-01' AND deb.DEB_DATE_APPROBATION  < DATE '2025-01-01')
       OR (deb.DEB_DATE_DECAISSEMENT >= DATE '2024-01-01' AND deb.DEB_DATE_DECAISSEMENT < DATE '2025-01-01'))
GROUP BY p.TPE_CODE
ORDER BY p.TPE_CODE;

-- Résultat obtenu (P98, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P98_montants_2024_par_date.tsv), FAF en 2024 : 7 954 458 000
-- au mois payé, 8 000 440 000 à la saisie, 8 015 320 000 à l'approbation.
-- Face à l'annuaire CNSS 2024 : 8 236 741 500 d'allocations familiales
-- (tableau 40, p. 32 ; tableau 36, p. 30).
-- Lecture : 7,69 Md pour les enfants comptés, au plus 8,00 Md à la saisie,
--   pour 8,24 Md publiés. 8 236 741 500 est un multiple de 500, comme les
--   allocations, pas comme les rappels versés par une correction de paiement
--   (AJUSTEMENT) : le publié n'en contient sans doute pas.
