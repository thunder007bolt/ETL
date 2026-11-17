-- =============================================================================
-- Point 44. Le sexe des nouveaux retraités (T42)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « le sexe de la fiche de la personne (INDIVIDU,
--   IND_SEXE). » ; « l'annuaire CNSS 2024 compte autrement : son tableau 25
--   donne 527 femmes (21,2 %) parmi les nouveaux retraités de 2024, la base
--   641 (25,6 %), révisions comprises. Sur l'ensemble des retraités, le sexe
--   retombe à moins de 3 % près. »
-- Requêtes :
--   1. Les nouveaux retraités de 2024 par âge et par sexe.
--   2. Les retraités comptés au 31/12/2024 par sexe.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P83 (exécutée le
--   01/11/2026) ; lot5_beneficiaires_31_12.sql, P75 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 2 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, bloc detail (le sexe de
--   la fiche INDIVIDU). Fiche des règles : règle 12.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Nouveaux retraités (RPV) saisis en 2024, par âge et par sexe
-- (IND_SEXE : 1 homme, 2 femme).
-- -----------------------------------------------------------------------------
WITH nouveaux AS (
    SELECT DISTINCT
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE = 'RPV'
      AND p.PE_DATE_INSERT >= DATE '2024-01-01'
      AND p.PE_DATE_INSERT <  DATE '2025-01-01'
)
SELECT CASE WHEN age IS NULL OR age < 0 OR age >= 120 THEN 'âge inconnu'
            WHEN age < 53  THEN 'moins de 53 ans'
            WHEN age >= 70 THEN '70 ans et plus'
            ELSE TO_CHAR(age) || ' ans'
       END                                                        AS age_2024,
       SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END)                AS sexe_1,
       SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END)                AS sexe_2,
       SUM(CASE WHEN sexe NOT IN ('1', '2') THEN 1 ELSE 0 END)    AS sexe_inconnu,
       COUNT(*)                                                   AS personnes
FROM nouveaux
GROUP BY CASE WHEN age IS NULL OR age < 0 OR age >= 120 THEN 'âge inconnu'
              WHEN age < 53  THEN 'moins de 53 ans'
              WHEN age >= 70 THEN '70 ans et plus'
              ELSE TO_CHAR(age) || ' ans'
         END
ORDER BY MIN(CASE WHEN age IS NULL OR age < 0 OR age >= 120 THEN 999 ELSE age END);

-- Résultat obtenu (P83, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P83_ages_nouveaux_retraites_2024.tsv) : 2 506 personnes,
-- 1 865 hommes et 641 femmes, face au tableau 25 de l'annuaire CNSS 2024
-- (p. 20) : 1 959 hommes et 527 femmes, 2 486 en tout.
-- Lecture (calculé ici) : 641 ÷ 2 506 = 25,6 % de femmes dans la base,
--   527 ÷ 2 486 = 21,2 % dans l'annuaire.


-- -----------------------------------------------------------------------------
-- Requête 2. Bénéficiaires comptés au 31/12/2024 par tableau et par sexe.
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
-- resultats/lot5_P75_montants_2024_2025.tsv), T32 au 31/12/2024 : 24 814
-- hommes et 6 279 femmes, face au tableau 19 de l'annuaire (24 627 hommes et
-- 6 103 femmes).
-- Lecture (calculé ici) : sur l'ensemble des retraités, +0,8 % d'hommes et
--   +2,9 % de femmes : le sexe retombe à moins de 3 % près. L'écart des
--   nouveaux retraités vient donc du tableau 25, pas de la fiche.
