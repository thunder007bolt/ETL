-- =============================================================================
-- Point 42. Le tableau des enfants (T41)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « les allocations des enfants (PRESTATION_ESP, code
--   FAF, IND_ID_PREST), leur âge (IND_DATE_NAISSANCE) et leurs paiements
--   (DEBOURS). » ; « même avec les enfants non payés, la base a 32,1 % de
--   20-21 ans de moins que l'annuaire en 2024. Ceux de l'annuaire touchent en
--   moyenne 18 177 FCFA dans l'année, contre 28 000 à 34 000 FCFA pour les
--   autres tranches. »
-- Requêtes :
--   1. Les bénéficiaires comptés au 31/12/2024 par tableau, tranche d'âge et
--      sexe, dont les enfants de T41 (payés ou non).
--   2. T41 selon plusieurs définitions : enfants payés dans l'année.
-- Reprise de : lot5_beneficiaires_31_12.sql, P76 (exécutée le 31/10/2026) ;
--   lot6_ecarts_t32_t41.sql, P79 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement ; les âges sortent en tranches. La
--   requête 2 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, blocs beneficiaire et
--   tranche (le tableau des enfants est à ajouter, la requête sort T41 par
--   allocataire). Fiche des règles : règle 56.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Bénéficiaires comptés au 31/12/2024 par tableau, tranche d'âge de
-- 5 ans et sexe ; en T41, les enfants en cours, payés ou non.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
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
compte AS (
    SELECT DISTINCT pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2024-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
      AND (pm.tableau NOT IN ('T35', 'T39')
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
      AND (pm.tableau <> 'T41'
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) <= 21)
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
FROM compte
GROUP BY tableau,
         CASE WHEN age IS NULL THEN 'âge inconnu'
              WHEN age < 0     THEN 'âge négatif'
              ELSE LPAD(TO_CHAR(FLOOR(age / 5) * 5), 3, '0') || '-'
                   || LPAD(TO_CHAR(FLOOR(age / 5) * 5 + 4), 3, '0')
         END
ORDER BY tableau, tranche_5_ans;

-- Résultat obtenu (P76, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P76_ages_2024.tsv), les enfants de T41, face au tableau 36
-- de l'annuaire CNSS 2024 (p. 30) :
--   Tranche      Base (payés ou non)   Publié    Montant publié    Montant moyen publié
--   0-4 ans      71 155                59 869    1 700 588 000     28 405
--   5-9 ans      79 887                74 632    2 462 568 500     32 996
--   10-14 ans    68 923                65 913    2 217 254 000     33 639
--   15-19 ans    50 538                48 889    1 507 732 500     30 840
--   20-21 ans    13 016                19 178      348 598 500     18 177
--   (la tranche « 20-24 » de la base n'a que des enfants de 20 et 21 ans ;
--   17 enfants ont un âge négatif)
-- Lecture (calculé ici) : la base a 6 162 enfants de 20-21 ans de moins que
--   l'annuaire (−32,1 %), même sans la condition de paiement. Les 20-21 ans
--   de l'annuaire touchent 18 177 FCFA en moyenne, contre 28 405 à 33 639 pour
--   les autres tranches : comme s'ils n'étaient payés qu'une partie de l'année.


-- -----------------------------------------------------------------------------
-- Requête 2. T41 de 2020 à 2025 : enfants selon plusieurs définitions.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
pe AS (
    SELECT p.PE_ID,
           p.DOS_CODE,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE = 'FAF'
),
-- un paiement par prestation et par année ; t4 = un mois payé d'octobre à
-- décembre
paie AS (
    SELECT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee,
           MAX(CASE WHEN EXTRACT(MONTH FROM deb.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS t4
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID, EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.DOS_CODE,
           pe.IND_ID_PREST,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND a.annee - pe.annee_naissance <= 21
                THEN 1 ELSE 0 END                                 AS compte,
           CASE WHEN a.annee - pe.annee_naissance <= 21
                THEN 1 ELSE 0 END                                 AS age_ok,
           CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END       AS paye,
           NVL(pa.t4, 0)                                          AS paye_t4,
           CASE WHEN pe.date_saisie <= TO_DATE((a.annee + 1) || '0630', 'YYYYMMDD')
                THEN 1 ELSE 0 END                                 AS vu
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN IND_ID_PREST END) AS enfants_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye_t4 = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_t4,
       COUNT(DISTINCT CASE WHEN compte = 1 AND vu = 1
                           THEN IND_ID_PREST END)                 AS enfants_vus,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1 AND vu = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_vus,
       COUNT(DISTINCT CASE WHEN age_ok = 1 AND paye = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_annee,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN DOS_CODE END)     AS allocataires_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1
                           THEN DOS_CODE END)                     AS allocataires_payes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye_t4 = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_t4,
       COUNT(DISTINCT CASE WHEN compte = 1 AND vu = 1
                           THEN DOS_CODE END)                     AS allocataires_vus,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1 AND vu = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_vus,
       COUNT(DISTINCT CASE WHEN age_ok = 1 AND paye = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_annee
FROM base
GROUP BY annee
ORDER BY annee;

-- Résultat obtenu (P79, exécutée le 01/11/2026 ; détail dans
-- resultats/lot6_P79_t41_variantes.tsv), 2024 : 283 534 enfants en cours,
-- 271 577 payés dans l'année, pour 268 481 publiés (tableau 36).
