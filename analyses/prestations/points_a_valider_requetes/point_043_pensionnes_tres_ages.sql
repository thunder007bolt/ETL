-- =============================================================================
-- Point 43. Les pensionnés très âgés
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « l'âge (INDIVIDU, IND_DATE_NAISSANCE), le décès
--   (IND_DATE_DECES) et la pension en cours (PRESTATION_ESP, sans
--   PE_DATE_RADIATION). » ; « fin 2025, 69 pensionnés payés ont 100 ans ou
--   plus, et 215 pensions sont encore actives alors que leur titulaire est
--   décédé. En 2024, 74 retraités de 100 ans et plus dans la base, 56 dans
--   l'annuaire CNSS 2024. »
-- Requêtes :
--   1. Au 31/12/2025 : prestations actives par les dates, payées au 4e
--      trimestre, et actives dont l'ayant droit est décédé.
--   2. Esquisse de T32 au 31/12/2025, dont les 100 ans et plus.
--   3. Les bénéficiaires comptés au 31/12/2024 par tranche d'âge.
-- Reprise de : lot2_statuts_stocks_nouveaux.sql, P25 et P26 (exécutées le
--   26/10/2026) ; lot5_beneficiaires_31_12.sql, P76 (exécutée le
--   31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement ; les âges sortent en
--   tranches. Les requêtes 1 et 2 lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc en_cours (la
--   personne compte tant que sa pension est en cours et qu'elle n'est pas
--   déclarée décédée). Fiche des règles : règle 49.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Au 31/12/2025, par code : prestations actives par les dates,
-- payées au 4e trimestre, actives non payées, payées non actives, et actives
-- dont l'ayant droit est décédé.
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

-- Résultat obtenu (P25, exécutée le 26/10/2026) : RPV 32 974 actives par les
-- dates, 29 643 payées au 4e trimestre ; actives dont l'ayant droit est
-- décédé : RPV 215, RPSV 439, FAF 85.


-- -----------------------------------------------------------------------------
-- Requête 2. Pensionnés de vieillesse (RPV et RPRA) payés au 4e trimestre
-- 2025, par sexe et par tranche d'âge, dont les 100 ans et plus.
-- -----------------------------------------------------------------------------
WITH paie AS (
    SELECT PE_ID,
           SUM(DEB_MONTANT)                                                  AS montant_2025,
           MAX(CASE WHEN DEB_DATE_EFFET >= DATE '2025-10-01' THEN 1 ELSE 0 END) AS paye_t4
    FROM USER_DWH.DEBOURS
    WHERE DEB_TYPE = 'PM' AND PE_ID IS NOT NULL
      AND DEB_DATE_EFFET >= DATE '2025-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
    GROUP BY PE_ID
),
base AS (
    SELECT p.IND_ID_PREST, i.IND_SEXE,
           2025 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) AS age,
           CASE WHEN p.PE_DATE_EFFET <= DATE '2025-12-31'
                 AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
                 AND (   p.PE_DATE_SUSPENSION IS NULL
                      OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
                      OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
                          AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
                THEN 1 ELSE 0 END                         AS actif_dates,
           NVL(pa.paye_t4, 0)                             AS paye_t4,
           pa.montant_2025
    FROM USER_DWH.PRESTATION_ESP p
    JOIN USER_DWH.INDIVIDU i  ON i.IND_ID  = p.IND_ID_PREST
    LEFT JOIN paie        pa ON pa.PE_ID  = p.PE_ID
    WHERE p.TPE_CODE IN ('RPV', 'RPRA')
),
tranche AS (
    SELECT b.*,
           CASE WHEN age < 55   THEN '00 moins de 55'
                WHEN age >= 100 THEN '20 100 et plus'
                ELSE LPAD(TO_CHAR(FLOOR(age / 5)), 2, '0') || ' '
                     || TO_CHAR(FLOOR(age / 5) * 5) || '-' || TO_CHAR(FLOOR(age / 5) * 5 + 4)
           END AS tranche_t32
    FROM base b
)
SELECT tranche_t32, IND_SEXE,
       COUNT(DISTINCT CASE WHEN actif_dates = 1 THEN IND_ID_PREST END) AS effectif_actif_dates,
       COUNT(DISTINCT CASE WHEN paye_t4 = 1 THEN IND_ID_PREST END)     AS effectif_paye_t4,
       SUM(CASE WHEN paye_t4 = 1 THEN montant_2025 END)                AS montant_2025_payes_t4
FROM tranche
GROUP BY tranche_t32, IND_SEXE
ORDER BY tranche_t32, IND_SEXE;

-- Résultat obtenu (P26, exécutée le 26/10/2026) : 29 721 pensionnés payés au
-- 4e trimestre 2025 ; 100 ans et plus : 69 payés (91 actifs).


-- -----------------------------------------------------------------------------
-- Requête 3. Bénéficiaires comptés au 31/12/2024 par tableau, tranche d'âge et
-- sexe.
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
-- resultats/lot5_P76_ages_2024.tsv), T32 au 31/12/2024 : 74 pensionnés de
-- 100 ans et plus (dont 1 de 120 à 124 ans et 26 de 105 à 119 ans).
-- Face au tableau 19 de l'annuaire CNSS 2024 : 56 retraités de 100 ans et
-- plus.
-- Lecture : fin 2025, 69 pensionnés payés ont 100 ans ou plus, et 215
--   pensions de vieillesse restent actives alors que leur titulaire est
--   décédé : un contrôle d'existence est à faire.
