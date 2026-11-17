-- =============================================================================
-- Point 100. Les invalides âgés (T33)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « PRESTATION_ESP, code RPI, et l'âge
--   (IND_DATE_NAISSANCE). » ; « en 2024, 72 des 125 invalides ont 60 ans et
--   plus. L'annuaire CNSS 2024 fait pareil : 70 sur 119. »
-- Requête : les bénéficiaires comptés au 31/12/2024 par tableau, tranche
--   d'âge et sexe.
-- Reprise de : lot5_beneficiaires_31_12.sql, P76 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement ; les âges sortent en tranches.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, blocs en_cours et
--   tranche (T33 garde ses invalides, ligne « 60 ans et + » de la grille).
--   Fiche des règles : règle 54.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Bénéficiaires comptés au 31/12/2024 par tableau, tranche d'âge de 5 ans et
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
-- resultats/lot5_P76_ages_2024.tsv), T33 au 31/12/2024 :
--   40-44 : 6 ; 45-49 : 8 ; 50-54 : 17 ; 55-59 : 22 ; 60-64 : 24 ; 65-69 : 24 ;
--   70-74 : 12 ; 75-79 : 10 ; 80-84 : 2. Total 125.
-- Face au tableau 21 de l'annuaire CNSS 2024 : 70 invalides de 60 ans et plus
-- sur 119.
-- Lecture (calculé ici) : 24 + 24 + 12 + 10 + 2 = 72 invalides de 60 ans et
--   plus sur 125. L'application ne passe pas l'invalide en vieillesse à l'âge
--   de la retraite ; l'annuaire non plus.
