-- =============================================================================
-- Point 113. La dernière tranche d'âge (T43)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « pensions RPSV et RPSF (PRESTATION_ESP), âge tiré de
--   la date de naissance (IND_DATE_NAISSANCE). » ; « 413 des 772 nouveaux
--   veufs et veuves saisis en 2024 (53 %) tombent dans cette tranche. »
-- Requête : les nouveaux bénéficiaires de 2024 par tableau, tranche d'âge et
--   sexe.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P84 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement ; les âges sortent en tranches.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, blocs grille et
--   tranche (la grille de l'Annuaire CIPRES, T43 jusqu'à « 60 ans et plus »).
--   Fiche des règles : règle 61.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Nouveaux bénéficiaires saisis en 2024, par tableau, tranche d'âge de 5 ans
-- et sexe.
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
-- resultats/lot7_P84_ages_nouveaux_2024.tsv) : T43, 772 nouveaux veufs et
-- veuves en 2024, de 25 à 89 ans, dont 413 de 60 ans et plus ; 35 hommes.
-- Lecture (calculé ici) : 413 ÷ 772 = 53 % dans la dernière tranche de la
--   grille, « 60 ans et plus » (maquette de l'Annuaire CIPRES, p. 42).
