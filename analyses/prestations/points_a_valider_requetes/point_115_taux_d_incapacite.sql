-- =============================================================================
-- Point 115. Le taux d'incapacité (T48)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « la rente viagère (PRESTATION_ESP, code ARI) et son
--   taux (PE_TAUX_INCAPACITE). » ; « de 2020 à 2025, le taux va de 15 à
--   100 %. Parmi les 39 nouveaux de 2024, au plus 1 personne a eu deux rentes
--   dans l'année. »
-- Requête : le degré d'IPP des rentes viagères saisies chaque année, de 2020 à
--   2025.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P87 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et statistiques seulement, aucune donnée
--   nominative.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, blocs entree et
--   beneficiaire (le taux de la dernière rente viagère saisie dans l'année).
--   Fiche des règles : règle 67.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Rentes viagères (ARI) saisies chaque année, de 2020 à 2025 : personnes,
-- rentes, plus petit, médiane, moyenne et plus grand taux
-- (PE_TAUX_INCAPACITE), répartition par tranche de taux.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM PE_DATE_INSERT)                          AS annee,
       COUNT(DISTINCT IND_ID_PREST)                               AS personnes,
       COUNT(*)                                                   AS rentes,
       SUM(CASE WHEN PE_TAUX_INCAPACITE IS NULL THEN 1 ELSE 0 END) AS taux_vide,
       SUM(CASE WHEN PE_TAUX_INCAPACITE = 0 THEN 1 ELSE 0 END)    AS taux_a_zero,
       SUM(CASE WHEN PE_TAUX_INCAPACITE > 100 THEN 1 ELSE 0 END)  AS plus_de_100,
       MIN(PE_TAUX_INCAPACITE)                                    AS minimum,
       MEDIAN(PE_TAUX_INCAPACITE)                                 AS mediane,
       ROUND(AVG(CASE WHEN PE_TAUX_INCAPACITE BETWEEN 1 AND 100
                      THEN PE_TAUX_INCAPACITE END), 1)            AS moyenne,
       MAX(PE_TAUX_INCAPACITE)                                    AS maximum,
       SUM(CASE WHEN PE_TAUX_INCAPACITE > 0
                 AND PE_TAUX_INCAPACITE < 10 THEN 1 ELSE 0 END)   AS de_1_a_9,
       SUM(CASE WHEN PE_TAUX_INCAPACITE >= 10
                 AND PE_TAUX_INCAPACITE < 20 THEN 1 ELSE 0 END)   AS de_10_a_19,
       SUM(CASE WHEN PE_TAUX_INCAPACITE >= 20
                 AND PE_TAUX_INCAPACITE < 40 THEN 1 ELSE 0 END)   AS de_20_a_39,
       SUM(CASE WHEN PE_TAUX_INCAPACITE >= 40
                 AND PE_TAUX_INCAPACITE < 60 THEN 1 ELSE 0 END)   AS de_40_a_59,
       SUM(CASE WHEN PE_TAUX_INCAPACITE >= 60
                 AND PE_TAUX_INCAPACITE <= 100 THEN 1 ELSE 0 END) AS de_60_a_100
FROM USER_DWH.PRESTATION_ESP
WHERE TPE_CODE = 'ARI'
  AND PE_DATE_INSERT >= DATE '2020-01-01'
  AND PE_DATE_INSERT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM PE_DATE_INSERT)
ORDER BY annee;

-- Résultat obtenu (P87, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P87_taux_ipp_rentes_viageres.tsv) :
--   Année   Personnes   Rentes   Plus petit   Médiane   Moyenne   Plus grand
--   2020    43          43       15           25        31,7      92
--   2021    63          63       15           25        30,3      85
--   2022    76          78       15           25        28,1      70
--   2023    41          41       15           23,5      29,6      85
--   2024    68          69       15           24        27,3      70
--   2025    84          85       15           25        30,6      100
--   Jamais vide, jamais à zéro, jamais au-dessus de 100 ; aucun taux de 1 à 9.
-- Lecture : le taux va de 15 à 100 % ; en 2024, 69 rentes pour 68 personnes :
--   au plus 1 personne a eu deux rentes dans l'année.
