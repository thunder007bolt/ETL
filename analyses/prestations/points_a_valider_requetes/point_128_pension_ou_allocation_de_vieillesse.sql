-- =============================================================================
-- Point 128. Pension ou allocation de vieillesse (T50)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « PRESTATION_ESP, TPE_CODE = RAV ; les mois cotisés
--   sont dans PE_MOIS_COTISATION. » ; « en 2024, 831 allocataires, avec 95,5
--   mois cotisés en moyenne à la CNSS (environ 8 ans). Pourtant une allocation
--   de vieillesse en a 489 (40 ans). »
-- Requêtes :
--   1. Les services passés des allocations de vieillesse liquidées, de 2020 à
--      2025.
--   2. La qualité des mois cotisés des pensions et des allocations, toutes
--      années.
-- Reprise de : lot8_evolution_5_ans.sql, P94 (exécutée le 01/11/2026) ;
--   lot2_statuts_stocks_nouveaux.sql, P30 (exécutée le 26/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et statistiques seulement, aucune donnée
--   nominative.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, blocs liquides et
--   effectifs (les mois de la dernière allocation liquidée, de 1 à 600).
--   Fiche des règles : règle 71.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Allocations de vieillesse (RAV) liquidées chaque année :
-- personnes, moyenne et médiane des mois cotisés, part CNSS moyenne.
-- -----------------------------------------------------------------------------
WITH rav AS (
    SELECT EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION)              AS annee,
           p.PE_MOIS_COTISATION                                   AS mois,
           p.PE_COT_AUTRE                                         AS mois_autre_caisse,
           ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION),
                                           p.IND_ID_PREST
                              ORDER BY p.PE_DATE_NOTIFICATION DESC, p.PE_ID DESC) AS rang
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE = 'RAV'
      AND p.PE_DATE_NOTIFICATION >= DATE '2020-01-01'
      AND p.PE_DATE_NOTIFICATION <  DATE '2026-01-01'
)
SELECT annee,
       COUNT(*)                                                   AS personnes,
       SUM(CASE WHEN mois IS NULL THEN 1 ELSE 0 END)              AS mois_vides,
       SUM(CASE WHEN mois = 0 THEN 1 ELSE 0 END)                  AS mois_a_zero,
       SUM(CASE WHEN mois > 600 THEN 1 ELSE 0 END)                AS plus_de_600,
       SUM(CASE WHEN mois BETWEEN 1 AND 600 THEN 1 ELSE 0 END)    AS mois_valides,
       ROUND(AVG(CASE WHEN mois BETWEEN 1 AND 600
                      THEN mois END), 1)                          AS moyenne,
       MEDIAN(CASE WHEN mois BETWEEN 1 AND 600 THEN mois END)     AS mediane,
       COUNT(mois_autre_caisse)                                   AS avec_autre_caisse,
       ROUND(AVG(CASE WHEN mois BETWEEN 1 AND 600
                      THEN GREATEST(mois - NVL(mois_autre_caisse, 0), 0)
                 END), 1)                                         AS moyenne_part_cnss
FROM rav
WHERE rang = 1
GROUP BY annee
ORDER BY annee;

-- Résultat obtenu (P94, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P94_services_passes_rav.tsv), 2024 : 831 personnes, 95,7
-- mois en moyenne, médiane 98, part CNSS moyenne 95,5.


-- -----------------------------------------------------------------------------
-- Requête 2. Mois cotisés (PE_MOIS_COTISATION) par code de prestation :
-- médiane, moyenne, plus grand, plus de 600 mois.
-- -----------------------------------------------------------------------------
SELECT TPE_CODE,
       COUNT(*)                                                           AS nb_prestations,
       SUM(CASE WHEN PE_MOIS_COTISATION IS NULL THEN 1 ELSE 0 END)        AS vides,
       SUM(CASE WHEN PE_MOIS_COTISATION = 0 THEN 1 ELSE 0 END)            AS a_zero,
       MIN(PE_MOIS_COTISATION)                                            AS minimum,
       ROUND(MEDIAN(PE_MOIS_COTISATION))                                  AS mediane,
       ROUND(AVG(PE_MOIS_COTISATION), 1)                                  AS moyenne,
       MAX(PE_MOIS_COTISATION)                                            AS maximum,
       SUM(CASE WHEN PE_MOIS_COTISATION > 600 THEN 1 ELSE 0 END)          AS plus_de_600,
       SUM(CASE WHEN PE_MOIS_COTISATION IS NOT NULL
                 AND NVL(PE_MOIS_COTI_ASSU_OBLI, 0) + NVL(PE_MOIS_COTI_ASSU_VOL, 0)
                     <> PE_MOIS_COTISATION
                THEN 1 ELSE 0 END)                                        AS total_different,
       SUM(CASE WHEN PE_MOIS_COTI_ASSU_VOL > 0 THEN 1 ELSE 0 END)         AS avec_volontaire,
       SUM(CASE WHEN PE_MOIS_NOUVELLE_LOI IS NOT NULL THEN 1 ELSE 0 END)  AS avec_mois_nouvelle_loi,
       ROUND(AVG(CASE WHEN PE_DATE_EFFET >= DATE '2025-01-01'
                       AND PE_DATE_EFFET <  DATE '2026-01-01'
                      THEN PE_MOIS_COTISATION END), 1)                    AS moyenne_effet_2025
FROM USER_DWH.PRESTATION_ESP
WHERE TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RAV', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                   'ARI', 'ARSV', 'ARSE', 'AIJ', 'MIJ')
GROUP BY TPE_CODE
ORDER BY nb_prestations DESC;

-- Résultat obtenu (P30, exécutée le 26/10/2026) : RAV, toutes années :
-- médiane 92 mois, plus grand 489. RPV : médiane 295 mois.
-- Lecture : 95,5 mois en moyenne pour les allocataires de 2024 (environ
--   8 ans), mais une allocation de vieillesse a 489 mois (40 ans) : une
--   carrière de 40 ans devrait donner une pension.
