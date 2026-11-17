-- =============================================================================
-- Point 16. Les services passés
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « les mois cotisés (PE_MOIS_COTISATION) de la dernière
--   pension de la personne saisie dans l'année (pour T50, de sa dernière
--   allocation de vieillesse liquidée dans l'année), écartés au-delà de 600
--   mois (50 ans). » ; « le champ n'est jamais vide de 2020 à 2025. Médiane de
--   270 mois pour les nouveaux retraités de 2024 hors coordination ; 95,5
--   mois en moyenne (part CNSS) pour les 831 allocataires de vieillesse de
--   2024. »
-- Requêtes :
--   1. Les services passés des nouveaux retraités (T42), par année et par
--      catégorie de dossier.
--   2. Les services passés des allocations de vieillesse liquidées (T50).
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P86, et
--   lot8_evolution_5_ans.sql, P94 (exécutées le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et moyennes seulement, aucune donnée nominative.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, blocs entree et
--   beneficiaire (mois de la dernière pension saisie, de 1 à 600, sans les
--   mois de l'autre caisse) ; t50_t57_requete_proposee.sql pour T50. Fiche
--   des règles : règle 66.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Nouveaux retraités (RPV, RPRA), dernière prestation saisie de
-- l'année, de 2020 à 2025 : mois vides, à zéro ou au-dessus de 600 ; par
-- catégorie, personnes, moyenne et médiane des mois.
-- -----------------------------------------------------------------------------
WITH nouveaux AS (
    SELECT EXTRACT(YEAR FROM p.PE_DATE_INSERT)                    AS annee,
           p.PE_MOIS_COTISATION                                   AS mois,
           p.PE_COT_AUTRE                                         AS cot_autre,
           NVL(d.DOS_CATEGORIE, '(sans catégorie)')               AS categorie,
           ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM p.PE_DATE_INSERT),
                                           p.IND_ID_PREST
                              ORDER BY p.PE_DATE_INSERT DESC, p.PE_ID DESC) AS rang
    FROM USER_DWH.PRESTATION_ESP p
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
    WHERE p.TPE_CODE IN ('RPV', 'RPRA')
      AND p.PE_DATE_INSERT >= DATE '2020-01-01'
      AND p.PE_DATE_INSERT <  DATE '2026-01-01'
)
SELECT annee,
       categorie,
       COUNT(*)                                                   AS personnes,
       SUM(CASE WHEN mois IS NULL THEN 1 ELSE 0 END)              AS mois_vides,
       SUM(CASE WHEN mois = 0 THEN 1 ELSE 0 END)                  AS mois_a_zero,
       SUM(CASE WHEN mois > 600 THEN 1 ELSE 0 END)                AS plus_de_600,
       SUM(CASE WHEN mois BETWEEN 1 AND 600 THEN 1 ELSE 0 END)    AS mois_valides,
       SUM(CASE WHEN mois BETWEEN 1 AND 600 THEN mois END)        AS total_mois_valides,
       ROUND(AVG(CASE WHEN mois BETWEEN 1 AND 600
                      THEN mois END), 1)                          AS moyenne,
       MEDIAN(CASE WHEN mois BETWEEN 1 AND 600 THEN mois END)     AS mediane,
       COUNT(cot_autre)                                           AS avec_cot_autre
FROM nouveaux
WHERE rang = 1
GROUP BY annee, categorie
ORDER BY annee, personnes DESC;

-- Résultat obtenu (P86, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P86_services_passes.tsv) : aucun mois vide ni à zéro de
-- 2020 à 2025 ; un seul au-dessus de 600 (2025, COOR_AC). En 2024, dossiers
-- ordinaires : 2 152 personnes, moyenne 286,8 mois, médiane 270.


-- -----------------------------------------------------------------------------
-- Requête 2. Allocations de vieillesse (RAV) liquidées chaque année :
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
-- resultats/lot8_P94_services_passes_rav.tsv) :
--   Année   Personnes   Moyenne   Médiane   Part CNSS moyenne
--   2020    585         85,2      86        84,9
--   2021    594         88,2      85        88,2
--   2022    707         89,5      86        89,3
--   2023    738         93,9      96        93,9
--   2024    831         95,7      98        95,5
--   2025    823         95,6      96        95,6
-- Lecture : le champ est toujours rempli ; 270 mois en médiane pour les
--   nouveaux retraités ordinaires de 2024, 95,5 mois en moyenne (part CNSS)
--   pour les 831 allocataires de vieillesse.
