-- =============================================================================
-- Point 11. La coordination
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « la catégorie du dossier (DOS_CATEGORIE) vaut COOR_AC
--   quand l'autre caisse paie, COOR_CNSS quand la CNSS paie. Les mois cotisés
--   sont dans PE_MOIS_COTISATION, ceux de l'autre caisse dans PE_COT_AUTRE. » ;
--   « parmi les nouveaux retraités de 2024, PE_COT_AUTRE est rempli pour les
--   234 COOR_AC et pour 118 des 123 COOR_CNSS (révisions comprises) ; il est
--   en moyenne plus petit que le total des mois. »
-- Requêtes :
--   1. Les services passés des nouveaux retraités par catégorie de dossier.
--   2. Les mois cotisés des pensions de vieillesse saisies en 2024, par
--      catégorie : total, et valeurs de PE_COT_AUTRE.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P86 et P89 (exécutées le
--   01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et moyennes seulement, aucune donnée nominative.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, bloc entree
--   (PE_MOIS_COTISATION et PE_COT_AUTRE) et bloc paie (une pension COOR_AC n'a
--   aucun paiement de la CNSS). Fiche des règles : règles 11 et 66.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Nouveaux retraités (RPV, RPRA), dernière prestation saisie de
-- l'année, par année et par catégorie du dossier : personnes, moyenne et
-- médiane des mois cotisés, avec des cotisations dans une autre caisse.
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
-- resultats/lot7_P86_services_passes.tsv), en 2024 :
--   Catégorie        Personnes   Moyenne des mois   Médiane   Avec PE_COT_AUTRE
--   ORDINAIRE        2 152       286,8              270       1
--   COOR_AC            234       342,3              342,5     234
--   COOR_CNSS          123       334,2              322       118
--   sans catégorie       1       454                454       1


-- -----------------------------------------------------------------------------
-- Requête 2. Pensions de vieillesse et de retraite anticipée saisies en 2024,
-- par catégorie : moyenne des mois (total, obligatoires, volontaires) et
-- PE_COT_AUTRE (rempli, nombre de valeurs, plus petit, plus grand).
-- -----------------------------------------------------------------------------
SELECT NVL(d.DOS_CATEGORIE, '(sans catégorie)')                   AS categorie,
       COUNT(*)                                                   AS prestations,
       ROUND(AVG(p.PE_MOIS_COTISATION), 1)                        AS moy_mois_total,
       ROUND(AVG(p.PE_MOIS_COTI_ASSU_OBLI), 1)                    AS moy_mois_obligatoires,
       ROUND(AVG(p.PE_MOIS_COTI_ASSU_VOL), 1)                     AS moy_mois_volontaires,
       COUNT(p.PE_COT_AUTRE)                                      AS avec_cot_autre,
       COUNT(DISTINCT p.PE_COT_AUTRE)                             AS valeurs_cot_autre,
       MIN(p.PE_COT_AUTRE)                                        AS min_cot_autre,
       MAX(p.PE_COT_AUTRE)                                        AS max_cot_autre,
       COUNT(p.PE_MOIS_NOUVELLE_LOI)                              AS avec_mois_nouvelle_loi,
       COUNT(p.PE_NO_PENSION_AUTRE)                               AS avec_no_pension_autre
FROM USER_DWH.PRESTATION_ESP p
JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
WHERE p.TPE_CODE IN ('RPV', 'RPRA')
  AND p.PE_DATE_INSERT >= DATE '2024-01-01'
  AND p.PE_DATE_INSERT <  DATE '2025-01-01'
GROUP BY NVL(d.DOS_CATEGORIE, '(sans catégorie)')
ORDER BY prestations DESC;

-- Résultat obtenu (P89, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P89_mois_par_categorie_2024.tsv) :
--   Catégorie   Prestations   Moyenne des mois   PE_COT_AUTRE rempli   Plus petit   Plus grand
--   COOR_AC     235           342                235                   110          446
--   COOR_CNSS   126           332,6              120                     6          464
--   ORDINAIRE   2 162         286,8                2                     6            6
-- Lecture : PE_COT_AUTRE n'est rempli qu'en coordination, et il est plus
--   petit que le total des mois : ce sont les mois de l'autre caisse. La part
--   CNSS est PE_MOIS_COTISATION moins PE_COT_AUTRE.
