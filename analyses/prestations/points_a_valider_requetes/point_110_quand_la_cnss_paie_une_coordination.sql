-- =============================================================================
-- Point 110. Quand la CNSS paie une coordination
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « DOS_CATEGORIE = COOR_CNSS ; le montant est la somme
--   des paiements (DEBOURS). » ; « 123 nouveaux retraités COOR_CNSS saisis en
--   2024, révisions comprises. »
-- Requête : les services passés des nouveaux retraités, par année et par
--   catégorie de dossier.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P86 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et moyennes seulement, aucune donnée nominative.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, bloc paie (le montant
--   est ce que la CNSS a payé). Fiche des règles : règle 11.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Nouveaux retraités (RPV, RPRA), dernière prestation saisie de l'année, par
-- année et par catégorie du dossier : personnes, mois cotisés.
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
-- resultats/lot7_P86_services_passes.tsv), en 2024 : COOR_CNSS 123
-- personnes (révisions comprises), 334,2 mois en moyenne, 118 avec des
-- cotisations dans une autre caisse.
-- Lecture : pour ces 123 nouveaux retraités, la CNSS paie : si elle verse
--   toute la pension et se fait rembourser la part de l'autre caisse, le
--   montant de T42 est trop haut. La base ne dit pas si un remboursement
--   suit.
