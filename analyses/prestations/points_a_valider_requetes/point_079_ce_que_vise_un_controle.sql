-- =============================================================================
-- Point 79. Ce que vise un contrôle
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T28 à
--   T30.
-- Ce que dit le point : « DOSSIERS_CONTROLES a une colonne pour le code de la
--   prestation contrôlée ; on ne sait pas encore si elle est remplie. » ;
--   « 62 231 pensions au 31/12/2024 dans l'annuaire CNSS 2024 (tableaux 18 et
--   22), 47 732 dossiers de pension (TDOS_CODE = V) qui ont reçu un débours
--   en 2025 dans la base. »
-- Requêtes :
--   1. La colonne du code de la prestation contrôlée (TPE_CODE) : remplie ou
--      non, et avec quels codes.
--   2. Les dossiers payés de 2025 par branche (au moins un débours).
-- Reprise de : la requête 1 est nouvelle ; la requête 2 est
--   lot1_inventaire_prestations.sql, P13 (exécutée le 26/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 2
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : aucune requête proposée, T28 à T30 ne sont pas
--   produits. Fiche des règles : règle 37.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Les contrôles (DOSSIERS_CONTROLES) par branche (TDOS_CODE) et par
-- code de la prestation contrôlée (TPE_CODE, vide ou non). Les colonnes de la
-- table sont celles que copie le chargement des faits
-- (domaines/prestation_recouvrement/facts/sql/controle_prestation.sql).
-- -----------------------------------------------------------------------------
SELECT TO_CHAR(TDOS_CODE)                                         AS branche,
       NVL(TO_CHAR(TPE_CODE), '(vide)')                           AS code_controle,
       COUNT(*)                                                   AS nb_controles,
       COUNT(DISTINCT DOS_CODE)                                   AS nb_dossiers
FROM USER_DWH.DOSSIERS_CONTROLES
GROUP BY TDOS_CODE, TPE_CODE
ORDER BY 1, 2;

-- Résultat : pas encore exécutée.
-- Lecture attendue : si TPE_CODE est rempli, chaque contrôle va sur la ligne
--   de sa prestation ; s'il est vide, on ne connaît que la branche du dossier.


-- -----------------------------------------------------------------------------
-- Requête 2. Contrôles a posteriori, débours vérifiés et dossiers payés (au
-- moins un débours dont le mois payé, DEB_DATE_EFFET, tombe dans l'année),
-- par année et par branche, de 2019 à 2025.
-- -----------------------------------------------------------------------------
SELECT 'a. Dossiers contrôlés (DOSSIERS_CONTROLES)' AS source,
       EXTRACT(YEAR FROM DC_DATE_CONTROLE) AS annee, TO_CHAR(TDOS_CODE) AS branche,
       COUNT(*) AS nb_lignes, COUNT(DISTINCT DOS_CODE) AS nb_dossiers
FROM USER_DWH.DOSSIERS_CONTROLES
GROUP BY EXTRACT(YEAR FROM DC_DATE_CONTROLE), TDOS_CODE
UNION ALL
SELECT 'b. Débours vérifiés (DEB_VERIFIE = O)',
       EXTRACT(YEAR FROM DEB_DATE_EFFET), TO_CHAR(CODE_BRANCHE),
       COUNT(*), COUNT(DISTINCT DOS_CODE)
FROM USER_DWH.DEBOURS
WHERE DEB_VERIFIE = 'O'
  AND DEB_DATE_EFFET >= DATE '2019-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM DEB_DATE_EFFET), CODE_BRANCHE
UNION ALL
SELECT 'c. Dossiers payés (au moins un débours)',
       EXTRACT(YEAR FROM DEB_DATE_EFFET), TO_CHAR(CODE_BRANCHE),
       COUNT(*), COUNT(DISTINCT DOS_CODE)
FROM USER_DWH.DEBOURS
WHERE DEB_DATE_EFFET >= DATE '2019-01-01' AND DEB_DATE_EFFET < DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM DEB_DATE_EFFET), CODE_BRANCHE
ORDER BY 1, 3, 2;

-- Résultat obtenu (P13, exécutée le 26/10/2026), dossiers payés en 2025 :
--   A 2 134 ; F 100 113 ; M 5 227 ; V 47 732.
-- Face à l'annuaire CNSS 2024 : 62 231 pensions au 31/12/2024 (tableaux 18 et
-- 22, p. 16 et 18 ; « bénéficiaires de la branche des pensions » des chiffres
-- clés).
-- Lecture : 47 732 dossiers de pension payés en 2025, pour 62 231 pensions
--   publiées fin 2024 : un ordre de grandeur, les années diffèrent. Une
--   pension n'est pas un dossier : la veuve et les orphelins sont souvent
--   payés sur le dossier du défunt.
