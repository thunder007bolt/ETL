-- =============================================================================
-- Point 69. La table des contrôles, pour le passé
-- Fiche : points_a_valider_v2.md, rubrique « À trancher en priorité ».
-- Ce que dit le point : « la table DOSSIERS_CONTROLES a 1 542 lignes en tout,
--   de 1 à 212 contrôles par an et par branche ; la dernière saisie date du
--   09/07/2025. » ; « 5 % des 155 206 dossiers qui ont reçu au moins un
--   débours en 2025, ce serait environ 7 760 contrôles dans l'année. »
-- Requêtes :
--   1. Les contrôles de la table, par année et par branche.
--   2. Le nombre de contrôles qu'il faudrait en 2025 pour la norme de 5 %.
-- Reprise de : lot1_inventaire_prestations.sql, P01 et P13 (exécutées le
--   26/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 2
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : aucune requête, la table ne sert pas, même pour le
--   passé. Fiche des règles : règle 39.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Les contrôles par année (de DC_DATE_CONTROLE) et par branche
-- (TDOS_CODE : A accidents du travail, F prestations familiales, M maternité,
-- V pensions), avec le total de la table et la dernière saisie.
-- -----------------------------------------------------------------------------
SELECT 1                                                          AS ordre,
       EXTRACT(YEAR FROM DC_DATE_CONTROLE)                        AS annee,
       TO_CHAR(TDOS_CODE)                                         AS branche,
       COUNT(*)                                                   AS nb_controles,
       COUNT(DISTINCT DOS_CODE)                                   AS nb_dossiers,
       MAX(DC_DATE_INSERT)                                        AS derniere_saisie
FROM USER_DWH.DOSSIERS_CONTROLES
GROUP BY EXTRACT(YEAR FROM DC_DATE_CONTROLE), TDOS_CODE
UNION ALL
SELECT 2, NULL, 'Total', COUNT(*), COUNT(DISTINCT DOS_CODE), MAX(DC_DATE_INSERT)
FROM USER_DWH.DOSSIERS_CONTROLES
ORDER BY 1, 2, 3;

-- Résultat obtenu (P01 et P13) :
--   · 1 542 lignes en tout ; dernière saisie le 09/07/2025.
--   · De 1 à 212 contrôles par an et par branche. En 2025 : A 4, F 40, M 46,
--     V 25, soit 115.
--   · Trois années aberrantes dans DC_DATE_CONTROLE : 202, 203 et 2055 (des
--     erreurs de saisie de la date).
--   Le détail année par année n'a pas été recopié dans le lot 1 : la requête
--   le donne.


-- -----------------------------------------------------------------------------
-- Requête 2. Combien de contrôles pour la norme de 5 % en 2025
-- Dossier payé en 2025 : au moins un débours dont le mois payé
-- (DEB_DATE_EFFET) tombe en 2025, compté par branche puis additionné, comme
-- P13.
-- -----------------------------------------------------------------------------
SELECT SUM(dossiers_payes)                                        AS dossiers_payes_2025,
       ROUND(0.05 * SUM(dossiers_payes))                          AS controles_pour_5_pct
FROM (SELECT CODE_BRANCHE,
             COUNT(DISTINCT DOS_CODE)                             AS dossiers_payes
      FROM USER_DWH.DEBOURS
      WHERE DEB_DATE_EFFET >= DATE '2025-01-01'
        AND DEB_DATE_EFFET <  DATE '2026-01-01'
      GROUP BY CODE_BRANCHE);

-- Résultat obtenu (P13 : A 2 134, F 100 113, M 5 227, V 47 732 dossiers payés
-- en 2025 ; le 5 % est calculé ici) :
--   Dossiers payés 2025   Contrôles pour 5 %
--   155 206               7 760
-- Lecture : il faudrait environ 7 760 contrôles en 2025 ; la table en a 1 542
--   en tout, toutes années confondues, et au plus 212 par an et par branche.
