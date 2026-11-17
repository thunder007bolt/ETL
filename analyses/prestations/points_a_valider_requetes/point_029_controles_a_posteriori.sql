-- =============================================================================
-- Point 29. Les contrôles a posteriori
-- Fiche : points_a_valider_v2.md, rubrique « À trancher en priorité ».
-- Ce que dit le point : « la table DOSSIERS_CONTROLES n'a que 1 542 lignes en
--   tout, la dernière saisie le 09/07/2025. » ; « en 2025, 115 contrôles
--   notés (jusqu'au 9 juillet) pour 155 206 dossiers qui ont reçu au moins un
--   débours (DEBOURS) dans l'année, soit 0,07 %. »
-- Requêtes :
--   1. La table des contrôles : nombre de lignes, dernière saisie, contrôles
--      de 2025.
--   2. Le taux de contrôle de 2025, par branche : contrôles ÷ dossiers payés.
-- Reprise de : lot1_inventaire_prestations.sql, P01 et P13 (exécutées le
--   26/10/2026). Mêmes comptages, limités à ce que dit le point.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 2
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : aucune requête proposée, T28 à T30 ne sont pas
--   produits. Fiche des règles : règles 39 à 41, et la ligne « À faire (DAI
--   et DSI) ».
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. La table des contrôles (DOSSIERS_CONTROLES)
-- Contrôle de 2025 : date du contrôle (DC_DATE_CONTROLE) en 2025.
-- Dernière saisie : DC_DATE_INSERT la plus récente.
-- -----------------------------------------------------------------------------
SELECT COUNT(*)                                                   AS nb_lignes,
       MAX(DC_DATE_INSERT)                                        AS derniere_saisie,
       SUM(CASE WHEN DC_DATE_CONTROLE >= DATE '2025-01-01'
                 AND DC_DATE_CONTROLE <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                                AS controles_2025
FROM USER_DWH.DOSSIERS_CONTROLES;

-- Résultat obtenu (P01 et P13) :
--   Lignes   Dernière saisie   Contrôles de 2025
--   1 542    09/07/2025        115


-- -----------------------------------------------------------------------------
-- Requête 2. Le taux de contrôle de 2025, par branche
-- Dossier payé en 2025 : au moins un débours dont le mois payé
-- (DEB_DATE_EFFET) tombe en 2025, quel que soit son type ou son statut, comme
-- P13. Branche : TDOS_CODE de DOSSIERS_CONTROLES et CODE_BRANCHE de DEBOURS
-- (A accidents du travail, F prestations familiales, M maternité, V pensions).
-- Le total additionne les branches, comme P13.
-- -----------------------------------------------------------------------------
WITH payes AS (
    SELECT TO_CHAR(CODE_BRANCHE)                                  AS branche,
           COUNT(DISTINCT DOS_CODE)                               AS dossiers_payes
    FROM USER_DWH.DEBOURS
    WHERE DEB_DATE_EFFET >= DATE '2025-01-01'
      AND DEB_DATE_EFFET <  DATE '2026-01-01'
    GROUP BY CODE_BRANCHE
),
controles AS (
    SELECT TO_CHAR(TDOS_CODE)                                     AS branche,
           COUNT(*)                                               AS controles
    FROM USER_DWH.DOSSIERS_CONTROLES
    WHERE DC_DATE_CONTROLE >= DATE '2025-01-01'
      AND DC_DATE_CONTROLE <  DATE '2026-01-01'
    GROUP BY TDOS_CODE
),
par_branche AS (
    SELECT p.branche,
           NVL(c.controles, 0)                                    AS controles_2025,
           p.dossiers_payes                                       AS dossiers_payes_2025
    FROM payes p
    LEFT JOIN controles c ON c.branche = p.branche
)
SELECT 1                                                          AS ordre,
       branche,
       controles_2025,
       dossiers_payes_2025,
       ROUND(100 * controles_2025 / dossiers_payes_2025, 2)       AS taux_controle_pct
FROM par_branche
UNION ALL
SELECT 2,
       'Total',
       SUM(controles_2025),
       SUM(dossiers_payes_2025),
       ROUND(100 * SUM(controles_2025) / SUM(dossiers_payes_2025), 2)
FROM par_branche
ORDER BY 1, 2;

-- Résultat obtenu (P13, exécutée le 26/10/2026 ; les taux sont calculés ici) :
--   Branche   Contrôles 2025   Dossiers payés 2025   Taux
--   A           4                2 134               0,19 %
--   F          40              100 113               0,04 %
--   M          46                5 227               0,88 %
--   V          25               47 732               0,05 %
--   Total     115              155 206               0,07 %
-- Lecture : 0,07 % de dossiers contrôlés en 2025, pour une norme d'au moins
--   5 %. Rien n'est saisi après le 09/07/2025.
