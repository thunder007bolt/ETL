-- =============================================================================
-- Point 14. L'année de chaque étape
-- Fiche : points_a_valider_v2.md, rubrique « À trancher en priorité ».
-- Ce que dit le point : « un dossier déposé le 20/11/2024 et notifié le
--   05/02/2025 est déposé en 2024 et liquidé en 2025. [...] un dossier déposé
--   en décembre et liquidé en janvier ne serait liquidé dans aucun tableau. »
--   Le point ne donne pas de chiffre : la requête mesure combien de dossiers
--   changent d'année entre le dépôt et la liquidation.
-- Requête : par branche et par année de dépôt (2019 à 2024), les réceptions
--   qui ont ouvert un dossier, selon l'année de leur liquidation.
-- Nouvelle requête : aucun lot ne l'avait mesuré.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t25_requete_proposee.sql, t26_requete_proposee.sql
--   et t27_requete_proposee.sql, blocs depose_agg (année du dépôt), liq_agg
--   (année de la liquidation), rejet_agg (année du rejet) et instance_fin
--   (stock au 31/12). Fiche des règles : règle 19.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Dépôt = réception au guichet (RECEPTION_DOSSIER, RD_DATE_RECEPTION) qui a
-- ouvert un dossier (DOSSIER.RD_ID). Liquidation = première notification des
-- droits du dossier (PRESTATION_ESP, PE_DATE_NOTIFICATION). Branche : V
-- pensions, A accidents du travail, F prestations familiales, M maternité.
-- Une réception qui a ouvert deux dossiers compte deux fois (11 cas en
-- pensions, P52).
-- -----------------------------------------------------------------------------
WITH liquidation AS (
    SELECT DOS_CODE,
           TRUNC(MIN(PE_DATE_NOTIFICATION))                       AS date_liquidation
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
depot AS (
    SELECT TO_CHAR(r.TDOS_CODE)                                   AS branche,
           EXTRACT(YEAR FROM r.RD_DATE_RECEPTION)                 AS annee_depot,
           EXTRACT(YEAR FROM l.date_liquidation)                  AS annee_liquidation
    FROM USER_DWH.RECEPTION_DOSSIER r
    JOIN USER_DWH.DOSSIER           d ON d.RD_ID    = r.RD_ID
    LEFT JOIN liquidation           l ON l.DOS_CODE = d.DOS_CODE
    WHERE r.RD_DATE_RECEPTION >= DATE '2019-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2025-01-01'
)
SELECT branche,
       annee_depot,
       COUNT(*)                                                   AS deposes_avec_dossier,
       SUM(CASE WHEN annee_liquidation = annee_depot
                THEN 1 ELSE 0 END)                                AS liquides_meme_annee,
       SUM(CASE WHEN annee_liquidation = annee_depot + 1
                THEN 1 ELSE 0 END)                                AS liquides_annee_suivante,
       SUM(CASE WHEN annee_liquidation > annee_depot + 1
                THEN 1 ELSE 0 END)                                AS liquides_plus_tard,
       SUM(CASE WHEN annee_liquidation < annee_depot
                THEN 1 ELSE 0 END)                                AS liquides_avant_le_depot,
       SUM(CASE WHEN annee_liquidation IS NULL
                THEN 1 ELSE 0 END)                                AS pas_encore_liquides
FROM depot
GROUP BY branche, annee_depot
ORDER BY branche, annee_depot;

-- Résultat : pas encore exécutée.
-- Lecture attendue :
--   · « liquidés l'année suivante » : les dossiers qui comptent dans deux
--     tableaux annuels, déposés dans le premier, liquidés dans le second.
--     Avec la lecture « seulement les dossiers déposés dans l'année », ils ne
--     seraient liquidés dans aucun tableau.
--   · « liquidés avant le dépôt » : une notification antérieure à la
--     réception (un dossier repris, ou une réception saisie après coup).
