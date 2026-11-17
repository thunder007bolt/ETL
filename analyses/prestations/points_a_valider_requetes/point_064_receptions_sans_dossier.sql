-- =============================================================================
-- Point 64. Les réceptions sans dossier
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « des réceptions (RECEPTION_DOSSIER,
--   RD_TYPE_RECEPTION) ne sont reliées à aucun dossier (DOSSIER). » ; « en
--   pensions en 2025, 4 671 réceptions pour 3 638 dossiers ouverts. Fin 2025,
--   environ 1 132 réceptions de pension de 2021 à 2025 restent sans
--   dossier. »
-- Requêtes :
--   1. Par année et par branche : réceptions, dossiers ouverts, rejets.
--   2. Les demandes de pension en instance au 31/12/2025, par année de
--      réception, dont celles sans dossier.
-- Reprise de : lot1_inventaire_prestations.sql, P09 (exécutée le
--   26/10/2026) ; lot3b_controles_t25.sql, P50 (exécutée le 30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 1
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t25_requete_proposee.sql, blocs parametres
--   (debut_stock) et instance_fin ; t26_requete_proposee.sql et
--   t27_requete_proposee.sql, bloc parametres (sans_suite_mois = 24). Fiche
--   des règles : règles 23 et 32.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Dossiers par année et par branche, à chaque étape : réceptions,
-- dossiers ouverts, avec un débours, rejets, notifiés.
-- -----------------------------------------------------------------------------
SELECT annee, branche,
       SUM(CASE WHEN etape = 'RECUS'      THEN nb END) AS recus,
       SUM(CASE WHEN etape = 'OUVERTS'    THEN nb END) AS ouverts,
       SUM(CASE WHEN etape = 'AVEC_DEB'   THEN nb END) AS ouverts_avec_debours,
       SUM(CASE WHEN etape = 'REJETES'    THEN nb END) AS rejetes,
       SUM(CASE WHEN etape = 'REJ_SANS_D' THEN nb END) AS rejetes_sans_dossier,
       SUM(CASE WHEN etape = 'NOTIFIES'   THEN nb END) AS notifies
FROM (
    SELECT 'RECUS' AS etape, EXTRACT(YEAR FROM RD_DATE_RECEPTION) AS annee,
           TO_CHAR(TDOS_CODE) AS branche, COUNT(*) AS nb
    FROM USER_DWH.RECEPTION_DOSSIER
    WHERE RD_DATE_RECEPTION >= DATE '2019-01-01' AND RD_DATE_RECEPTION < DATE '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM RD_DATE_RECEPTION), TDOS_CODE
    UNION ALL
    SELECT 'OUVERTS', EXTRACT(YEAR FROM DOS_DATE_OUVERTURE), TO_CHAR(TDOS_CODE), COUNT(*)
    FROM USER_DWH.DOSSIER
    WHERE DOS_DATE_OUVERTURE >= DATE '2019-01-01' AND DOS_DATE_OUVERTURE < DATE '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM DOS_DATE_OUVERTURE), TDOS_CODE
    UNION ALL
    SELECT 'AVEC_DEB', EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE), TO_CHAR(d.TDOS_CODE), COUNT(*)
    FROM USER_DWH.DOSSIER d
    WHERE d.DOS_DATE_OUVERTURE >= DATE '2019-01-01' AND d.DOS_DATE_OUVERTURE < DATE '2026-01-01'
      AND d.DOS_CODE IN (SELECT DOS_CODE FROM USER_DWH.DEBOURS)
    GROUP BY EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE), d.TDOS_CODE
    UNION ALL
    SELECT 'REJETES', EXTRACT(YEAR FROM DOSRE_DATE_REJET), TO_CHAR(TDOS_CODE), COUNT(*)
    FROM USER_DWH.DOSSIER_REJETE
    WHERE DOSRE_DATE_REJET >= DATE '2019-01-01' AND DOSRE_DATE_REJET < DATE '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM DOSRE_DATE_REJET), TDOS_CODE
    UNION ALL
    SELECT 'REJ_SANS_D', EXTRACT(YEAR FROM DOSRE_DATE_REJET), TO_CHAR(TDOS_CODE), COUNT(*)
    FROM USER_DWH.DOSSIER_REJETE
    WHERE DOSRE_DATE_REJET >= DATE '2019-01-01' AND DOSRE_DATE_REJET < DATE '2026-01-01'
      AND DOS_CODE IS NULL
    GROUP BY EXTRACT(YEAR FROM DOSRE_DATE_REJET), TDOS_CODE
    UNION ALL
    SELECT 'NOTIFIES', EXTRACT(YEAR FROM n.dt_notif), TO_CHAR(d.TDOS_CODE), COUNT(*)
    FROM (SELECT DOS_CODE, MAX(PE_DATE_NOTIFICATION) AS dt_notif
          FROM USER_DWH.PRESTATION_ESP GROUP BY DOS_CODE) n
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = n.DOS_CODE
    WHERE n.dt_notif >= DATE '2019-01-01' AND n.dt_notif < DATE '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM n.dt_notif), d.TDOS_CODE
)
GROUP BY annee, branche
ORDER BY branche, annee;

-- Résultat obtenu (P09, exécutée le 26/10/2026) : pensions (V) en 2025,
-- 4 671 réceptions et 3 638 dossiers ouverts ; 1 033 réceptions de 2025 sans
-- dossier ni rejet.


-- -----------------------------------------------------------------------------
-- Requête 2. Demandes de pension en instance au 31/12/2025, par année de
-- réception : sans dossier, dont visant un dossier existant.
-- -----------------------------------------------------------------------------
WITH liq AS (
    SELECT d.DOS_CODE, d.RD_ID, d.DOS_STATUT, d.DOS_CATEGORIE,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))  AS date_liquidation,
           COUNT(pe.PE_ID)                      AS nb_prestations
    FROM USER_DWH.DOSSIER d
    LEFT JOIN USER_DWH.PRESTATION_ESP pe ON pe.DOS_CODE = d.DOS_CODE
    WHERE d.TDOS_CODE = 'V'
    GROUP BY d.DOS_CODE, d.RD_ID, d.DOS_STATUT, d.DOS_CATEGORIE
),
instance AS (
    SELECT EXTRACT(YEAR FROM r.RD_DATE_RECEPTION) AS annee_reception,
           r.RD_STATUT,
           r.DOS_CODE                             AS dossier_vise,
           l.DOS_CODE,
           l.DOS_STATUT,
           l.nb_prestations
    FROM USER_DWH.RECEPTION_DOSSIER r
    LEFT JOIN liq l ON l.RD_ID = r.RD_ID
    WHERE r.TDOS_CODE = 'V'
      AND r.RD_DATE_RECEPTION < DATE '2026-01-01'
      AND NVL(l.DOS_CATEGORIE, 'ORDINAIRE') NOT IN ('COOR_AC', 'CNSS_AC')
      AND (l.date_liquidation IS NULL OR l.date_liquidation >= DATE '2026-01-01')
      AND NOT EXISTS (SELECT 1
                      FROM   USER_DWH.DOSSIER_REJETE x
                      WHERE  x.RD_ID = r.RD_ID
                        AND  x.DOSRE_DATE_REJET < DATE '2026-01-01')
)
SELECT CASE WHEN annee_reception <= 2015 THEN '2015 et avant'
            WHEN annee_reception <= 2020 THEN '2016 à 2020'
            ELSE TO_CHAR(annee_reception) END                                  AS annee_de_reception,
       COUNT(*)                                                                AS en_instance_31_12_2025,
       SUM(CASE WHEN DOS_CODE IS NULL THEN 1 ELSE 0 END)                       AS sans_dossier,
       SUM(CASE WHEN DOS_CODE IS NULL AND dossier_vise IS NOT NULL
                THEN 1 ELSE 0 END)                                             AS sans_dossier_vise_un_dossier,
       SUM(CASE WHEN DOS_CODE IS NOT NULL AND nb_prestations = 0
                THEN 1 ELSE 0 END)                                             AS dossier_sans_prestation,
       SUM(CASE WHEN DOS_CODE IS NOT NULL AND nb_prestations > 0
                THEN 1 ELSE 0 END)                                             AS prestation_pas_notifiee,
       SUM(CASE WHEN DOS_STATUT = 'F' THEN 1 ELSE 0 END)                       AS dossier_ferme,
       SUM(CASE WHEN RD_STATUT = 'E' THEN 1 ELSE 0 END)                        AS rd_statut_e,
       SUM(CASE WHEN NVL(RD_STATUT, '-') <> 'E' THEN 1 ELSE 0 END)             AS rd_statut_autre
FROM instance
GROUP BY CASE WHEN annee_reception <= 2015 THEN '2015 et avant'
              WHEN annee_reception <= 2020 THEN '2016 à 2020'
              ELSE TO_CHAR(annee_reception) END
ORDER BY 1;

-- Résultat obtenu (P50, exécutée le 30/10/2026) : réceptions sans dossier en
-- instance fin 2025 (sans dossier / dont visant un dossier) : 2021 811 / 633 ;
-- 2022 786 / 601 ; 2023 734 / 524 ; 2024 848 / 589 ; 2025 914 / 614.
-- Lecture (calculé ici) : sans dossier et sans lien vers un dossier, de 2021
--   à 2025 : 178 + 185 + 210 + 259 + 300 = 1 132 réceptions.
