-- =============================================================================
-- Point 21. Les demandes qui traînent
-- Fiche : points_a_valider_v2.md, rubrique « À trancher en priorité ».
-- Ce que dit le point : « fin 2025, sans rien écarter, 23 218 demandes de
--   pension seraient en instance. Avec les règles actuelles, leur taux restant
--   à traiter serait encore d'environ 70 % avec la formule du recueil
--   (estimation), pour une norme de 15 %. »
-- Requêtes :
--   1. Les demandes de pension en instance au 31/12/2025, sans les exclusions
--      d'aujourd'hui, par année de réception.
--   2. L'estimation avec les règles d'aujourd'hui, et le taux restant à
--      traiter avec la formule du recueil.
-- Reprise de : lot3b_controles_t25.sql, P50 (exécutée le 30/10/2026). La
--   requête 2 refait en SQL l'estimation tirée de P50 le 08/11/2026.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans :
--   · t25_requete_proposee.sql : bloc parametres (debut_stock = 01/01/2016)
--     et bloc instance_fin (on écarte les dossiers fermés jamais notifiés et
--     les réceptions d'avant debut_stock jamais reliées à un dossier) ;
--   · t26_requete_proposee.sql et t27_requete_proposee.sql : bloc parametres
--     (sans_suite_mois = 24).
--   Fiche des règles : règles 23, 26 et 32.
-- À noter :
--   · « Sans rien écarter » veut dire sans ces exclusions. P50 écarte déjà
--     les dossiers de coordination (COOR_AC, CNSS_AC), que la règle
--     d'aujourd'hui garde (point 19) : quelques demandes de plus seraient en
--     instance.
--   · t25_requete_proposee.sql calcule encore le taux avec l'ancienne formule
--     (en instance au 31/12 ÷ (en instance au 1er janvier + déposés)), pas
--     avec celle du recueil (÷ reçus dans l'année). Sa colonne
--     TAUX_RESTANT_A_TRAITER ne donnera donc pas les 70 % : le réglage
--     formule_taux reste à créer (regles_t25_t27_dossiers.md, N4, Q19).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. En instance au 31/12/2025 : reçue au guichet, ni liquidée (pas de
-- notification avant 2026) ni rejetée (pas de rejet avant 2026), hors
-- coordination. Colonnes : où en est la demande.
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

-- Résultat obtenu (P50, exécutée le 30/10/2026) :
--   Année de       En        Sans      dont visant    Dossier sans  Prestation     Dossier
--   réception      instance  dossier   un dossier     prestation    pas notifiée   fermé
--   2015 et avant  14 643    13 149        1             18          1 476          1 465
--   2016 à 2020     3 646     2 328      991             18          1 300          1 268
--   2021              832       811      633              4             17              5
--   2022              816       786      601              7             23              3
--   2023              772       734      524              8             30              2
--   2024              953       848      589             14             91              4
--   2025            1 556       914      614             43            599              4
--   Total (calculé) 23 218   19 570    3 953            112          3 536          2 751
--   RD_STATUT = E pour 99 % des demandes. La requête ne sort pas de ligne
--   Total : elle est faite ici en additionnant les lignes.
-- Lecture : 23 218 demandes, cinq fois les 4 671 réceptions de 2025, dont 79 %
--   reçues avant 2021.


-- -----------------------------------------------------------------------------
-- Requête 2. L'estimation avec les règles d'aujourd'hui (mêmes blocs liq et
-- instance que la requête 1). Restent en instance :
--   · les réceptions depuis 2016 sans dossier et qui ne visent aucun dossier ;
--   · les demandes dont le dossier est ouvert (pas fermé) et pas notifié.
-- Hypothèse de l'estimation : les réceptions qui visent un dossier existant
-- (3 952 depuis 2016) sont rattachées à une demande déjà liquidée de ce
-- dossier. La requête t25 proposée, une fois lancée, donnera le chiffre exact.
-- Taux, formule du recueil : en instance au 31/12 ÷ reçus dans l'année × 100.
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
           r.DOS_CODE                             AS dossier_vise,
           l.DOS_CODE,
           l.DOS_STATUT
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
),
reste AS (
    SELECT SUM(CASE WHEN DOS_CODE IS NULL AND dossier_vise IS NULL
                     AND annee_reception BETWEEN 2016 AND 2020
                    THEN 1 ELSE 0 END)                            AS sans_dossier_2016_2020,
           SUM(CASE WHEN DOS_CODE IS NULL AND dossier_vise IS NULL
                     AND annee_reception >= 2021
                    THEN 1 ELSE 0 END)                            AS sans_dossier_2021_2025,
           SUM(CASE WHEN DOS_CODE IS NOT NULL AND NVL(DOS_STATUT, '-') <> 'F'
                    THEN 1 ELSE 0 END)                            AS dossier_ouvert_pas_notifie
    FROM instance
),
recus AS (
    SELECT COUNT(*)                                               AS recus_2025
    FROM USER_DWH.RECEPTION_DOSSIER
    WHERE TDOS_CODE = 'V'
      AND RD_DATE_RECEPTION >= DATE '2025-01-01'
      AND RD_DATE_RECEPTION <  DATE '2026-01-01'
)
SELECT r.sans_dossier_2016_2020,
       r.sans_dossier_2021_2025,
       r.dossier_ouvert_pas_notifie,
       r.sans_dossier_2016_2020 + r.sans_dossier_2021_2025
         + r.dossier_ouvert_pas_notifie                           AS en_instance_estime,
       c.recus_2025,
       ROUND(100 * (r.sans_dossier_2016_2020 + r.sans_dossier_2021_2025
                    + r.dossier_ouvert_pas_notifie) / c.recus_2025, 1)
                                                                  AS taux_restant_pct
FROM reste r
CROSS JOIN recus c;

-- Résultat : pas encore exécutée sous cette forme. Attendu, calculé depuis le
-- résultat de P50 (requête 1) et les 4 671 réceptions de 2025 (lot 1, P09) :
--   Sans dossier ni lien    Sans dossier ni lien    Dossier ouvert    En instance    Reçus    Taux
--   2016 à 2020             2021 à 2025             pas notifié       (estimation)   2025
--   1 337                   1 132                   897               3 366          4 671    72,1 %
--   Calcul : 2 328 − 991 = 1 337 ; (811 − 633) + (786 − 601) + (734 − 524)
--   + (848 − 589) + (914 − 614) = 1 132 ; 112 + 3 536 − 2 751 = 897.
-- Lecture : avec les règles d'aujourd'hui, environ 3 300 à 3 500 demandes
--   restent en instance fin 2025, soit un taux d'environ 70 % (formule du
--   recueil), pour une norme de 15 %.
