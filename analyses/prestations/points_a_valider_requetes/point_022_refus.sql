-- =============================================================================
-- Point 22. Les refus
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « le module des rejets (DOSSIER_REJETE), l'année du
--   rejet (DOSRE_DATE_REJET). Ni le statut de la réception, ni la fermeture du
--   dossier ne valent refus. » ; « 14 rejets de pension en 2025 pour 4 671
--   réceptions ; 21 en prestations familiales en 2025 pour 14 306
--   réceptions. »
-- Requêtes :
--   1. Par année et par branche : réceptions, dossiers ouverts, rejets.
--   2. Le statut des réceptions de pension (RD_STATUT) face au dossier et au
--      rejet.
--   3. Les dossiers de pension fermés sans aucune notification.
-- Reprise de : lot1_inventaire_prestations.sql, P09 (exécutée le
--   26/10/2026) ; lot3b_controles_t25.sql, P55 et P57 (exécutées le
--   30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 1
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t25_requete_proposee.sql, t26_requete_proposee.sql
--   et t27_requete_proposee.sql, blocs rej et rejet_agg (un refus est une
--   ligne de DOSSIER_REJETE, comptée l'année de DOSRE_DATE_REJET) et bloc
--   instance_fin (un refus non saisi reste en instance). Fiche des règles :
--   règle 22.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Dossiers par année et par branche, à chaque étape : réceptions
-- (RECEPTION_DOSSIER), dossiers ouverts (DOSSIER), avec un débours, rejets
-- (DOSSIER_REJETE, année de DOSRE_DATE_REJET), notifiés.
-- Branches : V pensions, A accidents du travail, F prestations familiales,
-- M maternité.
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

-- Résultat obtenu (P09, exécutée le 26/10/2026), l'année 2025 :
--   Branche   Reçus   Ouverts   Avec débours   Rejetés   Notifiés
--   A         1 481   1 237       413             1         357
--   F         9 968   9 205     5 591            19      16 075
--   M         4 338   4 238     4 134             2       4 265
--   V         4 671   3 638     3 030            14       4 783
--   Rejets enregistrés : de 0 à 62 par an et par branche.
-- Lecture (calculé ici) : 14 rejets de pension pour 4 671 réceptions ; en
--   prestations familiales avec la maternité, 19 + 2 = 21 rejets pour
--   9 968 + 4 338 = 14 306 réceptions.


-- -----------------------------------------------------------------------------
-- Requête 2. Le statut des réceptions de pension (RD_STATUT) : réceptions, en
-- 2025, avec un rejet, sans dossier ni rejet, au dossier notifié, au dossier
-- fermé sans notification, au dossier actif sans notification.
-- -----------------------------------------------------------------------------
WITH dos AS (
    SELECT d.RD_ID,
           MAX(CASE WHEN pe.PE_DATE_NOTIFICATION IS NOT NULL THEN 1 ELSE 0 END)  AS notifie,
           MAX(CASE WHEN d.DOS_STATUT = 'F' THEN 1 ELSE 0 END)                   AS ferme
    FROM USER_DWH.DOSSIER d
    LEFT JOIN USER_DWH.PRESTATION_ESP pe ON pe.DOS_CODE = d.DOS_CODE
    WHERE d.RD_ID IS NOT NULL
    GROUP BY d.RD_ID
),
rej AS (
    SELECT DISTINCT RD_ID
    FROM USER_DWH.DOSSIER_REJETE
    WHERE RD_ID IS NOT NULL
)
SELECT NVL(TO_CHAR(r.RD_STATUT), '(vide)')                                     AS rd_statut,
       COUNT(*)                                                                AS receptions_v,
       SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                 AND r.RD_DATE_RECEPTION <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS receptions_2025,
       SUM(CASE WHEN x.RD_ID IS NOT NULL THEN 1 ELSE 0 END)                    AS avec_rejet,
       SUM(CASE WHEN o.RD_ID IS NULL AND x.RD_ID IS NULL THEN 1 ELSE 0 END)    AS ni_dossier_ni_rejet,
       SUM(CASE WHEN o.notifie = 1 THEN 1 ELSE 0 END)                          AS dossier_notifie,
       SUM(CASE WHEN o.notifie = 0 AND o.ferme = 1 THEN 1 ELSE 0 END)          AS dossier_ferme_sans_notif,
       SUM(CASE WHEN o.notifie = 0 AND o.ferme = 0 THEN 1 ELSE 0 END)          AS dossier_actif_sans_notif,
       MIN(r.RD_DATE_RECEPTION)                                                AS reception_min,
       MAX(r.RD_DATE_RECEPTION)                                                AS reception_max
FROM USER_DWH.RECEPTION_DOSSIER r
LEFT JOIN dos o ON o.RD_ID = r.RD_ID
LEFT JOIN rej x ON x.RD_ID = r.RD_ID
WHERE r.TDOS_CODE = 'V'
GROUP BY NVL(TO_CHAR(r.RD_STATUT), '(vide)')
ORDER BY receptions_v DESC;

-- Résultat obtenu (P55, exécutée le 30/10/2026) :
--   RD_STATUT   Réceptions   En 2025   Avec rejet   Ni dossier ni rejet   Dossier notifié   Fermé sans notif.   Actif sans notif.
--   E           55 228       4 601     141          19 579                30 922            2 754               1 953
--   A              111          70       2             109                     0                0                   0
--   I               62           0       0              58                     4                0                   0
--   R               47           0       4              42                     1                1                   0
--   L : 2 ; vide : 2. A existe depuis le 26/05/2025 ; I de 2005 à 2021 ; R
--   de 2005 à 2023.
-- Lecture : le statut R ne concerne que 47 réceptions anciennes, et 141
--   réceptions E ont un rejet : le statut de la réception ne note pas les
--   refus.


-- -----------------------------------------------------------------------------
-- Requête 3. Dossiers de pension fermés (DOS_STATUT = F) par période
-- d'ouverture : avec une notification, prestation jamais notifiée, avec une
-- radiation sans notification.
-- -----------------------------------------------------------------------------
WITH notif AS (
    SELECT DOS_CODE,
           COUNT(*)                   AS nb_prestations,
           MIN(PE_DATE_NOTIFICATION)  AS premiere_notif
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
),
rej AS (
    SELECT DISTINCT DOS_CODE
    FROM USER_DWH.DOSSIER_REJETE
    WHERE DOS_CODE IS NOT NULL
)
SELECT EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE)                                 AS annee_ouverture,
       COUNT(*)                                                                AS dossiers_v_fermes,
       SUM(CASE WHEN n.premiere_notif IS NOT NULL THEN 1 ELSE 0 END)           AS avec_notification,
       SUM(CASE WHEN n.DOS_CODE IS NULL THEN 1 ELSE 0 END)                     AS sans_prestation,
       SUM(CASE WHEN n.DOS_CODE IS NOT NULL AND n.premiere_notif IS NULL
                THEN 1 ELSE 0 END)                                             AS prestation_jamais_notifiee,
       SUM(CASE WHEN n.premiere_notif IS NULL AND x.DOS_CODE IS NOT NULL
                THEN 1 ELSE 0 END)                                             AS sans_notif_avec_rejet,
       SUM(CASE WHEN n.premiere_notif IS NULL AND d.DOS_DATE_RADIATION IS NOT NULL
                THEN 1 ELSE 0 END)                                             AS sans_notif_avec_radiation
FROM USER_DWH.DOSSIER d
LEFT JOIN notif n ON n.DOS_CODE = d.DOS_CODE
LEFT JOIN rej   x ON x.DOS_CODE = d.DOS_CODE
WHERE d.TDOS_CODE  = 'V'
  AND d.DOS_STATUT = 'F'
GROUP BY EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE)
ORDER BY 1;

-- Résultat obtenu (P57, exécutée le 30/10/2026 ; détail dans
-- resultats/lot3b_P57_dossiers_fermes_sans_notification.tsv) :
--   Ouverture      Fermés   Avec notification   Jamais notifiée   Radiation sans notification
--   avant 2010     6 145    3 107               2 968             3 024
--   2010 à 2017    5 738      662               5 060             5 076
--   2018 à 2025    2 068    1 991                  65                65
--   13 951 dossiers fermés en tout ; 98 sans prestation ; 7 avec un rejet.
-- Lecture : un dossier fermé sans notification n'est pas un refus : il a
--   presque toujours une date de radiation. Ce sont des pensions servies puis
--   arrêtées, dont la notification n'a pas été saisie.
