-- =============================================================================
-- Point 26. L'attente des indemnités journalières (T26)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « ni la reprise du travail, ni le certificat de
--   guérison ne sont saisis. » ; « en 2025, en médiane, l'indemnité est
--   notifiée 211 jours (environ 7 mois) après la fin de l'arrêt et 251 jours
--   après la déclaration. »
-- Requêtes :
--   1. Les IJ liquidées en 2025 : dates remplies, durée de l'arrêt, attente
--      après la fin de l'arrêt.
--   2. Les demandes liquidées en 2025 et leur durée selon la date de départ,
--      dont la déclaration pour les IJ.
-- Reprise de : lot4_controles_t26_t27.sql, P69 et P62 (exécutées le
--   30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t26_requete_proposee.sql, blocs demande et depot
--   (l'IJ est déposée avec la déclaration, qui ouvre le dossier ; une seule
--   demande même si l'arrêt est prolongé) et bloc liq_agg (la durée). Fiche
--   des règles : règle 30.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. IJ d'accident du travail (AIJ) liquidées en 2025, première IJ
-- notifiée du dossier : date de reprise du travail, date de traitement,
-- début et durée de l'arrêt, attente entre la fin de l'arrêt et la
-- liquidation, dernière pièce saisie, pièce de guérison ou de reprise.
-- -----------------------------------------------------------------------------
WITH ij AS (
    SELECT pe.DOS_CODE,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))                    AS date_liq,
           MIN(TRUNC(pe.PE_DATE_EFFET))
               KEEP (DENSE_RANK FIRST ORDER BY pe.PE_DATE_NOTIFICATION) AS debut_ij,
           MIN(pe.PE_DUREE_ARRET)
               KEEP (DENSE_RANK FIRST ORDER BY pe.PE_DATE_NOTIFICATION) AS duree_arret,
           MIN(TRUNC(pe.PE_DATE_TRAITEMENT))
               KEEP (DENSE_RANK FIRST ORDER BY pe.PE_DATE_NOTIFICATION) AS date_traitement
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE = 'A'
      AND pe.TPE_CODE = 'AIJ'
      AND pe.PE_DATE_NOTIFICATION IS NOT NULL
    GROUP BY pe.DOS_CODE
),
recep_dos AS (
    SELECT DOS_CODE, RD_ID
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'A'
      AND RD_ID IS NOT NULL
    UNION
    SELECT DOS_CODE, RD_ID
    FROM USER_DWH.RECEPTION_DOSSIER
    WHERE TDOS_CODE = 'A'
      AND DOS_CODE IS NOT NULL
),
pieces AS (
    SELECT x.DOS_CODE,
           TRUNC(p.RDP_DATE_INSERT)                               AS date_piece,
           CASE WHEN TRIM(p.TPJ_CODE) IN ('CMG', 'CRT', 'CMCON')
                THEN 1 ELSE 0 END                                 AS fin_arret
    FROM recep_dos x
    JOIN USER_DWH.RECEPTION_DOS_PIECE p ON p.RD_ID = x.RD_ID
),
base AS (
    SELECT ij.date_liq,
           ij.debut_ij,
           ij.duree_arret,
           ij.debut_ij + ij.duree_arret                           AS fin_arret,
           ij.date_traitement,
           TRUNC(d.DOS_DATE_REPRISE)                              AS date_reprise,
           (SELECT MAX(pc.date_piece)
            FROM pieces pc
            WHERE pc.DOS_CODE    = ij.DOS_CODE
              AND pc.date_piece <= ij.date_liq)                   AS derniere_piece,
           (SELECT MAX(pc.date_piece)
            FROM pieces pc
            WHERE pc.DOS_CODE    = ij.DOS_CODE
              AND pc.fin_arret   = 1
              AND pc.date_piece <= ij.date_liq)                   AS piece_fin_arret
    FROM ij
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = ij.DOS_CODE
    WHERE ij.date_liq >= DATE '2025-01-01'
      AND ij.date_liq <  DATE '2026-01-01'
)
SELECT COUNT(*)                                                   AS ij_liquidees_2025,
       COUNT(date_reprise)                                        AS avec_date_reprise,
       ROUND(MEDIAN(date_liq - date_reprise))                     AS med_depuis_reprise_j,
       SUM(CASE WHEN date_liq - date_reprise > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_reprise,
       SUM(CASE WHEN date_reprise > date_liq
                THEN 1 ELSE 0 END)                                AS reprise_apres_liq,
       COUNT(debut_ij)                                            AS avec_debut_ij,
       ROUND(MEDIAN(date_liq - debut_ij))                         AS med_depuis_debut_ij_j,
       COUNT(duree_arret)                                         AS avec_duree_arret,
       ROUND(MEDIAN(duree_arret))                                 AS med_duree_arret,
       ROUND(MEDIAN(date_liq - fin_arret))                        AS med_depuis_fin_arret_j,
       COUNT(date_traitement)                                     AS avec_date_traitement,
       ROUND(MEDIAN(date_liq - date_traitement))                  AS med_depuis_traitement_j,
       COUNT(derniere_piece)                                      AS avec_piece_avant_liq,
       ROUND(MEDIAN(date_liq - derniere_piece))                   AS med_depuis_derniere_piece_j,
       SUM(CASE WHEN date_liq - derniere_piece > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_derniere_piece,
       COUNT(piece_fin_arret)                                     AS avec_piece_fin_arret,
       ROUND(MEDIAN(date_liq - piece_fin_arret))                  AS med_depuis_piece_fin_j,
       SUM(CASE WHEN date_liq - piece_fin_arret > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_piece_fin
FROM base;

-- Résultat obtenu (P69, exécutée le 30/10/2026) :
--   · 226 IJ liquidées en 2025. Date de reprise du travail et date de
--     traitement : jamais remplies.
--   · Début de l'IJ : toujours rempli, 266 j avant la liquidation en médiane.
--     Durée de l'arrêt : toujours remplie, 27 j en médiane ; de la fin de
--     l'arrêt à la liquidation : 211 j.
--   · Dernière pièce saisie avant la liquidation : 225 IJ, 183 j avant en
--     médiane, plus de 45 j pour 192.
--   · Pièce de guérison, de reprise ou de consolidation : 14 IJ seulement,
--     saisie la veille de la liquidation en médiane.
-- Lecture : l'arrêt dure un mois en médiane, puis l'IJ attend 211 jours
--   (environ 7 mois) ; la reprise n'est jamais saisie et le certificat de
--   guérison ne l'est que pour 14 IJ sur 226.


-- -----------------------------------------------------------------------------
-- Requête 2. Demandes d'accident du travail liquidées en 2025 et leur durée
-- médiane selon la date de départ (la réception d'ouverture est la
-- déclaration de l'accident).
-- -----------------------------------------------------------------------------
WITH prest AS (
    SELECT pe.DOS_CODE,
           CASE WHEN pe.TPE_CODE = 'ARI'                            THEN '1 rente viagère'
                WHEN pe.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA') THEN '2 rentes de survivants'
                WHEN pe.TPE_CODE = 'AIJ'                            THEN '3 IJ'
                WHEN pe.TPE_CODE = 'AAI'                            THEN '4 allocation d''incapacité'
                ELSE                                                     '5 autres'
           END                                                    AS ligne,
           pe.PE_DATE_NOTIFICATION,
           pe.PE_DATE_DEMANDE
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE = 'A'
),
demande AS (
    SELECT DOS_CODE,
           ligne,
           TRUNC(MIN(PE_DATE_NOTIFICATION))                       AS date_liq,
           TRUNC(MIN(PE_DATE_DEMANDE))                            AS date_demande
    FROM prest
    GROUP BY DOS_CODE, ligne
),
recep AS (
    SELECT d.DOS_CODE, TRUNC(r.RD_DATE_RECEPTION) AS date_reception, 1 AS ouvre
    FROM USER_DWH.DOSSIER d
    JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID = d.RD_ID
    WHERE d.TDOS_CODE = 'A'
    UNION ALL
    SELECT r.DOS_CODE, TRUNC(r.RD_DATE_RECEPTION), 0
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE = 'A'
      AND r.DOS_CODE IS NOT NULL
),
base AS (
    SELECT dm.ligne,
           dm.date_liq,
           dm.date_demande,
           TRUNC(d.DOS_DATE_ACCIDENT)                             AS date_accident,
           TRUNC(d.DOS_DATE_REC_CM_RENTE)                         AS date_cm_rente,
           TRUNC(d.DOS_DATE_REC_AVIS_MC)                          AS date_avis_mc,
           (SELECT MIN(x.date_reception)
            FROM recep x
            WHERE x.DOS_CODE = dm.DOS_CODE
              AND x.ouvre    = 1)                                 AS date_recep_ouverture,
           (SELECT MAX(x.date_reception)
            FROM recep x
            WHERE x.DOS_CODE        = dm.DOS_CODE
              AND x.date_reception <= dm.date_liq)                AS date_recep_proche
    FROM demande dm
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = dm.DOS_CODE
    WHERE dm.date_liq >= DATE '2025-01-01'
      AND dm.date_liq <  DATE '2026-01-01'
)
SELECT ligne,
       COUNT(*)                                                   AS demandes_liquidees_2025,
       COUNT(date_recep_ouverture)                                AS avec_recep_ouverture,
       ROUND(MEDIAN(date_liq - date_recep_ouverture))             AS med_depuis_recep_ouverture,
       COUNT(date_recep_proche)                                   AS avec_recep_avant_liq,
       ROUND(MEDIAN(date_liq - date_recep_proche))                AS med_depuis_recep_proche,
       SUM(CASE WHEN date_liq - date_recep_proche > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_recep_proche,
       COUNT(date_accident)                                       AS avec_date_accident,
       ROUND(MEDIAN(date_liq - date_accident))                    AS med_depuis_accident,
       COUNT(date_cm_rente)                                       AS avec_cm_rente,
       ROUND(MEDIAN(date_liq - date_cm_rente))                    AS med_depuis_cm_rente,
       SUM(CASE WHEN date_liq - date_cm_rente > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_cm_rente,
       SUM(CASE WHEN date_cm_rente > date_liq
                THEN 1 ELSE 0 END)                                AS cm_rente_apres_liq,
       COUNT(date_avis_mc)                                        AS avec_avis_mc,
       ROUND(MEDIAN(date_liq - date_avis_mc))                     AS med_depuis_avis_mc,
       COUNT(date_demande)                                        AS avec_date_demande,
       ROUND(MEDIAN(date_liq - date_demande))                     AS med_depuis_demande
FROM base
GROUP BY ligne
ORDER BY ligne;

-- Résultat obtenu (P62, exécutée le 30/10/2026), la colonne des IJ :
--   226 IJ liquidées en 2025 ; durée médiane depuis la réception d'ouverture,
--   la déclaration : 251 j ; depuis la dernière réception : 183 j ; depuis
--   l'accident : 294 j ; depuis la date de demande : 156 j.
-- Lecture : comptée depuis la déclaration, la durée d'une IJ est de 251 jours
--   en médiane ; elle est publiée telle quelle.
