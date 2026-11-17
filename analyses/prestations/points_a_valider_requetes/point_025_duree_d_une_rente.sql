-- =============================================================================
-- Point 25. La durée d'une rente (T26)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « en 2025, une rente est liquidée en 14 jours
--   (médiane) depuis la demande, en 64 jours depuis le certificat. Le
--   certificat manque pour 15 des 43 rentes viagères liquidées en 2025. »
-- Requête : les demandes d'accident du travail liquidées en 2025, par ligne
--   de T26, et leur durée médiane selon la date de départ retenue.
-- Reprise de : lot4_controles_t26_t27.sql, P62 (exécutée le 30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t26_requete_proposee.sql, blocs ancien_cand et
--   ancien (la réception ANCIEN qui précède la liquidation est le dépôt de la
--   rente), bloc depot et bloc liq_agg (la durée). Fiche des règles :
--   règles 24 et 29.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Demandes d'accident du travail liquidées en 2025 (première
-- notification de la ligne) : rente viagère (ARI), rentes de survivants, IJ,
-- allocation d'incapacité. Durée médiane depuis : la réception qui a ouvert
-- le dossier, la dernière réception avant la liquidation (la demande au
-- guichet), l'accident, le certificat médical de consolidation, l'avis du
-- médecin conseil, la date de demande.
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

-- Résultat obtenu (P62, exécutée le 30/10/2026) :
--   Liquidées en 2025 : rente viagère 43, survivants 13, IJ 226, allocation
--   d'incapacité 61.
--   Durée médiane selon le départ      Rente   Survivants   IJ      Allocation
--   réception d'ouverture               986 j   619 j        251 j   444 j
--   dernière réception avant           14 j    35 j         183 j    13 j
--     (plus de 45 j)                    12      6            193      16
--   accident                          1 535 j  857 j        294 j   560 j
--   certificat médical, rempli pour     28      1            41       52
--     durée depuis le certificat        64 j   595 j         98 j   107 j
--   avis du médecin conseil, rempli     29      1            40       51
--     durée depuis l'avis               12 j   125 j         14 j    15 j
--   date de demande                    803 j   562 j        156 j   283 j
-- Lecture : la rente viagère est liquidée 14 jours (médiane) après la demande
--   au guichet, 64 jours après le certificat de consolidation. Le certificat
--   n'est rempli que pour 28 des 43 rentes : il manque pour 15 (calculé ici).
