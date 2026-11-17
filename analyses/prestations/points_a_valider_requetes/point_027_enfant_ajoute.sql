-- =============================================================================
-- Point 27. L'enfant ajouté (T27)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « une allocation familiale de plus (code FAF,
--   PRESTATION_ESP) sur le dossier, sans nouvelle réception au guichet
--   (RECEPTION_DOSSIER). » ; « sans les enfants ajoutés, 8 246 allocations
--   familiales liquidées en 2025 ; avec eux, environ 16 400. »
-- Requêtes :
--   1. Les dossiers de prestations familiales par année : à leur première
--      notification, ou renotifiés pour un enfant de plus.
--   2. Les réceptions des prestations familiales et de la maternité, par type.
-- Reprise de : lot4_controles_t26_t27.sql, P64 et P65 (exécutées le
--   30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t27_requete_proposee.sql, blocs prest, initiale et
--   demande (chaque enfant ajouté est une demande, déposée à la date de
--   demande de son allocation) et bloc depot. Fiche des règles : règle 34.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Dossiers de prestations familiales (TDOS_CODE = F) par année :
-- dossiers ouverts, prestations notifiées, à la première notification du
-- dossier ou après, dossiers à leur première notification, dossiers
-- renotifiés, écart médian avec la première notification.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2018 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 7
),
pf AS (
    SELECT pe.DOS_CODE,
           pe.TPE_CODE,
           TRUNC(pe.PE_DATE_NOTIFICATION)                         AS date_notif,
           EXTRACT(YEAR FROM pe.PE_DATE_NOTIFICATION)             AS annee_notif,
           MIN(TRUNC(pe.PE_DATE_NOTIFICATION))
               OVER (PARTITION BY pe.DOS_CODE)                    AS premiere_notif
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE = 'F'
),
ouverts AS (
    SELECT EXTRACT(YEAR FROM DOS_DATE_OUVERTURE)                  AS annee,
           COUNT(*)                                               AS nb
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'F'
    GROUP BY EXTRACT(YEAR FROM DOS_DATE_OUVERTURE)
)
SELECT a.annee,
       MAX(o.nb)                                                  AS dossiers_ouverts,
       COUNT(p.DOS_CODE)                                          AS prestations_notifiees,
       SUM(CASE WHEN p.TPE_CODE = 'FAF' THEN 1 ELSE 0 END)        AS dont_faf,
       SUM(CASE WHEN p.TPE_CODE = 'FAP' THEN 1 ELSE 0 END)        AS dont_fap,
       SUM(CASE WHEN p.date_notif = p.premiere_notif
                THEN 1 ELSE 0 END)                                AS avec_1re_notif_du_dossier,
       SUM(CASE WHEN p.date_notif > p.premiere_notif
                THEN 1 ELSE 0 END)                                AS apres_1re_notif_du_dossier,
       COUNT(DISTINCT CASE WHEN p.date_notif = p.premiere_notif
                           THEN p.DOS_CODE END)                   AS dossiers_1re_notif,
       COUNT(DISTINCT CASE WHEN p.date_notif > p.premiere_notif
                           THEN p.DOS_CODE END)                   AS dossiers_renotifies,
       ROUND(MEDIAN(CASE WHEN p.date_notif > p.premiere_notif
                         THEN p.date_notif - p.premiere_notif END)) AS med_ecart_1re_notif_j
FROM annees a
LEFT JOIN pf      p ON p.annee_notif = a.annee
LEFT JOIN ouverts o ON o.annee       = a.annee
GROUP BY a.annee
ORDER BY a.annee;

-- Résultat obtenu (P64, exécutée le 30/10/2026) :
--   Année   Ouverts   Notifiées   À la 1re notif.   Après    Dossiers à leur 1re notif.   Renotifiés   Écart médian
--   2019    9 447     31 667      21 267            10 400   9 992                        9 595        1 785 j
--   2020    8 261     26 852      17 462             9 390   8 505                        8 693        1 759 j
--   2021    8 876     29 579      19 153            10 426   9 179                        9 637        1 706 j
--   2022    8 527     27 438      17 595             9 843   8 617                        9 082        1 868 j
--   2023    8 958     28 423      18 154            10 269   9 049                        9 479        1 959 j
--   2024    9 682     29 415      19 587             9 828   9 653                        8 980        1 980 j
--   2025    9 205     25 786      16 806             8 980   8 246                        8 124        2 098 j
--   Presque toutes sont des FAF (FAP : 33 en 2025).
-- Lecture (calculé ici) : en 2025, 8 246 dossiers à leur première
--   notification, les nouveaux allocataires, et 8 124 renotifiés pour un
--   enfant de plus, en médiane 5 ans après : 8 246 + 8 124 = 16 370,
--   « environ 16 400 ».


-- -----------------------------------------------------------------------------
-- Requête 2. Réceptions de 2019 à 2025 des prestations familiales et de la
-- maternité : type (NOUVEAU, ANCIEN) et pièces fournies.
-- -----------------------------------------------------------------------------
WITH recep0 AS (
    SELECT r.RD_ID,
           r.TDOS_CODE,
           r.DOS_CODE,
           NVL(r.RD_TYPE_RECEPTION, '(vide)')                     AS type_reception,
           EXTRACT(YEAR FROM r.RD_DATE_RECEPTION)                 AS annee
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE IN ('F', 'M')
      AND r.RD_DATE_RECEPTION >= DATE '2019-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
),
pieces AS (
    SELECT p.RD_ID,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) = 'CAC'
                    THEN 1 ELSE 0 END)                            AS accouchement,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('AC', 'CAT', 'CMAT')
                    THEN 1 ELSE 0 END)                            AS conge,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CEP1', 'CEP2', 'CEP3', 'CG')
                    THEN 1 ELSE 0 END)                            AS prenatal,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('AN', 'ANENFANT')
                    THEN 1 ELSE 0 END)                            AS naissance,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CS', 'CV', 'CVENFANT')
                    THEN 1 ELSE 0 END)                            AS scolarite_vie
    FROM USER_DWH.RECEPTION_DOS_PIECE p
    JOIN recep0 r ON r.RD_ID = p.RD_ID
    GROUP BY p.RD_ID
),
ouvre AS (
    SELECT DISTINCT o.RD_ID
    FROM USER_DWH.DOSSIER o
    JOIN recep0 r ON r.RD_ID = o.RD_ID
),
recep AS (
    SELECT r.TDOS_CODE,
           r.type_reception,
           r.annee,
           CASE WHEN p.RD_ID IS NULL         THEN '7 sans pièce'
                WHEN p.accouchement  = 1     THEN '1 accouchement'
                WHEN p.conge         = 1     THEN '2 congé ou arrêt'
                WHEN p.prenatal      = 1     THEN '3 examens prénataux'
                WHEN p.naissance     = 1     THEN '4 acte de naissance'
                WHEN p.scolarite_vie = 1     THEN '5 scolarité ou vie'
                ELSE                              '6 autres pièces'
           END                                                    AS motif,
           CASE WHEN o.RD_ID IS NOT NULL THEN 1 ELSE 0 END        AS ouvre_dossier,
           CASE WHEN r.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END     AS vise_dossier
    FROM recep0 r
    LEFT JOIN pieces p ON p.RD_ID = r.RD_ID
    LEFT JOIN ouvre  o ON o.RD_ID = r.RD_ID
)
SELECT TDOS_CODE,
       type_reception,
       motif,
       COUNT(*)                                                   AS receptions_2019_2025,
       SUM(CASE WHEN annee = 2025 THEN 1 ELSE 0 END)              AS receptions_2025,
       SUM(ouvre_dossier)                                         AS ouvrent_un_dossier,
       SUM(vise_dossier)                                          AS visent_un_dossier
FROM recep
GROUP BY TDOS_CODE, type_reception, motif
ORDER BY TDOS_CODE, type_reception, motif;

-- Résultat obtenu (P65, exécutée le 30/10/2026) :
--   · Prestations familiales : NOUVEAU 65 731, dont 65 204 avec un acte de
--     naissance (9 930 en 2025) ; ANCIEN 300 seulement (6 en 2025) ; type
--     vide 62.
--   · Maternité : 27 381 réceptions, toutes NOUVEAU sauf 18 vides.
-- Lecture : en 2025, 6 réceptions ANCIEN pour 8 980 enfants ajoutés (P64) :
--   l'enfant ajouté ne passe pas par le guichet ; sa date de dépôt est la
--   date de demande de son allocation.
