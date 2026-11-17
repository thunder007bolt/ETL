-- =============================================================================
-- Point 28. La grossesse sur deux lignes (T27)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « une prestation MIJ et une prestation MAP
--   (PRESTATION_ESP) dans le même dossier de maternité (TDOS_CODE = M) :
--   99,4 % des dossiers de maternité portent les deux. » ; « 4 273
--   indemnités de maternité et 4 245 allocations prénatales liquidées en
--   2025 : sur une seule ligne, le Total de T27 baisserait d'environ
--   4 200. »
-- Requêtes :
--   1. Les dossiers de maternité par année : un par grossesse, avec l'IJ et
--      l'allocation prénatale.
--   2. Les prestations des PF et de la maternité par code, dont celles
--      notifiées en 2025.
-- Reprise de : lot4_controles_t26_t27.sql, P67 et P66 (exécutées le
--   30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t27_requete_proposee.sql, bloc prest (MIJ au rang 2,
--   MAP et FAP au rang 3 : une grossesse fait deux demandes) et bloc demande.
--   Fiche des règles : règle 35.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Dossiers de maternité (TDOS_CODE = M) par année d'ouverture :
-- dossiers, mères, IJ et prénatale dans le même dossier, délai de la
-- réception à la notification, réception avant ou après la date probable
-- d'accouchement.
-- -----------------------------------------------------------------------------
WITH dosm AS (
    SELECT d.DOS_CODE,
           d.IND_ID,
           d.RD_ID,
           EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE)                AS annee,
           TRUNC(d.DOS_DATE_GROSSESSE)                            AS date_grossesse
    FROM USER_DWH.DOSSIER d
    WHERE d.TDOS_CODE = 'M'
      AND d.DOS_DATE_OUVERTURE >= DATE '2019-01-01'
      AND d.DOS_DATE_OUVERTURE <  DATE '2026-01-01'
),
prest AS (
    SELECT pe.DOS_CODE,
           MAX(CASE WHEN pe.TPE_CODE = 'MIJ' THEN 1 ELSE 0 END)  AS ij,
           MAX(CASE WHEN pe.TPE_CODE = 'MAP' THEN 1 ELSE 0 END)  AS prenat,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))                    AS premiere_notif,
           TRUNC(MIN(pe.PE_DATEPROBABLE_ACC))                     AS date_probable_acc,
           TRUNC(MIN(pe.PE_DATE_CONGE))                           AS date_conge
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN dosm ON dosm.DOS_CODE = pe.DOS_CODE
    GROUP BY pe.DOS_CODE
),
base AS (
    SELECT dm.annee,
           dm.IND_ID,
           COUNT(*) OVER (PARTITION BY dm.IND_ID, dm.annee)       AS nb_dossiers_mere,
           NVL(p.ij, 0)                                           AS ij,
           NVL(p.prenat, 0)                                       AS prenat,
           CASE WHEN p.DOS_CODE IS NULL THEN 1 ELSE 0 END         AS sans_prest,
           p.premiere_notif,
           p.date_probable_acc,
           p.date_conge,
           dm.date_grossesse,
           TRUNC(r.RD_DATE_RECEPTION)                             AS date_reception
    FROM dosm dm
    LEFT JOIN prest p ON p.DOS_CODE = dm.DOS_CODE
    LEFT JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID = dm.RD_ID
)
SELECT annee,
       COUNT(*)                                                   AS dossiers,
       COUNT(DISTINCT IND_ID)                                     AS meres,
       SUM(CASE WHEN nb_dossiers_mere >= 2 THEN 1 ELSE 0 END)     AS dossiers_meme_mere_annee,
       SUM(CASE WHEN ij = 1 AND prenat = 0 THEN 1 ELSE 0 END)     AS ij_seules,
       SUM(CASE WHEN ij = 0 AND prenat = 1 THEN 1 ELSE 0 END)     AS prenatale_seule,
       SUM(CASE WHEN ij = 1 AND prenat = 1 THEN 1 ELSE 0 END)     AS ij_et_prenatale,
       SUM(sans_prest)                                            AS sans_prestation,
       ROUND(MEDIAN(premiere_notif - date_reception))             AS med_recep_a_notif_j,
       SUM(CASE WHEN date_reception <  date_probable_acc
                THEN 1 ELSE 0 END)                                AS recus_avant_acc_probable,
       SUM(CASE WHEN date_reception >= date_probable_acc
                THEN 1 ELSE 0 END)                                AS recus_apres_acc_probable,
       ROUND(MEDIAN(date_reception - date_probable_acc))          AS med_acc_probable_a_recep_j,
       COUNT(date_conge)                                          AS avec_date_conge,
       COUNT(date_grossesse)                                      AS avec_date_grossesse
FROM base
GROUP BY annee
ORDER BY annee;

-- Résultat obtenu (P67, exécutée le 30/10/2026) :
--   Année   Dossiers   Mères   Deux dossiers la même année   IJ et prénatale   Délai médian
--   2019    3 248      3 244    8                            3 223              7 j
--   2020    3 560      3 554   12                            3 543              8 j
--   2021    3 823      3 822    2                            3 813              8 j
--   2022    3 976      3 974    4                            3 954             12 j
--   2023    4 060      4 058    4                            4 042              8 j
--   2024    3 938      3 934    8                            3 914             11 j
--   2025    4 238      4 233    9                            4 211             16 j
--   IJ seules 3 à 19 par an, prénatale seule 0 ou 1, sans prestation 4 à 9.
-- Lecture (calculé ici) : en 2025, 4 211 dossiers sur 4 238 portent l'IJ et
--   l'allocation prénatale, soit 99,4 % ; de 2019 à 2025, 26 700 sur 26 843
--   (99,5 %). Un dossier de maternité est une grossesse.


-- -----------------------------------------------------------------------------
-- Requête 2. Prestations des prestations familiales et de la maternité par
-- code (FAF, FAP, MAP, MIJ) : prestations, saisies en 2025, avec une date de
-- notification, notifiées en 2025, délai médian de la demande à la
-- notification.
-- -----------------------------------------------------------------------------
WITH prest AS (
    SELECT pe.TPE_CODE,
           pe.PE_DATE_INSERT,
           pe.PE_DATE_DEMANDE,
           pe.PE_DATE_NOTIFICATION,
           pe.PE_DATE_GROSSESSE,
           pe.PE_DATEPROBABLE_ACC,
           d.RD_ID,
           MAX(CASE WHEN pe.TPE_CODE IN ('FAF', 'MIJ') THEN 1 ELSE 0 END)
               OVER (PARTITION BY pe.DOS_CODE)                    AS avec_faf_ou_mij
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE IN ('F', 'M')
)
SELECT TPE_CODE,
       COUNT(*)                                                   AS prestations,
       SUM(CASE WHEN PE_DATE_INSERT >= DATE '2025-01-01'
                 AND PE_DATE_INSERT <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                                AS saisies_2025,
       SUM(CASE WHEN PE_DATE_DEMANDE IS NOT NULL
                THEN 1 ELSE 0 END)                                AS avec_date_demande,
       SUM(CASE WHEN PE_DATE_NOTIFICATION IS NOT NULL
                THEN 1 ELSE 0 END)                                AS avec_date_notification,
       SUM(CASE WHEN PE_DATE_GROSSESSE IS NOT NULL
                  OR PE_DATEPROBABLE_ACC IS NOT NULL
                THEN 1 ELSE 0 END)                                AS avec_date_grossesse,
       SUM(CASE WHEN RD_ID IS NOT NULL THEN 1 ELSE 0 END)         AS dossier_ouvert_par_reception,
       SUM(avec_faf_ou_mij)                                       AS dossier_avec_faf_ou_mij,
       SUM(CASE WHEN PE_DATE_NOTIFICATION >= DATE '2025-01-01'
                 AND PE_DATE_NOTIFICATION <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                                AS notifiees_2025,
       ROUND(MEDIAN(CASE WHEN PE_DATE_NOTIFICATION >= DATE '2025-01-01'
                          AND PE_DATE_NOTIFICATION <  DATE '2026-01-01'
                         THEN TRUNC(PE_DATE_NOTIFICATION)
                              - TRUNC(PE_DATE_DEMANDE) END))      AS med_demande_notif_2025_j
FROM prest
WHERE TPE_CODE IN ('FAF', 'FAP', 'MIJ', 'MAP')
GROUP BY TPE_CODE
ORDER BY TPE_CODE;

-- Résultat obtenu (P66, exécutée le 30/10/2026) :
--   Code   Prestations   Saisies 2025   Avec notification   Dossier ouvert par réception   Dans un dossier FAF ou MIJ   Notifiées 2025   Délai médian
--   FAF    614 811       29 794         419 898             293 468                        614 811                      25 753           11 j
--   FAP      9 600           37           3 869                 498                          8 275                          33            2 j
--   MAP     52 716        4 236          31 950              41 506                         52 708                       4 245           15 j
--   MIJ     56 426        4 260          45 611              41 772                         56 426                       4 273           15 j
-- Lecture : en 2025, 4 273 IJ de maternité et 4 245 allocations prénatales
--   notifiées. Compter la grossesse sur une seule ligne retirerait environ
--   4 200 demandes du Total de T27.
