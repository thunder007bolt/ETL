-- =============================================================================
-- Point 24. Les soins (T26)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T25 à
--   T27.
-- Ce que dit le point : « chaque remboursement de soins est un paiement
--   (DEBOURS) du dossier d'accident du travail, saisi sans passer par le
--   guichet. » ; « en 2024, 973 vieux dossiers ont été approuvés d'un coup. » ;
--   « environ 800 dossiers par an ont un remboursement de soins, hors 2024.
--   L'annuaire CNSS 2024 compte autrement : 139,7 M de soins publiés en 2024,
--   contre 18,8 M dans la base. »
-- Requêtes :
--   1. Les dossiers d'accident du travail par année de leur premier
--      remboursement de soins.
--   2. Le pic de 2024 : les dossiers au premier remboursement approuvé en
--      2024, par période de l'accident.
--   3. Les montants servis de 2024 par branche et par type de débours, dont
--      les remboursements de soins.
-- Reprise de : lot4_controles_t26_t27.sql, P63 et P73 (exécutées le
--   30/10/2026) ; lot8_evolution_5_ans.sql, P95 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   trois requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t26_requete_proposee.sql, bloc demande (les soins,
--   débours avec un TPN_CODE, font une demande par dossier en « Autres
--   prestations de la branche », liquidée au premier remboursement saisi).
--   Fiche des règles : règle 31.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Dossiers d'accident du travail par année du premier
-- remboursement de soins : dossiers, dossiers sans autre prestation,
-- remboursements par dossier, délai de la réception à la première
-- approbation.
-- -----------------------------------------------------------------------------
WITH dosa AS (
    SELECT DOS_CODE, RD_ID, TRUNC(DOS_DATE_ACCIDENT) AS date_accident
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'A'
),
soins AS (
    SELECT deb.DOS_CODE,
           COUNT(*)                                               AS nb_rb,
           SUM(CASE WHEN deb.DEB_STATUT IN ('D', 'P')
                    THEN 1 ELSE 0 END)                            AS nb_payes,
           TRUNC(MIN(deb.DEB_DATE_INSERT))                        AS premiere_saisie,
           TRUNC(MIN(deb.DEB_DATE_APPROBATION))                   AS premiere_approbation,
           TRUNC(MIN(deb.DEB_DATE_PRESTATION))                    AS premier_soin
    FROM USER_DWH.DEBOURS deb
    JOIN dosa ON dosa.DOS_CODE = deb.DOS_CODE
    WHERE deb.TPN_CODE IS NOT NULL
    GROUP BY deb.DOS_CODE
),
esp AS (
    SELECT DISTINCT pe.DOS_CODE
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN dosa ON dosa.DOS_CODE = pe.DOS_CODE
),
base AS (
    SELECT s.nb_rb,
           s.nb_payes,
           s.premiere_saisie,
           s.premiere_approbation,
           s.premier_soin,
           a.date_accident,
           TRUNC(r.RD_DATE_RECEPTION)                             AS date_reception,
           CASE WHEN e.DOS_CODE IS NULL THEN 1 ELSE 0 END         AS soins_seuls,
           EXTRACT(YEAR FROM NVL(s.premiere_approbation,
                                 s.premiere_saisie))              AS annee
    FROM soins s
    JOIN dosa a ON a.DOS_CODE = s.DOS_CODE
    LEFT JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID    = a.RD_ID
    LEFT JOIN esp                        e ON e.DOS_CODE = s.DOS_CODE
)
SELECT annee,
       COUNT(*)                                                   AS dossiers_avec_soins,
       SUM(soins_seuls)                                           AS dont_soins_seuls,
       ROUND(AVG(nb_rb), 1)                                       AS remboursements_par_dossier,
       SUM(CASE WHEN premiere_approbation IS NULL
                THEN 1 ELSE 0 END)                                AS jamais_approuves,
       SUM(CASE WHEN nb_payes > 0 THEN 1 ELSE 0 END)              AS avec_un_paiement,
       COUNT(date_reception)                                      AS avec_reception,
       ROUND(MEDIAN(premiere_saisie - date_reception))            AS med_recep_a_saisie_j,
       ROUND(MEDIAN(premiere_approbation - date_reception))       AS med_recep_a_approbation_j,
       SUM(CASE WHEN premiere_approbation - date_reception > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_recep_approbation,
       ROUND(MEDIAN(premiere_approbation - date_accident))        AS med_accident_a_approbation_j,
       ROUND(MEDIAN(premiere_approbation - premier_soin))         AS med_soin_a_approbation_j
FROM base
WHERE annee BETWEEN 2019 AND 2025
GROUP BY annee
ORDER BY annee;

-- Résultat obtenu (P63, exécutée le 30/10/2026) :
--   Année   Dossiers   Soins seuls   Remboursements par dossier   Délai médian réception → approbation   Plus de 45 j
--   2019      756        406         10,4                          77 j                                  453
--   2020      840        431         10,4                          91 j                                  587
--   2021      960        547         10,0                          84 j                                  655
--   2022      855        512         10,2                          78 j                                  565
--   2023      699        375          9,6                         125 j                                  554
--   2024    1 477        780          7,8                         150 j                                  426
--   2025      659        423          7,7                         161 j                                  573
--   En 2024 : 973 dossiers sans réception, accident et soin 17 ans avant
--   l'approbation en médiane.
-- Lecture (calculé ici) : hors 2024, 795 dossiers par an en moyenne ont un
--   remboursement de soins, « environ 800 ».


-- -----------------------------------------------------------------------------
-- Requête 2. Dossiers au premier remboursement approuvé en 2024, par période
-- de l'accident et selon qu'ils ont une réception ou non.
-- -----------------------------------------------------------------------------
WITH dosa AS (
    SELECT d.DOS_CODE,
           d.RD_ID,
           CASE WHEN d.DOS_DATE_ACCIDENT IS NULL               THEN '5 sans date'
                WHEN d.DOS_DATE_ACCIDENT <  DATE '2000-01-01'  THEN '1 avant 2000'
                WHEN d.DOS_DATE_ACCIDENT <  DATE '2010-01-01'  THEN '2 2000-2009'
                WHEN d.DOS_DATE_ACCIDENT <  DATE '2020-01-01'  THEN '3 2010-2019'
                ELSE                                                '4 2020 et après'
           END                                                    AS periode_accident
    FROM USER_DWH.DOSSIER d
    WHERE d.TDOS_CODE = 'A'
),
rb AS (
    SELECT deb.DOS_CODE,
           TRUNC(deb.DEB_DATE_INSERT)                             AS date_saisie,
           TRUNC(deb.DEB_DATE_APPROBATION)                        AS date_approbation
    FROM USER_DWH.DEBOURS deb
    JOIN dosa ON dosa.DOS_CODE = deb.DOS_CODE
    WHERE deb.TPN_CODE IS NOT NULL
),
premier AS (
    SELECT DOS_CODE
    FROM rb
    GROUP BY DOS_CODE
    HAVING EXTRACT(YEAR FROM NVL(MIN(date_approbation), MIN(date_saisie))) = 2024
),
rente AS (
    SELECT DISTINCT DOS_CODE
    FROM USER_DWH.PRESTATION_ESP
    WHERE TPE_CODE IN ('ARI', 'AAI', 'ARSV', 'ARSF', 'ARSE', 'ARSA')
),
detail AS (
    SELECT a.periode_accident,
           CASE WHEN a.RD_ID IS NULL THEN 0 ELSE 1 END            AS avec_reception,
           rb.DOS_CODE,
           CASE WHEN r.DOS_CODE IS NULL THEN 0 ELSE 1 END         AS avec_rente,
           rb.date_saisie,
           TO_CHAR(rb.date_saisie, 'YYYY-MM')                     AS mois_saisie
    FROM rb
    JOIN premier p ON p.DOS_CODE = rb.DOS_CODE
    JOIN dosa    a ON a.DOS_CODE = rb.DOS_CODE
    LEFT JOIN rente r ON r.DOS_CODE = rb.DOS_CODE
),
par_mois AS (
    SELECT periode_accident, avec_reception, mois_saisie,
           COUNT(*)                                               AS nb,
           ROW_NUMBER() OVER (PARTITION BY periode_accident, avec_reception
                              ORDER BY COUNT(*) DESC)             AS rang
    FROM detail
    GROUP BY periode_accident, avec_reception, mois_saisie
)
SELECT d.periode_accident,
       d.avec_reception,
       COUNT(DISTINCT d.DOS_CODE)                                 AS dossiers,
       COUNT(DISTINCT CASE WHEN d.avec_rente = 1
                           THEN d.DOS_CODE END)                   AS dont_avec_rente,
       COUNT(*)                                                   AS remboursements,
       MIN(d.date_saisie)                                         AS premiere_saisie,
       MAX(d.date_saisie)                                         AS derniere_saisie,
       MAX(m.mois_saisie)                                         AS mois_le_plus_charge,
       MAX(m.nb)                                                  AS saisies_ce_mois
FROM detail d
LEFT JOIN par_mois m ON m.periode_accident = d.periode_accident
                    AND m.avec_reception   = d.avec_reception
                    AND m.rang             = 1
GROUP BY d.periode_accident, d.avec_reception
ORDER BY d.periode_accident, d.avec_reception;

-- Résultat obtenu (P73, exécutée le 30/10/2026) :
--   Période de l'accident, réception      Dossiers   Avec rente   Remboursements   Saisies
--   avant 2000, sans réception                36         30          303          du 16/06/2005 au 15/11/2024
--   avant 2000, avec réception                 2          1            7          2024
--   2000 à 2009, sans réception              936        157        6 428          du 17/05/2005 au 06/01/2026
--   2010 à 2019, sans réception                1          1          139          2012 à 2025
--   2010 à 2019, avec réception                4          1           36          2024 et 2025
--   2020 et après, avec réception            498         64        4 678          du 20/10/2022 au 02/02/2026
-- Lecture : les 973 vieux dossiers (36 + 936 + 1) ont des remboursements
--   saisis depuis 2005, mais une première approbation en 2024 : les dates
--   d'approbation ont été remplies d'un coup. D'où la règle : un dossier de
--   soins est liquidé à la saisie de son premier remboursement.


-- -----------------------------------------------------------------------------
-- Requête 3. Montants de 2024 au mois payé, débours au statut D ou P, par
-- branche et par type : paiements (PM), remboursements de soins (RB),
-- ajustements (AJ).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                      AS annee,
       NVL(deb.CODE_BRANCHE, '?')                                 AS branche,
       deb.DEB_TYPE                                               AS type_debours,
       COUNT(*)                                                   AS debours,
       ROUND(SUM(deb.DEB_MONTANT))                                AS montant,
       SUM(CASE WHEN deb.PE_ID IS NULL THEN 1 ELSE 0 END)         AS sans_prestation,
       ROUND(SUM(CASE WHEN deb.PE_ID IS NULL
                      THEN deb.DEB_MONTANT END))                  AS montant_sans_prestation
FROM USER_DWH.DEBOURS deb
WHERE deb.DEB_STATUT IN ('D', 'P')
  AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
  AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM deb.DEB_DATE_EFFET), NVL(deb.CODE_BRANCHE, '?'), deb.DEB_TYPE
ORDER BY annee, branche, type_debours;

-- Résultat obtenu (P95, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P95_montants_par_branche.tsv) :
--   Branche   Paiements (PM)    Remboursements (RB)   Ajustements (AJ)
--   V         47 237 434 040    -                     587 162 398
--   F          7 954 774 500    -                     201 000 381
--   M          2 796 098 842    -                       8 766 415
--   A            744 609 484    18 787 945             25 799 280
-- Face au tableau 82 de l'annuaire CNSS 2024 (p. 63) : 139 668 695 de
-- prestations en nature des risques professionnels en 2024.
-- Lecture : la base ne rembourse que 18,8 M de soins en 2024, pour 139,7 M
--   publiés : des soins sont payés ailleurs, et des dossiers de soins peuvent
--   manquer à T26.
