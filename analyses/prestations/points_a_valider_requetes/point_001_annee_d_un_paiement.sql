-- =============================================================================
-- Point 1. L'année d'un paiement
-- Fiche : points_a_valider_v2.md, rubrique « À trancher en priorité ».
-- Ce que dit le point : « en 2024, la saisie retombe mieux sur l'annuaire CNSS
--   2024 pour 11 lignes sur 13, dont les allocations de vieillesse : 497,8 M à
--   la saisie, 497,7 M publiés, 517,3 M au mois payé. Le mois payé l'emporte
--   pour la pension de vieillesse, la plus grosse ligne, et de peu la rente
--   viagère. Mois par mois, les montants publiés suivent les paiements
--   saisis. »
-- Requêtes :
--   1. Les 13 lignes de montants de l'annuaire, au mois payé et à la saisie.
--   2. Le rythme mois par mois : corrélation entre les montants publiés et
--      ceux de la base, au mois payé et à la saisie.
-- Reprise de : lot8_evolution_5_ans.sql, P98, et lot9_indus.sql, P105
--   (exécutées le 01/11/2026). Mêmes tables, mêmes filtres ; les montants
--   publiés de l'annuaire sont écrits dans la requête.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Chaque requête
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : le réglage date_montant ('MOIS') du bloc parametres
--   de t31_requete_proposee.sql, t32_t41_requete_proposee.sql et
--   t50_t57_requete_proposee.sql. Fiche des règles : règle 8.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Les 13 lignes de l'annuaire, au mois payé et à la saisie
-- Paiement = débours de paiement (DEB_TYPE = 'PM') au statut D ou P.
-- Mois payé = DEB_DATE_EFFET en 2024 ; saisie = DEB_DATE_INSERT en 2024.
-- -----------------------------------------------------------------------------
WITH publie AS (
    -- Montants publiés de 2024, en F CFA : annuaire CNSS 2024, tableau 34
    -- (p. 28) pour les lignes 1 à 6, tableau 40 (p. 32) pour 7 à 9,
    -- tableau 97 (p. 74) pour 10 à 13.
    SELECT 1 AS rang, 'Pension de vieillesse' AS ligne, 38235880033 AS montant_publie FROM DUAL
    UNION ALL SELECT  2, 'Majoration pour enfant',          537343000 FROM DUAL
    UNION ALL SELECT  3, 'Pensions de survivants',         7047164871 FROM DUAL
    UNION ALL SELECT  4, 'Pension d''invalidité',            113603730 FROM DUAL
    UNION ALL SELECT  5, 'Allocation de vieillesse',         497726931 FROM DUAL
    UNION ALL SELECT  6, 'Allocations de survivants',        197906991 FROM DUAL
    UNION ALL SELECT  7, 'Allocations familiales',          8236741500 FROM DUAL
    UNION ALL SELECT  8, 'IJ de maternité',                 2865630056 FROM DUAL
    UNION ALL SELECT  9, 'Allocations prénatales',            52789000 FROM DUAL
    UNION ALL SELECT 10, 'IJ d''accident du travail',          65420082 FROM DUAL
    UNION ALL SELECT 11, 'Allocations d''incapacité',          20034111 FROM DUAL
    UNION ALL SELECT 12, 'Rentes viagères',                  455021317 FROM DUAL
    UNION ALL SELECT 13, 'Rentes de survivants AT/MP',       368602710 FROM DUAL
),
code_ligne AS (
    -- Les codes de prestation de chaque ligne (TPE_CODE de PRESTATION_ESP).
    SELECT 'RPV' AS code, 1 AS rang FROM DUAL
    UNION ALL SELECT 'RBEC',  2 FROM DUAL
    UNION ALL SELECT 'RPSV',  3 FROM DUAL
    UNION ALL SELECT 'RPSF',  3 FROM DUAL
    UNION ALL SELECT 'RPSE',  3 FROM DUAL
    UNION ALL SELECT 'RPA',   3 FROM DUAL
    UNION ALL SELECT 'RPI',   4 FROM DUAL
    UNION ALL SELECT 'RAV',   5 FROM DUAL
    UNION ALL SELECT 'RASC',  6 FROM DUAL
    UNION ALL SELECT 'RASO',  6 FROM DUAL
    UNION ALL SELECT 'RASA',  6 FROM DUAL
    UNION ALL SELECT 'FAF',   7 FROM DUAL
    UNION ALL SELECT 'MIJ',   8 FROM DUAL
    UNION ALL SELECT 'MAP',   9 FROM DUAL
    UNION ALL SELECT 'FAP',   9 FROM DUAL
    UNION ALL SELECT 'AIJ',  10 FROM DUAL
    UNION ALL SELECT 'AAI',  11 FROM DUAL
    UNION ALL SELECT 'ARI',  12 FROM DUAL
    UNION ALL SELECT 'ARSV', 13 FROM DUAL
    UNION ALL SELECT 'ARSF', 13 FROM DUAL
    UNION ALL SELECT 'ARSE', 13 FROM DUAL
    UNION ALL SELECT 'ARSA', 13 FROM DUAL
),
base AS (
    SELECT cl.rang,
           SUM(CASE WHEN deb.DEB_DATE_EFFET  >= DATE '2024-01-01'
                     AND deb.DEB_DATE_EFFET  <  DATE '2025-01-01'
                    THEN deb.DEB_MONTANT END)                     AS mois_paye,
           SUM(CASE WHEN deb.DEB_DATE_INSERT >= DATE '2024-01-01'
                     AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
                    THEN deb.DEB_MONTANT END)                     AS saisie
    FROM USER_DWH.DEBOURS deb
    JOIN USER_DWH.PRESTATION_ESP p ON p.PE_ID   = deb.PE_ID
    JOIN code_ligne             cl ON cl.code   = p.TPE_CODE
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND (   (deb.DEB_DATE_EFFET  >= DATE '2024-01-01' AND deb.DEB_DATE_EFFET  < DATE '2025-01-01')
           OR (deb.DEB_DATE_INSERT >= DATE '2024-01-01' AND deb.DEB_DATE_INSERT < DATE '2025-01-01'))
    GROUP BY cl.rang
)
SELECT pu.rang,
       pu.ligne,
       pu.montant_publie,
       ROUND(b.mois_paye)                                         AS mois_paye_2024,
       ROUND(b.saisie)                                            AS saisie_2024,
       ROUND(100 * (b.mois_paye / pu.montant_publie - 1), 2)      AS ecart_mois_paye_pct,
       ROUND(100 * (b.saisie    / pu.montant_publie - 1), 2)      AS ecart_saisie_pct,
       CASE WHEN ABS(b.saisie - pu.montant_publie) < ABS(b.mois_paye - pu.montant_publie)
            THEN 'saisie' ELSE 'mois payé' END                    AS date_la_plus_proche
FROM publie pu
LEFT JOIN base b ON b.rang = pu.rang
ORDER BY pu.rang;

-- Résultat obtenu (sommes de P98, exécutée le 01/11/2026, regroupées selon les
-- 13 lignes ; voir resultats/lot8_P98_montants_2024_par_date.tsv) :
--   Rang  Ligne                          Publié           Mois payé        Saisie           Écart mois payé  Écart saisie  Plus proche
--    1    Pension de vieillesse          38 235 880 033   38 496 066 498   38 821 571 458    +0,68 %          +1,53 %      mois payé
--    2    Majoration pour enfant            537 343 000      554 254 000      539 376 000    +3,15 %          +0,38 %      saisie
--    3    Pensions de survivants          7 047 164 871    7 222 987 423    7 184 934 762    +2,49 %          +1,95 %      saisie
--    4    Pension d'invalidité              113 603 730      116 130 355      113 131 108    +2,22 %          −0,42 %      saisie
--    5    Allocation de vieillesse          497 726 931      517 334 647      497 818 548    +3,94 %          +0,02 %      saisie
--    6    Allocations de survivants         197 906 991      187 083 247      190 149 232    −5,47 %          −3,92 %      saisie
--    7    Allocations familiales          8 236 741 500    7 954 458 000    8 000 440 000    −3,43 %          −2,87 %      saisie
--    8    IJ de maternité                 2 865 630 056    2 745 737 842    2 749 474 700    −4,18 %          −4,05 %      saisie
--    9    Allocations prénatales             52 789 000       50 677 500       52 149 000    −4,00 %          −1,21 %      saisie
--   10    IJ d'accident du travail           65 420 082       46 020 175       51 859 178   −29,65 %         −20,73 %      saisie
--   11    Allocations d'incapacité           20 034 111       23 238 000       17 812 800   +15,99 %         −11,09 %      saisie
--   12    Rentes viagères                   455 021 317      399 428 069      397 834 986   −12,22 %         −12,57 %      mois payé
--   13    Rentes de survivants AT/MP        368 602 710      275 923 240      322 631 157   −25,14 %         −12,47 %      saisie
-- Lecture :
--   · La saisie est plus proche sur 11 lignes sur 13 ; le mois payé sur la
--     pension de vieillesse (ligne 1) et, de peu, la rente viagère (ligne 12).
--   · Ligne 5 : 497,8 M à la saisie, 497,7 M publiés, 517,3 M au mois payé.
-- À noter : la retraite anticipée (RPRA) n'a pas de ligne à part dans
--   l'annuaire. L'ajouter à la ligne 1 donne +1,02 % au mois payé et +1,86 %
--   à la saisie : le mois payé reste plus proche.


-- -----------------------------------------------------------------------------
-- Requête 2. Le rythme mois par mois de 2024 : corrélation avec l'annuaire
-- Branche du paiement : CODE_BRANCHE de DEBOURS (V = PVID, A = AT/MP, les
-- autres = PF, maternité comprise), comme P105.
-- -----------------------------------------------------------------------------
WITH publie AS (
    -- Montants publiés par mois en 2024, en F CFA, annuaire CNSS 2024 :
    -- PVID, tableau 28 (p. 22), ligne « Total (A)+(B) » ;
    -- PF, tableau 41 (p. 33), colonne Total ;
    -- AT/MP, tableau 90 (p. 71), total des prestations en espèces.
    SELECT 'PVID' AS branche, 1 AS mois, 3397274607 AS montant FROM DUAL
    UNION ALL SELECT 'PVID',   2, 3922781987 FROM DUAL
    UNION ALL SELECT 'PVID',   3, 3942140486 FROM DUAL
    UNION ALL SELECT 'PVID',   4, 3546568158 FROM DUAL
    UNION ALL SELECT 'PVID',   5, 4129263761 FROM DUAL
    UNION ALL SELECT 'PVID',   6, 4015794779 FROM DUAL
    UNION ALL SELECT 'PVID',   7, 3524307049 FROM DUAL
    UNION ALL SELECT 'PVID',   8, 4106167417 FROM DUAL
    UNION ALL SELECT 'PVID',   9, 4039356495 FROM DUAL
    UNION ALL SELECT 'PVID',  10, 3650497198 FROM DUAL
    UNION ALL SELECT 'PVID',  11, 4311198547 FROM DUAL
    UNION ALL SELECT 'PVID',  12, 4044275072 FROM DUAL
    UNION ALL SELECT 'PF',     1,  972343736 FROM DUAL
    UNION ALL SELECT 'PF',     2,  962500013 FROM DUAL
    UNION ALL SELECT 'PF',     3,  922343646 FROM DUAL
    UNION ALL SELECT 'PF',     4,  881433604 FROM DUAL
    UNION ALL SELECT 'PF',     5,  871190912 FROM DUAL
    UNION ALL SELECT 'PF',     6,  782563687 FROM DUAL
    UNION ALL SELECT 'PF',     7,  960210798 FROM DUAL
    UNION ALL SELECT 'PF',     8,  995938579 FROM DUAL
    UNION ALL SELECT 'PF',     9, 1016497815 FROM DUAL
    UNION ALL SELECT 'PF',    10,  927542895 FROM DUAL
    UNION ALL SELECT 'PF',    11,  911205503 FROM DUAL
    UNION ALL SELECT 'PF',    12,  976420237 FROM DUAL
    UNION ALL SELECT 'AT/MP',  1,   60846764 FROM DUAL
    UNION ALL SELECT 'AT/MP',  2,  108631515 FROM DUAL
    UNION ALL SELECT 'AT/MP',  3,   79866449 FROM DUAL
    UNION ALL SELECT 'AT/MP',  4,   49386305 FROM DUAL
    UNION ALL SELECT 'AT/MP',  5,  105846755 FROM DUAL
    UNION ALL SELECT 'AT/MP',  6,   73091689 FROM DUAL
    UNION ALL SELECT 'AT/MP',  7,   51485031 FROM DUAL
    UNION ALL SELECT 'AT/MP',  8,   88020983 FROM DUAL
    UNION ALL SELECT 'AT/MP',  9,   92750822 FROM DUAL
    UNION ALL SELECT 'AT/MP', 10,   52494992 FROM DUAL
    UNION ALL SELECT 'AT/MP', 11,   95451788 FROM DUAL
    UNION ALL SELECT 'AT/MP', 12,   76572036 FROM DUAL
),
au_mois_paye AS (
    SELECT CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END
                                                                  AS branche,
           EXTRACT(MONTH FROM deb.DEB_DATE_EFFET)                 AS mois,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
    GROUP BY CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END,
             EXTRACT(MONTH FROM deb.DEB_DATE_EFFET)
),
a_la_saisie AS (
    SELECT CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END
                                                                  AS branche,
           EXTRACT(MONTH FROM deb.DEB_DATE_INSERT)                AS mois,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_INSERT >= DATE '2024-01-01'
      AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
    GROUP BY CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP' ELSE 'PF' END,
             EXTRACT(MONTH FROM deb.DEB_DATE_INSERT)
)
SELECT pu.branche,
       ROUND(CORR(pu.montant, mp.montant), 2)                     AS correlation_mois_paye,
       ROUND(CORR(pu.montant, sa.montant), 2)                     AS correlation_saisie
FROM publie pu
LEFT JOIN au_mois_paye mp ON mp.branche = pu.branche AND mp.mois = pu.mois
LEFT JOIN a_la_saisie  sa ON sa.branche = pu.branche AND sa.mois = pu.mois
GROUP BY pu.branche
ORDER BY pu.branche;

-- Résultat obtenu (montants mensuels de P105, exécutée le 01/11/2026, voir
-- resultats/lot9_P105_t31_2024.tsv ; corrélation calculée le 08/11/2026) :
--   Branche  Corrélation au mois payé  Corrélation à la saisie
--   AT/MP    −0,08                     0,84
--   PF        0,26                     0,92
--   PVID     −0,12                     0,75
-- Les montants, en millions de F CFA, de janvier à décembre :
--   PVID  publié     3 397  3 923  3 942  3 547  4 129  4 016  3 524  4 106  4 039  3 650  4 311  4 044
--         saisie     3 458  4 322  3 721  3 615  4 575  3 682  3 564  4 556  3 756  3 732  4 728  3 776
--         mois payé  3 997  3 871  3 875  3 907  3 910  3 904  3 941  3 939  3 947  3 971  3 982  3 991
--   PF    publié       972    963    922    881    871    783    960    996  1 016    928    911    976
--         saisie       963    955    853    861    831    758    936    999    935    903    893    915
--         mois payé    904    870    875    863    881    867    873    880    909    954    950    924
--   AT/MP publié      60,8  108,6   79,9   49,4  105,8   73,1   51,5   88,0   92,8   52,5   95,5   76,6
--         saisie      49,6  111,9   55,1   41,5  109,5   42,8   44,1   95,9   53,0   45,1   96,3   45,3
--         mois payé   64,3   63,2   60,1   59,7   60,8   59,6   60,9   59,5   62,7   68,0   65,0   60,7
-- Lecture : au mois payé, la base est presque plate d'un mois à l'autre ; à la
--   saisie, elle monte et descend avec l'annuaire (pics de février, mai, août
--   et novembre en PVID). La CNSS publie les paiements le mois de leur saisie.
